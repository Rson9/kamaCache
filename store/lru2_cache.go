package store

import (
	"sync/atomic"
	"time"
)

// 内部缓存核心实现，包含双向链表和节点存储
type cache struct {
	capacity uint16 // 缓存容量
	// dlnk[0]是哨兵节点，记录链表头尾，dlnk[0][p]存储尾部索引，dlnk[0][n]存储头部索引
	dlnk           [][2]uint16       // 双向链表，0 表示前驱，1 表示后继
	m              []node            // 预分配内存存储节点
	hmap           map[string]uint16 // 键到节点索引的映射
	last           uint16            // 最后一个节点元素的索引
	onLengthChange func(delta int)
	onEvicted      func(string, Value)
}

var clock int64 // 内部时钟变量

const (
	prev = uint16(0) // 链表前驱方向
	next = uint16(1) // 链表后继方向
)

func Create(cap uint16, onEvicted func(string, Value)) *cache {
	return &cache{
		capacity:  cap,
		dlnk:      make([][2]uint16, cap+1),     // 包含哨兵节点
		m:         make([]node, cap),            //节点数组
		hmap:      make(map[string]uint16, cap), //key->节点索引
		onEvicted: onEvicted,
	}
}

// Len 返回缓存中当前的元素数量
func (c *cache) Len() int {
	return len(c.hmap)
}

// isFull 检查缓存是否已满
func (c *cache) isFull() bool {
	return len(c.hmap) == int(c.capacity)
}

// 从缓存中获取键对应的节点和状态
func (c *cache) get(key string) (*node, bool) {
	idx, ok := c.hmap[key]
	if !ok {
		return nil, false
	}

	nd := &c.m[idx-1]
	if nd.isExpired() {
		c.del(key)
		return nil, false
	}
	// 访问后提升优先级
	c.adjust(idx, prev, next)
	return nd, true
}

// put 在缓存中插入键值对，返回是否发生了淘汰
func (c *cache) put(key string, val Value, expireAt int64) (*node, bool) {
	if idx, ok := c.hmap[key]; ok {
		n := &c.m[idx-1]
		n.v, n.expireAt = val, expireAt
		c.adjust(idx, prev, next)
		return nil, false
	}

	// 容量已满，需要淘汰
	if c.isFull() {
		tailIdx := c.dlnk[0][prev] // LRU 节点索引
		if tailIdx == 0 {
			// 安全保护，链表空，不应该发生满的情况
			return nil, false
		}
		tail := &c.m[tailIdx-1]
		evicted := &node{
			k:        tail.k,
			v:        tail.v,
			expireAt: tail.expireAt,
		}

		delete(c.hmap, tail.k)
		c.hmap[key], tail.k, tail.v, tail.expireAt = tailIdx, key, val, expireAt
		c.adjust(tailIdx, prev, next)
		// 在这里调用更符合内聚性。
		if c.onEvicted != nil && !evicted.isExpired() {
			c.onEvicted(evicted.k, evicted.v)
		}

		return evicted, true
	}
	// --- 3. 容量未满，直接在末尾新增节点 ---
	idx := uint16(len(c.hmap) + 1)
	if idx > c.capacity {
		// 理论不会走到这里，防护
		return nil, false
	}
	c.m[idx-1] = node{k: key, v: val, expireAt: expireAt}
	c.hmap[key] = idx

	head := c.dlnk[0][next]
	c.dlnk[idx][prev] = 0
	c.dlnk[idx][next] = head
	c.dlnk[head][prev] = idx
	c.dlnk[0][next] = idx

	if c.last == 1 {
		c.dlnk[0][prev] = idx
	}

	if c.onLengthChange != nil {
		c.onLengthChange(1)
	}
	return nil, false
}

// 从缓存中删除键对应的项
func (c *cache) del(key string) (*node, bool) {
	idx, ok := c.hmap[key]
	if !ok {
		return nil, false
	}

	// 从 map 中删除
	delete(c.hmap, key)
	// 链表断开链接
	c.unlink(idx)

	// 通知上层长度变化
	if c.onLengthChange != nil {
		c.onLengthChange(-1)
	}

	return &c.m[idx-1], true
}

// walk 遍历缓存中的所有项，不过滤
func (c *cache) walk(walker func(key string, value Value, expireAt int64) bool) {
	// 遍历顺序：从 MRU 到 LRU
	for idx := c.dlnk[0][next]; idx != 0; idx = c.dlnk[idx][next] {
		// 不再进行任何过滤，直接将元素交给 walker 处理
		// walker 的返回值为 false 时，依然可以提前终止循环
		if !walker(c.m[idx-1].k, c.m[idx-1].v, c.m[idx-1].expireAt) {
			return
		}
	}
}

// 调整节点在链表中的位置
// 当 f=0, t=1 时，移动到链表头部；否则移动到链表尾部
func (c *cache) adjust(idx, f, t uint16) {
	if c.dlnk[idx][f] == 0 {
		return
	}
	// 从原位置断开
	c.dlnk[c.dlnk[idx][t]][f] = c.dlnk[idx][f]
	c.dlnk[c.dlnk[idx][f]][t] = c.dlnk[idx][t]

	// 插入到新头部
	c.dlnk[idx][f] = 0
	c.dlnk[idx][t] = c.dlnk[0][t]
	c.dlnk[c.dlnk[0][t]][f] = idx
	c.dlnk[0][t] = idx
}

// unlink 从双向链表中摘除一个节点。
func (c *cache) unlink(idx uint16) {
	prevIdx := c.dlnk[idx][prev]
	nextIdx := c.dlnk[idx][next]
	c.dlnk[prevIdx][next] = nextIdx
	c.dlnk[nextIdx][prev] = prevIdx
}

// removeExpired 扫描并删除所有过期项
func (c *cache) removeExpired(now int64) (count int) {
	currentIdx := c.dlnk[0][prev]

	for currentIdx != 0 {
		prevIdx := c.dlnk[currentIdx][prev]
		nd := &c.m[currentIdx-1]

		if nd.expireAt > 0 && now >= nd.expireAt {
			if _, ok := c.del(nd.k); ok {
				if c.onEvicted != nil {
					c.onEvicted(nd.k, nd.v)
				}
				count++
			}
		}

		currentIdx = prevIdx
	}
	return
}

// clear 清空整个缓存。
func (c *cache) clear() {
	// 报告总长度变化
	if c.onLengthChange != nil {
		delta := len(c.hmap)
		if delta > 0 {
			c.onLengthChange(-delta)
		}
	}

	// 重置所有状态，但保留预分配的内存
	c.hmap = make(map[string]uint16, c.capacity)
	c.last = 0
	// 重置哨兵节点
	c.dlnk[0][prev] = 0
	c.dlnk[0][next] = 0
}

// Now 返回 clock 变量的当前值。atomic.LoadInt64 是原子操作，用于保证在多线程/协程环境中安全地读取 clock 变量的值
func Now() int64 { return atomic.LoadInt64(&clock) }

func init() {
	ticker := time.NewTicker(10 * time.Millisecond)
	go func() {
		defer ticker.Stop()
		for t := range ticker.C {
			atomic.StoreInt64(&clock, t.UnixNano())
		}
	}()
}
