package store

import (
	"container/heap"
	"container/list"
	"sync"
	"time"
)

// lruCache 是基于标准库 list 的 LRU 缓存实现
type lruCache struct {
	mu    sync.Mutex
	list  *list.List               // 双向链表，用于维护 LRU 顺序
	items map[string]*list.Element // 键到链表节点的映射
	//expires         map[string]time.Time     // 过期时间映射(-1 表示无过期时间)
	maxBytes  int64 // 最大允许字节数
	usedBytes int64 // 当前使用的字节数
	onEvicted func(key string, value Value)
	// 用于高效过期处理的结构
	expireHeap expirationHeap // 最小堆，按过期时间排序
	heapIndex  map[string]int // 键到堆内索引的映射

	cleanupInterval time.Duration
	cleanupTicker   *time.Ticker
	closeCh         chan struct{} // 用于优雅关闭清理协程
}

// lruEntry 表示缓存中的一个条目
type lruEntry struct {
	key   string
	value Value
}

// newLRUCache 创建一个新的 LRU 缓存实例
func newLRUCache(opts Options) *lruCache {
	// 设置默认清理间隔
	cleanupInterval := opts.CleanupInterval
	if cleanupInterval <= 0 {
		cleanupInterval = time.Minute
	}

	c := &lruCache{
		list:  list.New(),
		items: make(map[string]*list.Element),
		//expires:         make(map[string]time.Time),
		maxBytes:        opts.MaxBytes,
		onEvicted:       opts.OnEvicted,
		expireHeap:      make(expirationHeap, 0),
		heapIndex:       make(map[string]int),
		cleanupInterval: cleanupInterval,
		closeCh:         make(chan struct{}),
	}

	heap.Init(&c.expireHeap)

	// 启动定期清理协程
	c.cleanupTicker = time.NewTicker(c.cleanupInterval)
	go c.cleanupLoop()

	return c
}

// Get 获取缓存项，如果存在且未过期则返回
func (c *lruCache) Get(key string) (Value, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	elem, ok := c.items[key]
	if !ok {
		return nil, false
	}

	// 检查是否过期
	if idx, hasExp := c.heapIndex[key]; hasExp {
		if c.expireHeap[idx].expiresAt.Before(time.Now()) {
			// 同步删除过期项
			c.removeElement(elem)
			return nil, false
		}
	}

	// 命中，移动到队尾
	c.list.MoveToBack(elem)
	return elem.Value.(*lruEntry).value, true
}

// Set 添加或更新缓存项
func (c *lruCache) Set(key string, value Value) error {
	return c.SetWithExpiration(key, value, -1)
}

// SetWithExpiration 添加或更新缓存项，并设置过期时间
func (c *lruCache) SetWithExpiration(key string, value Value, expiration time.Duration) error {
	if value == nil {
		c.Delete(key)
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// 如果键已存在，更新值
	if elem, ok := c.items[key]; ok {
		c.list.MoveToBack(elem)
		oldEntry := elem.Value.(*lruEntry)
		c.usedBytes += int64(value.Len() - oldEntry.value.Len())
		oldEntry.value = value
	} else {
		// 添加新项
		entry := &lruEntry{key: key, value: value}
		elem := c.list.PushBack(entry)
		c.items[key] = elem
		c.usedBytes += int64(len(key) + value.Len())
	}
	// 更新过期时间
	c.updateExpiration(key, expiration)

	// 检查是否需要淘汰旧项
	c.evict(true)

	return nil
}

// Delete 从缓存中删除指定键的项
func (c *lruCache) Delete(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.items[key]; ok {
		c.removeElement(elem)
		return true
	}
	return false
}

// Clear 清空缓存
func (c *lruCache) Clear() {
	c.mu.Lock()
	// 暂存需要回调的条目
	var entriesToEvict []*lruEntry
	if c.onEvicted != nil {
		entriesToEvict = make([]*lruEntry, 0, c.list.Len())
		for _, elem := range c.items {
			entriesToEvict = append(entriesToEvict, elem.Value.(*lruEntry))
		}
	}

	c.list.Init()
	c.items = make(map[string]*list.Element)
	c.expireHeap = make(expirationHeap, 0)
	c.heapIndex = make(map[string]int)
	c.usedBytes = 0
	c.mu.Unlock()
	// 在锁外执行回调
	if c.onEvicted != nil {
		for _, entry := range entriesToEvict {
			c.onEvicted(entry.key, entry.value)
		}
	}
}

// Len 返回缓存中的项数
func (c *lruCache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.list.Len()
}

// removeElement 从缓存中删除元素，调用此方法前必须持有锁
// 它不执行回调，而是返回待处理的条目。
func (c *lruCache) removeElement(elem *list.Element) *lruEntry {
	if elem == nil {
		return nil
	}
	entry := c.list.Remove(elem).(*lruEntry)
	delete(c.items, entry.key)
	c.usedBytes -= int64(len(entry.key) + entry.value.Len())
	// 从过期堆中移除
	if idx, ok := c.heapIndex[entry.key]; ok {
		heap.Remove(&c.expireHeap, idx)
		delete(c.heapIndex, entry.key)
	}

	return entry
}

// evict 清理过期和超出内存限制的缓存，调用此方法前必须持有锁
// atexit 表示是否需要在函数退出时执行回调
func (c *lruCache) evict(atexit bool) {
	var evictedEntries []*lruEntry

	// 1. 高效清理过期项
	now := time.Now()
	for c.expireHeap.Len() > 0 && c.expireHeap[0].expiresAt.Before(now) {
		item := heap.Pop(&c.expireHeap).(*expireItem)
		delete(c.heapIndex, item.key)

		if elem, ok := c.items[item.key]; ok {
			entry := c.list.Remove(elem).(*lruEntry)
			delete(c.items, entry.key)
			c.usedBytes -= int64(len(entry.key) + entry.value.Len())
			if atexit && c.onEvicted != nil {
				evictedEntries = append(evictedEntries, entry)
			}
		}
	}

	// 2. 根据内存限制清理最久未使用的项
	for c.maxBytes > 0 && c.usedBytes > c.maxBytes && c.list.Len() > 0 {
		elem := c.list.Front()
		if elem != nil {
			entry := c.removeElement(elem)
			if atexit && c.onEvicted != nil && entry != nil {
				evictedEntries = append(evictedEntries, entry)
			}
		}
	}

	// 在持有锁的函数退出前不执行回调
	if !atexit || c.onEvicted == nil || len(evictedEntries) == 0 {
		return
	}

	// 如果需要，释放锁并执行回调
	c.mu.Unlock()
	defer c.mu.Lock() // re-lock defer
	for _, entry := range evictedEntries {
		c.onEvicted(entry.key, entry.value)
	}
}

// cleanupLoop 定期清理过期缓存的协程
func (c *lruCache) cleanupLoop() {
	for {
		select {
		case <-c.cleanupTicker.C:
			c.mu.Lock()
			c.evict(true)
			c.mu.Unlock()
		case <-c.closeCh:
			return
		}
	}
}

// Close 关闭缓存，停止清理协程
func (c *lruCache) Close() {
	if c.cleanupTicker != nil {
		c.cleanupTicker.Stop()
		close(c.closeCh)
	}
}

// GetWithExpiration 获取缓存项及其剩余过期时间。
// 如果项存在且未过期，会更新其 LRU 位置。
// 如果项已过期，会将其从缓存中移除。
func (c *lruCache) GetWithExpiration(key string) (Value, time.Duration, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	elem, ok := c.items[key]
	if !ok {
		return nil, 0, false
	}

	// 检查该项是否有过期时间设置
	if idx, hasExp := c.heapIndex[key]; hasExp {
		expTime := c.expireHeap[idx].expiresAt
		now := time.Now()

		if now.After(expTime) {
			// 项已过期，立即将其移除
			c.removeElement(elem)
			return nil, 0, false
		}

		// 项未过期，计算剩余TTL，并更新LRU位置
		ttl := expTime.Sub(now)
		c.list.MoveToBack(elem)
		return elem.Value.(*lruEntry).value, ttl, true
	}

	// 项存在，但没有设置过期时间
	c.list.MoveToBack(elem)
	return elem.Value.(*lruEntry).value, 0, true
}

// GetExpiration 获取键的过期时间。
// 这是一个只读查询，不会改变缓存的LRU顺序。
func (c *lruCache) GetExpiration(key string) (time.Time, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 通过 heapIndex 查询是否存在过期设置
	if idx, ok := c.heapIndex[key]; ok {
		// 确保这个 key 确实存在于缓存中，防止极端情况下的不一致
		if _, exists := c.items[key]; exists {
			return c.expireHeap[idx].expiresAt, true
		}
	}

	// 如果 key 不存在或没有设置过期时间，返回零值
	return time.Time{}, false
}

// updateExpiration 内部方法，更新一个key的过期时间。调用前需持有锁。
func (c *lruCache) updateExpiration(key string, expiration time.Duration) {
	if expiration > 0 {
		expTime := time.Now().Add(expiration)
		if idx, ok := c.heapIndex[key]; ok {
			// 更新现有项
			c.expireHeap[idx].expiresAt = expTime
			heap.Fix(&c.expireHeap, idx)
		} else {
			// 添加新项
			item := &expireItem{key: key, expiresAt: expTime}
			heap.Push(&c.expireHeap, item)
			c.heapIndex[key] = item.index
		}
	} else {
		// 移除过期时间
		if idx, ok := c.heapIndex[key]; ok {
			heap.Remove(&c.expireHeap, idx)
			delete(c.heapIndex, key)
		}
	}
}

// UsedBytes 返回当前使用的字节数
func (c *lruCache) UsedBytes() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.usedBytes
}

// MaxBytes 返回最大允许字节数
func (c *lruCache) MaxBytes() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.maxBytes
}

// SetMaxBytes 设置最大允许字节数并触发淘汰
func (c *lruCache) SetMaxBytes(maxBytes int64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.maxBytes = maxBytes
	if maxBytes > 0 {
		c.evict(true)
	}
}
