package store

import (
	"sync"
	"sync/atomic"
	"time"
)

type lru2Store struct {
	locks       []sync.Mutex
	caches      [][2]*cache
	onEvicted   func(key string, value Value)
	cleanupTick *time.Ticker
	mask        int32
	len         int64 // 总长度
	hash        HashFunc
}

func newLRU2Cache(opts Options) *lru2Store {
	if opts.BucketCount == 0 {
		opts.BucketCount = 16
	}
	if opts.CapPerBucket == 0 {
		opts.CapPerBucket = 1024
	}
	if opts.Level2Cap == 0 {
		opts.Level2Cap = 1024
	}
	if opts.CleanupInterval <= 0 {
		opts.CleanupInterval = time.Minute
	}
	if opts.Hash == nil {
		opts.Hash = HashWithMurmur3Library // 默认使用第三方实现的Murmur3
	}

	mask := maskOfNextPowOf2(opts.BucketCount)
	s := &lru2Store{
		locks:       make([]sync.Mutex, mask+1),
		caches:      make([][2]*cache, mask+1),
		onEvicted:   opts.OnEvicted,
		cleanupTick: time.NewTicker(opts.CleanupInterval),
		mask:        int32(mask),
		hash:        opts.Hash,
	}

	for i := range s.caches {
		// 【优化】在创建内部 cache 时，传入 onEvicted 回调，让底层淘汰时直接调用
		// 这比顶层管理更高效
		s.caches[i][0] = Create(opts.CapPerBucket, s.onEvicted)
		s.caches[i][1] = Create(opts.Level2Cap, s.onEvicted)
		s.caches[i][0].onLengthChange = func(delta int) {
			atomic.AddInt64(&s.len, int64(delta))
		}
		s.caches[i][1].onLengthChange = func(delta int) {
			atomic.AddInt64(&s.len, int64(delta))
		}
	}

	if opts.CleanupInterval > 0 {
		go s.cleanupLoop()
	}

	return s
}

func (s *lru2Store) Get(key string) (Value, bool) {
	idx := s.getBucketIndex(key)
	s.locks[idx].Lock()
	defer s.locks[idx].Unlock()

	l1, l2 := &s.caches[idx][0], &s.caches[idx][1] // 使用指针别名，代码更简洁
	// 首先检查L1缓存(热数据)
	if val, ok := (*l1).get(key); ok {
		return val.v, true
	}

	// 2. 检查L2缓存(冷数据)
	if val, ok := (*l2).get(key); ok {
		// ✅ 提升：从 L2 删除并添加回 L1
		(*l2).del(val.k)
		evictedNode, evicted := (*l1).put(val.k, val.v, val.expireAt)
		// 如果没有发生淘汰，则最终len不变，如果发生淘汰，则最终长度-1
		if evicted && evictedNode != nil {
			(*l2).put(evictedNode.k, evictedNode.v, evictedNode.expireAt)
		}
		return val.v, true
	}

	return nil, false
}

func (s *lru2Store) Set(key string, value Value) error {
	return s.SetWithExpiration(key, value, 0)
}

// SetWithExpiration 支持带过期时间的缓存设置
func (s *lru2Store) SetWithExpiration(key string, value Value, expiration time.Duration) error {
	expireAt := int64(0)
	if expiration > 0 {
		expireAt = Now() + expiration.Nanoseconds()
	}

	idx := s.getBucketIndex(key)
	s.locks[idx].Lock()
	defer s.locks[idx].Unlock()

	l1, l2 := &s.caches[idx][0], &s.caches[idx][1]

	// --------------------------------------------------------------------
	// 核心逻辑: 先删后加 (Remove-Then-Add)
	// --------------------------------------------------------------------

	// 1. 无条件尝试从 L1 和 L2 中删除旧的副本。
	// del 方法会自行处理 key 不存在的情况。
	// 如果 key 真的存在并被删除，del 内部的回调会正确地处理 s.len--。
	(*l1).del(key)
	(*l2).del(key)
	// 此刻，我们保证了缓存中不再有这个 key 的任何副本，
	// 并且 s.len 的计数也是完全准确的。

	// 2. 将新值/更新值放入 L1。
	// l1.put 会负责 LRU 逻辑，并可能返回一个被淘汰的节点。
	// 如果 L1 未满，put 内部的回调会正确地处理 s.len++。
	demotedNode, evicted := (*l1).put(key, value, expireAt)

	// 3. 如果 L1 淘汰了一个节点，尝试将其降级到 L2。
	if evicted && demotedNode != nil && !demotedNode.isExpired() {
		// 将从 L1 降级的有效节点放入 L2。
		// l2.put 同样可能会触发回调 s.len++ 或淘汰一个更旧的节点。
		(*l2).put(demotedNode.k, demotedNode.v, demotedNode.expireAt)
	}
	return nil
}

// Delete 的最终版本，优雅、健壮且一致
func (s *lru2Store) Delete(key string) bool {
	idx := s.getBucketIndex(key)
	s.locks[idx].Lock()
	defer s.locks[idx].Unlock()

	// 尝试从 L1 删除。
	// cache.del 内部会自动调用 onLengthChange(-1) 来更新 s.len。
	// 我们不再需要在这里手动管理长度。
	_, ok1 := s.caches[idx][0].del(key)

	// 尝试从 L2 删除。
	// 同上，回调机制会处理一切。
	_, ok2 := s.caches[idx][1].del(key)

	// 只要任意一层删除成功，整个操作就算成功。
	return ok1 || ok2
}

// Clear 实现Store接口
func (s *lru2Store) Clear() {
	for i := range s.caches {
		s.locks[i].Lock()
		s.caches[i][0].clear()
		s.caches[i][1].clear()
		s.locks[i].Unlock()
	}
	atomic.StoreInt64(&s.len, 0)
}

// Len 实现Store接口
func (s *lru2Store) Len() int {
	return int(atomic.LoadInt64(&s.len))
}

// Close 关闭缓存相关资源
func (s *lru2Store) Close() {
	if s.cleanupTick != nil {
		s.cleanupTick.Stop()
	}
}

// maskOfNextPowOf2 计算大于或等于输入值的最近 2 的幂次方减一作为掩码值
func maskOfNextPowOf2(cap uint16) uint16 {
	if cap > 0 && cap&(cap-1) == 0 {
		return cap - 1
	}

	// 通过多次右移和按位或操作，将二进制中最高的 1 位右边的所有位都填充为 1
	cap |= cap >> 1
	cap |= cap >> 2
	cap |= cap >> 4

	return cap | (cap >> 8)
}

// 使用范例：分区键计算
func (s *lru2Store) getBucketIndex(key string) uint32 {
	hash := s.hash(key)
	return hash & uint32(s.mask) // 按位与，避免除零
}

// cleanupLoop 清理过期项的定时任务
func (s *lru2Store) cleanupLoop() {
	for range s.cleanupTick.C {
		currentTime := Now()
		for i := range s.caches {
			s.locks[i].Lock()
			s.caches[i][0].removeExpired(currentTime)
			s.caches[i][1].removeExpired(currentTime)
			s.locks[i].Unlock()
		}
	}
}
