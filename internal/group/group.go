package group

import (
	"context"
	"github.com/rson9/kamaCache/internal/cache"
	"github.com/sirupsen/logrus"
	"sync/atomic"
	"time"
)

// Name 获取组名称
func (g *Group) Name() string {
	return g.name
}

// Get 从缓存或远程节点获取数据。
func (g *Group) Get(ctx context.Context, key string) (cache.ByteView, error) {
	if key == "" {
		return cache.ByteView{}, ErrKeyRequired
	}
	if atomic.LoadInt32(&g.closed) == 1 {
		return cache.ByteView{}, ErrGroupClosed
	}

	// 1. 查本地缓存
	if view, ok := g.mainCache.Get(key); ok {
		atomic.AddInt64(&g.stats.localHits, 1)
		return view, nil
	}
	atomic.AddInt64(&g.stats.localMisses, 1)
	g.logger.Infof("[Test] key=%s, picking peer...", key)
	// 2. 本地未命中，调用统一的 load 方法
	return g.load(ctx, key)
}

// Set 设置缓存并且同步到对等节点
func (g *Group) Set(key string, value []byte) error {
	if atomic.LoadInt32(&g.closed) == 1 {
		return ErrGroupClosed
	}
	if key == "" {
		return ErrKeyRequired
	}

	// 1. 立刻更新本地缓存
	g.SetLocally(key, cache.NewByteView(value))

	// 2. 如果存在对等节点系统，决定如何同步
	if g.picker != nil {
		// 使用后台 goroutine 异步同步，避免阻塞主流程
		go func() {
			if remote, isSelf := g.picker.PickPeer(key); !isSelf {
				// 这个 key 应该由远端节点管理，我们通知它去 Set
				syncCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()

				g.logger.Debugf("Forwarding SET for key '%s' to peer %s", key, remote.Addr())
				if err := remote.Set(syncCtx, g.name, key, value); err != nil {
					g.logger.WithError(err).Warnf("Failed to sync SET for key '%s' to peer %s", key, remote.Addr())
				}
			}
			// 如果 isSelf 为 true，说明数据本来就该归我管，无需远程同步。
		}()
	}
	return nil
}

// Delete 删除缓存并且同步到对等节点
func (g *Group) Delete(key string) error {
	if atomic.LoadInt32(&g.closed) == 1 {
		return ErrGroupClosed
	}
	if key == "" {
		return ErrKeyRequired
	}

	// 1. 立刻删除本地缓存
	g.mainCache.Delete(key)

	// 2.委托对等节点系统处理删除同步
	// 2. 决定如何同步删除
	if g.picker != nil {
		go func() {
			if remote, isSelf := g.picker.PickPeer(key); !isSelf {
				syncCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				g.logger.Debugf("Forwarding DELETE for key '%s' to peer %s", key, remote.Addr())
				if err := remote.Delete(syncCtx, g.name, key); err != nil {
					g.logger.WithError(err).Warnf("Failed to sync DELETE for key '%s' to peer %s", key, remote.Addr())
				}
			}
		}()
	}
	return nil
}

// SetLocally 是一个辅助函数，用于将数据放入本地缓存。
func (g *Group) SetLocally(key string, value cache.ByteView) {
	if atomic.LoadInt32(&g.closed) == 1 {
		return
	}
	if g.expiration > 0 {
		g.mainCache.AddWithExpiration(key, value, time.Now().Add(g.expiration))
	} else {
		g.mainCache.Add(key, value)
	}
}

// GetLocally 是为 gRPC 服务器设计的专用方法。
// 它只从本节点的缓存或数据源(getter)加载数据，绝不涉及任何远程节点查找，以防止死锁。
func (g *Group) GetLocally(ctx context.Context, key string) (cache.ByteView, error) {
	// 1. 检查本地缓存 (与你的 Get 函数开头逻辑一致)
	if view, ok := g.mainCache.Get(key); ok {
		atomic.AddInt64(&g.stats.localHits, 1)
		return view, nil
	}

	// 2. 如果本地缓存未命中，直接从数据源加载 (这部分逻辑来自你的 load 函数)
	// 注意：这里不需要再用 singleflight，因为 singleflight 的保护应该在首次请求的节点上。
	// 如果需要对数据源的重复调用也进行保护，可以在这里再次使用 singleflight，但逻辑上最外层的 singleflight 已足够。
	return g.loadLocally(ctx, key)
}

// DeleteLocally 用于仅删除本地缓存中的条目，而不触发对等节点同步。
// 这个方法主要用于响应来自对等节点的删除请求。
func (g *Group) DeleteLocally(key string) {
	if atomic.LoadInt32(&g.closed) == 1 {
		return
	}
	g.mainCache.Delete(key)
}

// load 负责在缓存未命中时，加载数据的核心逻辑。
// 它是一个协调者，决定是从远程节点加载还是从本地数据源加载。
func (g *Group) load(ctx context.Context, key string) (cache.ByteView, error) {
	// 使用 singleflight 确保对同一个 key 的加载操作（远程或本地）只会执行一次
	view, err := g.loader.Do(ctx, key, func() (cache.ByteView, error) {
		startTime := time.Now()
		defer func() {
			atomic.AddInt64(&g.stats.loadDuration, time.Since(startTime).Nanoseconds())
		}()
		atomic.AddInt64(&g.stats.loads, 1)

		// a. 尝试从远程节点获取
		if g.picker != nil {
			//1. 使用 PickPeer 进行选择
			remote, isSelf := g.picker.PickPeer(key)

			// 2. 如果选择的不是自己，则发起远程调用
			if !isSelf {
				g.logger.Debugf("Attempting to get key '%s' from peer %s", key, remote.Addr())
				bytes, err := remote.Get(ctx, g.name, key)
				if err == nil {
					// 远程获取成功
					atomic.AddInt64(&g.stats.peerHits, 1)
					bv := cache.NewByteView(bytes)
					g.SetLocally(key, bv) // 填充本地缓存
					return bv, nil
				}

				// 远程节点获取失败 (网络错误，节点宕机等)
				g.logger.WithError(err).Warnf("Failed to get key '%s' from peer %s. Falling back to local load.", key, remote.Addr())
				atomic.AddInt64(&g.stats.peerMisses, 1)
				// 容错降级：远程失败后，尝试本地加载
				return g.loadLocally(ctx, key)
			}

			//3. 如果 PickPeer 选择的是自己 (isSelf is true)
			// 流程会自然向下走到本地加载逻辑，不再需要 `errors.Is(err, peer.ErrIsSelf)` 判断
			g.logger.Debugf("Key '%s' maps to self, loading locally", key)
		}

		// b. 如果没有远程节点，或者 key 属于本节点，则从本地数据源加载
		return g.loadLocally(ctx, key)
	})

	if err != nil {
		return cache.ByteView{}, err
	}

	return view.(cache.ByteView), nil
}

// loadLocally 从用户提供的 Getter 加载数据，并填充到本地缓存。
func (g *Group) loadLocally(ctx context.Context, key string) (cache.ByteView, error) {
	g.logger.Debugf("Loading key '%s' from local getter", key)
	bytes, err := g.getter.Get(ctx, key)
	if err != nil {
		atomic.AddInt64(&g.stats.loaderErrors, 1)
		return cache.ByteView{}, err
	}
	atomic.AddInt64(&g.stats.loaderHits, 1)
	bv := cache.NewByteView(bytes)
	g.SetLocally(key, bv) // 从本地加载后，也填充缓存
	return bv, nil
}

// Stats 返回缓存统计信息
func (g *Group) Stats() map[string]any {
	stats := map[string]any{
		"name":          g.name,
		"closed":        atomic.LoadInt32(&g.closed) == 1,
		"expiration":    g.expiration,
		"loads":         atomic.LoadInt64(&g.stats.loads),
		"local_hits":    atomic.LoadInt64(&g.stats.localHits),
		"local_misses":  atomic.LoadInt64(&g.stats.localMisses),
		"peer_hits":     atomic.LoadInt64(&g.stats.peerHits),
		"peer_misses":   atomic.LoadInt64(&g.stats.peerMisses),
		"loader_hits":   atomic.LoadInt64(&g.stats.loaderHits),
		"loader_errors": atomic.LoadInt64(&g.stats.loaderErrors),
	}

	// 计算各种命中率
	totalGets := stats["local_hits"].(int64) + stats["local_misses"].(int64)
	if totalGets > 0 {
		stats["hit_rate"] = float64(stats["local_hits"].(int64)) / float64(totalGets)
	}

	totalLoads := stats["loads"].(int64)
	if totalLoads > 0 {
		stats["avg_load_time_ms"] = float64(atomic.LoadInt64(&g.stats.loadDuration)) / float64(totalLoads) / float64(time.Millisecond)
	}

	// 添加缓存大小
	if g.mainCache != nil {
		cacheStats := g.mainCache.Stats()
		for k, v := range cacheStats {
			stats["cache_"+k] = v
		}
	}

	return stats
}

// Clear 清空缓存
func (g *Group) Clear() {
	// 检查组是否已关闭
	if atomic.LoadInt32(&g.closed) == 1 {
		return
	}

	g.mainCache.Clear()
	logrus.Infof("[KamaCache] cleared cache for group [%s]", g.name)
}

// Close 关闭组并释放资源
func (g *Group) Close() error {
	// 如果已经关闭，直接返回
	if !atomic.CompareAndSwapInt32(&g.closed, 0, 1) {
		return nil
	}

	// 关闭本地缓存
	if g.mainCache != nil {
		g.mainCache.Close()
	}

	// 从全局组映射中移除
	groupsMu.Lock()
	delete(groups, g.name)
	groupsMu.Unlock()

	logrus.Infof("[KamaCache] closed cache group [%s]", g.name)
	return nil
}

// GetGroup 根据名称获取已注册的缓存组
func GetGroup(name string) *Group {
	groupsMu.RLock()
	defer groupsMu.RUnlock()
	return groups[name]
}

// ListGroups 返回所有缓存组的名称
func ListGroups() []string {
	groupsMu.RLock()
	defer groupsMu.RUnlock()

	names := make([]string, 0, len(groups))
	for name := range groups {
		names = append(names, name)
	}

	return names
}

// DestroyGroup 销毁指定名称的缓存组
func DestroyGroup(name string) bool {
	groupsMu.Lock()
	defer groupsMu.Unlock()

	if g, exists := groups[name]; exists {
		g.Close()
		delete(groups, name)
		logrus.Infof("[KamaCache] destroyed cache group [%s]", name)
		return true
	}

	return false
}

// DestroyAllGroups 销毁所有缓存组
func DestroyAllGroups() {
	groupsMu.Lock()
	defer groupsMu.Unlock()

	for name, g := range groups {
		g.Close()
		delete(groups, name)
		logrus.Infof("[KamaCache] destroyed cache group [%s]", name)
	}
}
