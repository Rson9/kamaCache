package group

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	cache "github.com/rson9/KamaCache-Go/cache"
	"github.com/rson9/KamaCache-Go/peer"
	"github.com/rson9/KamaCache-Go/singleflight"
	"github.com/rson9/KamaCache-Go/utils"
	"github.com/sirupsen/logrus"
	"go.uber.org/zap"
)

var (
	groupsMu sync.RWMutex
	groups   = make(map[string]*Group)
)

// ErrKeyRequired 键不能为空错误
var ErrKeyRequired = errors.New("key is required")

// ErrValueRequired 值不能为空错误
var ErrValueRequired = errors.New("value is required")

// ErrGroupClosed 组已关闭错误
var ErrGroupClosed = errors.New("cache group is closed")

// ErrKeyNotFound 键未找到错误
var ErrKeyNotFound = errors.New("key not found")

// Getter 加载键值的回调函数接口
type Getter interface {
	Get(ctx context.Context, key string) ([]byte, error)
}

// GetterFunc 函数类型实现 Getter 接口
type GetterFunc func(ctx context.Context, key string) ([]byte, error)

// Get 实现 Getter 接口
func (f GetterFunc) Get(ctx context.Context, key string) ([]byte, error) {
	return f(ctx, key)
}

// Group 是一个缓存命名空间
type Group struct {
	name       string
	getter     Getter
	mainCache  *cache.Cache       // 本地缓存
	peers      *peer.ClientPicker // 节点发现器
	loader     *singleflight.Group
	expiration time.Duration // 缓存过期时间，0表示永不过期
	closed     int32         // 原子变量，标记组是否已关闭
	stats      groupStats    // 统计信息
}

// groupStats 保存组的统计信息
type groupStats struct {
	loads        int64 // 加载次数
	localHits    int64 // 本地缓存命中次数
	localMisses  int64 // 本地缓存未命中次数
	peerHits     int64 // 从对等节点获取成功次数
	peerMisses   int64 // 从对等节点获取失败次数
	loaderHits   int64 // 从加载器获取成功次数
	loaderErrors int64 // 从加载器获取失败次数
	loadDuration int64 // 加载总耗时（纳秒）
}

// GroupOption 定义Group的配置选项
type GroupOption func(*Group)

// WithExpiration 设置缓存过期时间
func WithExpiration(d time.Duration) GroupOption {
	return func(g *Group) {
		g.expiration = d
	}
}

// WithPeers 设置分布式节点
func WithPeers(peers *peer.ClientPicker) GroupOption {
	return func(g *Group) {
		g.peers = peers
	}
}

// WithCacheOptions 设置缓存选项
func WithCacheOptions(opts cache.CacheOptions) GroupOption {
	return func(g *Group) {
		g.mainCache = cache.NewCache(opts)
	}
}

// NewGroup 创建一个新的 Group 实例
func NewGroup(name string, cacheBytes int64, getter Getter, opts ...GroupOption) *Group {
	if getter == nil {
		panic("nil Getter")
	}

	// 创建默认缓存选项
	cacheOpts := cache.DefaultCacheOptions()
	cacheOpts.MaxBytes = cacheBytes

	g := &Group{
		name:      name,
		getter:    getter,
		mainCache: cache.NewCache(cacheOpts),
		loader:    &singleflight.Group{},
	}

	// 应用选项
	for _, opt := range opts {
		opt(g)
	}

	// 注册到全局组映射
	groupsMu.Lock()
	defer groupsMu.Unlock()

	if _, exists := groups[name]; exists {
		logrus.Warnf("Group with name %s already exists, will be replaced", name)
	}

	groups[name] = g
	logrus.Infof("Created cache group [%s] with cacheBytes=%d, expiration=%v", name, cacheBytes, g.expiration)

	return g
}

// Name 获取组名称
func (g *Group) Name() string {
	return g.name
}

// GetGroup 根据名称获取已注册的缓存组
func GetGroup(name string) *Group {
	groupsMu.RLock()
	defer groupsMu.RUnlock()
	return groups[name]
}

// Get 从缓存或远程节点获取数据。
// 它处理本地缓存查找，并在缓存未命中时协调从其他节点加载。
// Get 封装了获取缓存、请求来源判断、fallback 加载的完整流程
func (g *Group) Get(ctx context.Context, key string) (cache.ByteView, bool) {
	if g == nil || key == "" || atomic.LoadInt32(&g.closed) == 1 {
		return cache.ByteView{}, false
	}

	// 优先查本地缓存
	view, ok := g.mainCache.Get(key)
	if ok {
		atomic.AddInt64(&g.stats.localHits, 1)
		return view, true
	}
	atomic.AddInt64(&g.stats.localMisses, 1)

	// 如果是 peer 节点发起的请求，不进行 fallback，直接返回 miss
	if utils.IsFromPeer(ctx) {
		return cache.ByteView{}, false
	}

	// 如果是直接请求，尝试从其他节点或 fallback 加载
	return g.load(ctx, key)
}

// Set 设置缓存值
func (g *Group) Set(ctx context.Context, key string, value []byte) error {
	if atomic.LoadInt32(&g.closed) == 1 {
		return ErrGroupClosed
	}
	if key == "" {
		return ErrKeyRequired
	}
	if len(value) == 0 {
		return ErrValueRequired
	}

	view := cache.NewByteView(value)

	// 写入本地缓存，支持过期策略
	if g.expiration > 0 {
		g.mainCache.AddWithExpiration(key, view, time.Now().Add(g.expiration))
	} else {
		g.mainCache.Add(key, view)
	}

	if !utils.IsFromPeer(ctx) {
		g.startPeerSync(ctx, "set", key, value)
	}

	return nil
}

// Delete 删除缓存值
func (g *Group) Delete(ctx context.Context, key string) error {
	if atomic.LoadInt32(&g.closed) == 1 {
		return ErrGroupClosed
	}
	if key == "" {
		return ErrKeyRequired
	}

	// 删除本地缓存
	g.mainCache.Delete(key)

	// 只有本地调用才会向其他节点同步，避免来自 Peer 的请求再次触发同步
	if !utils.IsFromPeer(ctx) {
		g.startPeerSync(ctx, "delete", key, nil)
	}

	return nil
}

// 异步启动节点同步，不做超时控制，由 syncToPeers 内部控制
func (g *Group) startPeerSync(ctx context.Context, op, key string, value []byte) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				zap.L().Error("syncToPeers panic", zap.Any("err", r))
			}
		}()
		g.syncToPeers(ctx, op, key, value)
	}()
}

// syncToPeers 同步操作到节点
func (g *Group) syncToPeers(ctx context.Context, op, key string, value []byte) {
	// 如果节点发现器是空的，那直接返回，打印Debug
	if g.peers == nil {
		logrus.Debugf("[KamaCache] group '%s': no peers available for syncing", g.name)
		return
	}

	// 选择到对应的节点
	peer, err := g.peers.PickPeer(key)
	if err != nil {
		logrus.Debugf("[KamaCache] group '%s': no peer picked for key: %s", g.name, key)
		return
	}
	// 如果节点是自己不用改了
	if peer.Addr() == g.peers.Addr() {
		logrus.Debugf("[KamaCache] group '%s': skip syncing to self", g.name)
		return
	}
	logrus.Infof("[KamaCache] sync op='%s' key='%s' from='%s' to='%s'", op, key, g.peers.Addr(), peer.Addr())

	// 根据不同的op进行不同的远程调用，更新哈希环上对应节点的值
	var ok bool
	switch op {
	case "set":
		ok = peer.Set(ctx, g.name, key, value)
	case "delete":
		ok = peer.Delete(ctx, g.name, key)
	default:
		logrus.Warnf("[KamaCache] group '%s': unknown sync operation '%s' for key '%s'", g.name, op, key)
		return
	}

	if !ok {
		logrus.WithFields(logrus.Fields{
			"group": g.name,
			"key":   key,
			"op":    op,
			"peer":  peer.Addr(),
			"err":   err,
		}).Warn("[KamaCache] peer sync failed")
	}
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

// load 从其他节点加载数据
func (g *Group) load(ctx context.Context, key string) (cache.ByteView, bool) {
	// 判断peers是否为空
	if g.peers == nil {
		logrus.Debug("[group.go] function:load 单节点模式，无远程节点，跳过加载")
		return cache.ByteView{}, false
	}

	// 选择一个节点加载
	peer, err := g.peers.PickPeer(key)
	if err != nil {
		logrus.Error("[group.go] function:load ", err)
		return cache.ByteView{}, false
	}
	// 如果选择到的是空节点或者是本地节点，直接返回
	if peer == nil || peer.Addr() == g.peers.Addr() {
		return cache.ByteView{}, false
	}
	// 使用 singleflight 确保并发请求只加载一次
	startTime := time.Now()
	viewi, err := g.loader.Do(ctx, key, func() (cache.ByteView, error) {
		return g.getFromPeer(ctx, peer, key)
	})

	// 记录加载时间
	loadDuration := time.Since(startTime).Nanoseconds()
	atomic.AddInt64(&g.stats.loadDuration, loadDuration)
	atomic.AddInt64(&g.stats.loads, 1)

	if err != nil {
		atomic.AddInt64(&g.stats.loaderErrors, 1)
		return cache.ByteView{}, false
	}
	atomic.AddInt64(&g.stats.loaderHits, 1)

	view := viewi.(cache.ByteView)

	// 设置到本地缓存
	if g.expiration > 0 {
		g.mainCache.AddWithExpiration(key, view, time.Now().Add(g.expiration))
	} else {
		g.mainCache.Add(key, view)
	}
	return view, true
}

// getFromPeer 发起远程调用获取数据
func (g *Group) getFromPeer(ctx context.Context, remote *peer.RemotePeer, key string) (cache.ByteView, error) {
	value, ok := remote.Get(ctx, g.name, key)
	if !ok {
		// 包含 peer 地址和 key，便于调试
		return cache.ByteView{}, fmt.Errorf("[getFromPeer] group=%s key=%s peer=%s: ",
			g.name, key, remote.Addr())
	}
	return cache.NewByteView(value), nil
}

// RegisterPeers 注册PeerPicker
func (g *Group) RegisterPeers(peers *peer.ClientPicker) {
	if g.peers != nil {
		panic("RegisterPeers called more than once")
	}
	g.peers = peers
	logrus.Infof("[KamaCache] registered peers for group [%s]", g.name)
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
