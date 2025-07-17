package group

import (
	"context"
	"github.com/rson9/kamaCache/internal/cache"
	"github.com/rson9/kamaCache/internal/peer"
	"github.com/rson9/kamaCache/internal/singleflight"
	"github.com/sirupsen/logrus"
	"time"
)

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
	mainCache  *cache.Cache    // 本地缓存
	picker     peer.PeerPicker // 节点发现器
	loader     *singleflight.Group
	expiration time.Duration // 缓存过期时间，0表示永不过期
	closed     int32         // 原子变量，标记组是否已关闭
	stats      groupStats    // 统计信息
	logger     *logrus.Entry // ★ 新增: 为 group 增加独立的 logger
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

// WithPeers 设置节点发现器
func WithPeers(picker peer.PeerPicker) GroupOption {
	return func(g *Group) {
		g.picker = picker
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
		logger:    logrus.WithField("group", name),
	}

	// 应用选项
	for _, opt := range opts {
		opt(g)
	}

	// 注册到全局组映射
	groupsMu.Lock()
	defer groupsMu.Unlock()
	if _, exists := groups[name]; exists {
		g.logger.Warnf("Group with name %s already exists, will be replaced", name)
	}
	groups[name] = g
	g.logger.Infof("Cache group created (cacheBytes=%d, expiration=%v)", cacheBytes, g.expiration)

	return g
}
