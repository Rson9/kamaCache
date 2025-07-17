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
	logger     *logrus.Entry //为 group 增加独立的 logger
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

type GroupOptions struct {
	Expiration time.Duration
	CacheOpts  cache.CacheOptions
	Logger     *logrus.Entry
	picker     peer.PeerPicker
}

// GroupOption 定义Group的配置选项
type GroupOption interface {
	apply(*GroupOptions)
}

type FuncGroupOption struct {
	f func(*GroupOptions)
}

func (fo FuncGroupOption) apply(option *GroupOptions) {
	fo.f(option)
}

func DefaultGroupOptions() GroupOptions {
	return GroupOptions{
		Expiration: 0,
		CacheOpts:  cache.DefaultCacheOptions(),
		Logger:     logrus.WithField("component", "group"),
	}
}

// WithPeers 设置节点发现器
func WithPicker(picker peer.PeerPicker) GroupOption {
	return FuncGroupOption{
		f: func(options *GroupOptions) {
			options.picker = picker
		},
	}
}

// WithLogger 设置日志记录器
func WithLogger(logger *logrus.Entry) GroupOption {
	return FuncGroupOption{
		f: func(options *GroupOptions) {
			options.Logger = logger
		},
	}
}

// WithCacheOptions 设置缓存选项
func WithCacheOptions(opts cache.CacheOptions) GroupOption {
	return FuncGroupOption{
		f: func(options *GroupOptions) {
			options.CacheOpts = opts
		},
	}
}

// NewGroup 创建一个新的 Group 实例
func NewGroup(name string, getter Getter, opts ...GroupOption) *Group {
	if getter == nil {
		panic("nil Getter")
	}
	if name == "" {
		panic("empty group name")
	}

	// 默认8MB
	options := DefaultGroupOptions()
	for _, opt := range opts {
		opt.apply(&options)
	}

	g := &Group{
		name:      name,
		getter:    getter,
		mainCache: cache.NewCache(options.CacheOpts),
		picker:    options.picker,
		loader:    &singleflight.Group{},
		stats:     groupStats{},
		logger:    options.Logger,
	}

	// 注册到全局
	groupsMu.Lock()
	defer groupsMu.Unlock()
	if _, exists := groups[name]; exists {
		g.logger.Warnf("Group with name %s already exists, will be replaced", name)
	}
	groups[name] = g

	return g
}
