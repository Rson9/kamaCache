package kamacache

import (
	"github.com/rson9/kamaCache/internal/store"
	"github.com/sirupsen/logrus"
	"time"
)

// --- 常量与默认值 ---

const (
	DefaultEtcdLeaseTTL = 10 * time.Second
	DefaultServiceName  = "kamacache"
	DefaultCacheType    = LRU // 为 Group 设置默认缓存类型
)

// ======================================================================================
// Node 配置
// ======================================================================================

// NodeOptions 保存了 kamaCache 节点的所有配置。
type NodeOptions struct {
	SelfAddr      string
	EtcdEndpoints []string
	ServiceName   string
	EtcdLeaseTTL  time.Duration
	Logger        *logrus.Logger
}
type NodeOption func(*NodeOptions)

func defaultNodeOptions() NodeOptions {
	return NodeOptions{
		EtcdLeaseTTL: DefaultEtcdLeaseTTL,
		ServiceName:  DefaultServiceName,
		Logger:       logrus.StandardLogger(),
	}
}

// WithSelfAddr sets the address for the current node. This address is used for both
// listening for incoming gRPC requests and for registering with the discovery service.
func WithSelfAddr(addr string) NodeOption {
	return func(o *NodeOptions) {
		o.SelfAddr = addr
	}
}

// WithEtcdEndpoints sets the etcd endpoints for service discovery. At least one
// endpoint is required for the node to join the cluster.
func WithEtcdEndpoints(endpoints []string) NodeOption {
	return func(o *NodeOptions) {
		o.EtcdEndpoints = endpoints
	}
}

// WithServiceName sets the service name prefix used in etcd. All nodes using the
// same service name will belong to the same cluster.
func WithServiceName(name string) NodeOption {
	return func(o *NodeOptions) {
		o.ServiceName = name
	}
}

// WithEtcdLeaseTTL sets the time-to-live for the etcd registration lease. If the node
// fails to renew the lease within this duration, it will be considered offline.
func WithEtcdLeaseTTL(ttl time.Duration) NodeOption {
	return func(o *NodeOptions) {
		o.EtcdLeaseTTL = ttl
	}
}

// WithLogger sets a custom logger for the node and all its internal components.
// If not set, a default logrus logger will be used.
func WithLogger(logger *logrus.Logger) NodeOption {
	return func(o *NodeOptions) {
		o.Logger = logger
	}
}

// ======================================================================================
// Group & Cache 配置 (重大重构)
// ======================================================================================

// CacheType 定义了支持的缓存驱逐策略类型。这是公共 API 的一部分。
type CacheType string

const (
	LRU  CacheType = "lru"
	LRU2 CacheType = "lru2"
)

// CacheOptions 定义了【公共】的缓存配置选项。
// 这个结构体对用户可见，但不包含任何 internal 类型。
type CacheOptions struct {
	MaxBytes     int64
	CacheType    CacheType
	OnEvicted    func(key string, value store.Value) // store.Value 必须是导出的
	CleanupTime  time.Duration
	BucketCount  uint16
	CapPerBucket uint16
	Level2Cap    uint16
}

// GroupOption 是一个用于配置 Group 的函数类型。
type GroupOption func(*CacheOptions)

// WithMaxBytes 设置 Group 的最大缓存大小（字节）。
func WithMaxBytes(bytes int64) GroupOption {
	return func(o *CacheOptions) {
		o.MaxBytes = bytes
	}
}

// WithCacheType 设置 Group 的缓存驱逐策略。
func WithCacheType(cacheType CacheType) GroupOption {
	return func(o *CacheOptions) {
		o.CacheType = cacheType
	}
}

// WithOnEvicted 设置 Group 的缓存条目驱逐时的回调函数。
func WithOnEvicted(fn func(key string, value store.Value)) GroupOption {
	return func(o *CacheOptions) {
		o.OnEvicted = fn
	}
}
