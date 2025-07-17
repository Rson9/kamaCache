package kamacache

import (
	"errors"
	"fmt"
	"github.com/rson9/kamaCache/internal/cache"
	"io"
	"sync"
	"time"

	"github.com/rson9/kamaCache/internal/group"
	"github.com/rson9/kamaCache/internal/peer"
	"github.com/rson9/kamaCache/internal/peer/discovery"
	"github.com/rson9/kamaCache/internal/server"
	"github.com/sirupsen/logrus"
)

// ========= Public Types (Type Aliases) =========
// We expose internal types via aliases to avoid forcing users
// to import internal packages, which is a bad practice.

// Getter loads data for a key.
type Getter = group.Getter

// GetterFunc is an adapter to allow the use of ordinary functions as a Getter.
type GetterFunc = group.GetterFunc

// ByteView holds an immutable view of bytes.
type ByteView = cache.ByteView

// ========= Node Configuration =========

// NodeOptions holds all the configuration for a kamaCache Node.
type NodeOptions struct {
	// SelfAddr is the address (e.g., "127.0.0.1:8001") that this node will listen on.
	// This is a required field.
	SelfAddr string
	// EtcdEndpoints is a list of etcd cluster endpoints.
	// This is a required field for service discovery.
	EtcdEndpoints []string
	// EtcdLeaseTTL is the time-to-live for the node's registration in etcd.
	// Defaults to 10 seconds.
	EtcdLeaseTTL time.Duration
	// ServiceName is the prefix used in etcd for service discovery.
	// Defaults to "kamacache".
	ServiceName string
	// Logger is the logger instance for the node. Defaults to logrus.StandardLogger().
	Logger *logrus.Logger
}

// Option is a function that configures a Node.
type Option func(*NodeOptions)

// WithSelfAddr sets the address for the current node.
func WithSelfAddr(addr string) Option {
	return func(o *NodeOptions) {
		o.SelfAddr = addr
	}
}

// WithEtcdEndpoints sets the etcd endpoints for service discovery.
func WithEtcdEndpoints(endpoints []string) Option {
	return func(o *NodeOptions) {
		o.EtcdEndpoints = endpoints
	}
}

// WithEtcdLeaseTTL sets the time-to-live for the etcd registration lease.
func WithEtcdLeaseTTL(ttl time.Duration) Option {
	return func(o *NodeOptions) {
		o.EtcdLeaseTTL = ttl
	}
}

// WithServiceName sets the service name prefix used in etcd.
func WithServiceName(name string) Option {
	return func(o *NodeOptions) {
		o.ServiceName = name
	}
}

// WithLogger sets the logger for the node and its components.
func WithLogger(logger *logrus.Logger) Option {
	return func(o *NodeOptions) {
		o.Logger = logger
	}
}

// ========= The Main Node Object =========

// Node represents a single member of a kamaCache cluster.
type Node struct {
	mu     sync.RWMutex
	opts   NodeOptions
	server *server.Server
	picker peer.PeerPicker
	groups map[string]*group.Group
	logger *logrus.Entry
}

// NewNode creates and initializes a new kamaCache Node.
// It sets up the peer communication and server components but does not start them.
// Call Run() to start the node's server and enable peer communication.
func NewNode(opts ...Option) (*Node, error) {
	// 1. 应用配置和默认值
	options := NodeOptions{
		EtcdLeaseTTL: 10 * time.Second,
		ServiceName:  "kamacache",
		Logger:       logrus.StandardLogger(),
	}
	for _, opt := range opts {
		opt(&options)
	}

	// 2.验证必要配置
	if options.SelfAddr == "" {
		return nil, fmt.Errorf("self address is required")
	}
	if len(options.EtcdEndpoints) == 0 {
		return nil, fmt.Errorf("etcd endpoints are required")
	}

	logger := options.Logger.WithField("component", "node")
	logger.Infof("Initializing node at %s", options.SelfAddr)

	// 3. 创建服务发现组件
	discoverer, err := discovery.NewEtcdDiscoverer(options.EtcdEndpoints, discovery.WithServiceName(options.ServiceName), discovery.WithLogger(options.Logger))
	if err != nil {
		return nil, fmt.Errorf("failed to create etcd discoverer: %w", err)
	}

	// 4. 创建节点选择器组件
	picker, err := peer.NewConsistentHashPicker(options.SelfAddr, discoverer, peer.WithPickerLogger(options.Logger))
	if err != nil {
		discoverer.Close() // 如果 picker 创建失败，清理 discoverer
		return nil, fmt.Errorf("failed to create peer picker: %w", err)
	}

	// 5. 创建服务组件
	srv, err := server.NewServer(
		options.SelfAddr,
		options.ServiceName,
		server.WithEtcdEndpoints(options.EtcdEndpoints),
	)
	if err != nil {
		// Clean up previously created components
		picker.Close()
		return nil, fmt.Errorf("failed to create server: %w", err)
	}

	node := &Node{
		opts:   options,
		picker: picker,
		server: srv,
		groups: make(map[string]*group.Group),
		logger: logger,
	}

	return node, nil
}

// NewGroup 创建并注册一个缓存分组（Group）。
// 可在服务运行中动态创建，线程安全。
func (n *Node) NewGroup(name string, cacheBytes int64, getter Getter) (*group.Group, error) {
	if name == "" {
		return nil, errors.New("group 名称不能为空")
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	// 若已存在则返回错误，避免覆盖
	if _, exists := n.groups[name]; exists {
		return nil, fmt.Errorf("group '%s' 已存在", name)
	}

	n.logger.Infof("创建新 group: %s（容量 %d 字节）", name, cacheBytes)

	// 创建 Group，同时注入当前节点的 PeerPicker 实现跨节点访问
	g := group.NewGroup(name, cacheBytes, getter, group.WithPeers(n.picker))

	// 将新 group 注册进当前服务
	if err := n.server.RegisterGroup(g); err != nil {
		return nil, fmt.Errorf("注册 group 到 server 失败: %w", err)
	}

	// 缓存到本地 group map
	n.groups[name] = g
	return g, nil
}

// GetGroup retrieves a previously created Group by its name.
func (n *Node) GetGroup(name string) *group.Group {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.groups[name]
}

// Run starts the node's gRPC server and begins participating in the cache cluster.
// This is a blocking call. It will run until the server is stopped by Shutdown()
// or an unrecoverable error occurs.
func (n *Node) Run() error {
	n.logger.Infof("Node starting gRPC server on %s", n.opts.SelfAddr)
	return n.server.Start()
}

// Shutdown gracefully stops the node, unregistering it from service discovery
// and closing all network connections.
func (n *Node) Shutdown() {
	n.logger.Info("Shutting down node...")
	// 关闭顺序很重要

	// 1. 关闭选择器
	if n.picker != nil {
		if closer, ok := n.picker.(io.Closer); ok {
			if err := closer.Close(); err != nil {
				n.logger.WithError(err).Error("Error closing peer picker")
			}
		}
	}

	// 2. 关闭服务器
	if n.server != nil {
		n.server.Stop()
	}
	n.logger.Info("Node shut down successfully.")
}
