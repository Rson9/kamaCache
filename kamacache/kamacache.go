package kamacache

import (
	"errors"
	"fmt"
	"github.com/rson9/kamaCache/internal/cache"
	"github.com/rson9/kamaCache/internal/group"
	"github.com/rson9/kamaCache/internal/peer"
	"github.com/rson9/kamaCache/internal/peer/discovery"
	"github.com/rson9/kamaCache/internal/server"
	"github.com/rson9/kamaCache/internal/store"
	"github.com/sirupsen/logrus"
	"io"
	"sync"
)

// Node 是 kamaCache 集群的节点。
type Node struct {
	mu     sync.RWMutex
	opts   NodeOptions
	server *server.Server
	picker peer.PeerPicker
	groups map[string]*Group // 使用公共的 Group 类型
	logger *logrus.Entry
	closer io.Closer // 用于统一关闭所有可关闭的组件
}

// NewNode 初始化并返回一个新节点。
func NewNode(opts ...NodeOption) (*Node, error) {
	// 应用选项
	options := defaultNodeOptions()
	for _, opt := range opts {
		opt(&options)
	}

	// 参数校验
	if options.SelfAddr == "" {
		return nil, errors.New("node option SelfAddr is required")
	}
	if len(options.EtcdEndpoints) == 0 {
		return nil, errors.New("node option EtcdEndpoints is required")
	}

	logger := options.Logger.WithField("component", "node")
	logger.Info("Initializing node...")

	// 使用一个 closer 来管理所有需要关闭的资源，确保初始化失败时能优雅退出
	closer := &resourceCloser{}
	defer func() {
		if err := recover(); err != nil || closer.err != nil {
			closer.Close() // 如果发生 panic 或错误，则关闭所有已创建的资源
		}
	}()

	// 创建服务发现
	discoverer, err := discovery.NewEtcdDiscoverer(options.EtcdEndpoints, discovery.WithServiceName(options.ServiceName), discovery.WithLogger(options.Logger))
	if err != nil {
		return nil, fmt.Errorf("failed to create etcd discoverer: %w", err)
	}
	closer.Add(discoverer)

	// 创建 Peer Picker
	picker, err := peer.NewConsistentHashPicker(options.SelfAddr, discoverer, peer.WithPickerLogger(options.Logger))
	if err != nil {
		return nil, fmt.Errorf("failed to create peer picker: %w", err)
	}
	closer.Add(picker)

	// 创建 gRPC 服务
	srv, err := server.NewServer(options.SelfAddr, options.ServiceName, server.WithEtcdEndpoints(options.EtcdEndpoints))
	if err != nil {
		return nil, fmt.Errorf("failed to create server: %w", err)
	}
	closer.Add(srv)

	node := &Node{
		opts:   options,
		picker: picker,
		server: srv,
		groups: make(map[string]*Group),
		logger: logger,
		closer: closer,
	}

	logger.Info("Node initialized successfully")
	return node, nil
}

// NewGroup 创建并注册一个新的缓存组。
func (n *Node) NewGroup(name string, getter Getter, opts ...GroupOption) (*Group, error) {
	if name == "" {
		return nil, errors.New("group name cannot be empty")
	}
	if getter == nil {
		return nil, errors.New("getter cannot be nil")
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if _, exists := n.groups[name]; exists {
		return nil, fmt.Errorf("group '%s' already exists", name)
	}

	// 1. 应用用户提供的公共配置
	publicOpts := &CacheOptions{CacheType: DefaultCacheType} // 设置默认值
	for _, opt := range opts {
		opt(publicOpts)
	}

	// 2. 【转换层】将公共配置转换为内部 Group 所需的选项
	internalGroupOpts := n.translateGroupOptions(publicOpts)

	// 3. 创建内部 Group 实例
	internalG := group.NewGroup(name, getter, internalGroupOpts...)
	if err := n.server.RegisterGroup(internalG); err != nil {
		return nil, fmt.Errorf("failed to register group with server: %w", err)
	}

	// 4. 创建并存储公共的 Group 包装器
	g := &Group{internalGroup: internalG}
	n.groups[name] = g

	n.logger.WithField("group", name).Info("Group registered successfully")
	return g, nil
}

// translateGroupOptions 是将公共 CacheOptions 转换为内部 group.Option 的桥接函数。
func (n *Node) translateGroupOptions(publicOpts *CacheOptions) []group.GroupOption {
	var internalOpts []group.GroupOption

	// 将公共的字符串类型转换为内部的枚举类型
	var internalCacheType store.CacheType
	switch publicOpts.CacheType {
	case LRU:
		internalCacheType = store.LRU
	case LRU2:
		internalCacheType = store.LRU2
	default:
		internalCacheType = store.LRU // 安全默认值
	}

	// 创建内部 cache.Options
	internalCacheOpts := cache.CacheOptions{
		CacheType:    internalCacheType,
		MaxBytes:     publicOpts.MaxBytes,
		OnEvicted:    publicOpts.OnEvicted,
		CleanupTime:  publicOpts.CleanupTime,
		BucketCount:  publicOpts.BucketCount,
		CapPerBucket: publicOpts.CapPerBucket,
		Level2Cap:    publicOpts.Level2Cap,
	}

	// 将内部的 cache.Options 包装成 group.Option
	internalOpts = append(internalOpts, group.WithCacheOptions(internalCacheOpts))
	internalOpts = append(internalOpts, group.WithPicker(n.picker))

	return internalOpts
}

func (n *Node) GetGroup(name string) *Group {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.groups[name]
}

func (n *Node) Run() error {
	n.logger.Info("Node server starting...")
	return n.server.Start()
}

func (n *Node) Shutdown() {
	n.logger.Info("Shutting down node...")
	if err := n.closer.Close(); err != nil {
		n.logger.WithError(err).Error("Error during node shutdown")
	}
	n.logger.Info("Node shut down successfully.")
}

// resourceCloser 是一个辅助工具，用于安全地关闭多个 io.Closer
type resourceCloser struct {
	closers []io.Closer
	err     error
}

func (rc *resourceCloser) Add(c io.Closer) { rc.closers = append(rc.closers, c) }
func (rc *resourceCloser) Close() error {
	for i := len(rc.closers) - 1; i >= 0; i-- { // 以相反的顺序关闭
		if err := rc.closers[i].Close(); err != nil {
			rc.err = fmt.Errorf("failed to close resource: %w", err)
		}
	}
	return rc.err
}
