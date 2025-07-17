package kamacache

import (
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/rson9/kamaCache/internal/cache"
	"github.com/rson9/kamaCache/internal/group"
	"github.com/rson9/kamaCache/internal/peer"
	"github.com/rson9/kamaCache/internal/peer/discovery"
	"github.com/rson9/kamaCache/internal/server"
	"github.com/sirupsen/logrus"
)

// Getter 加载键值的回调函数接口
type Getter = group.Getter

// GetterFunc Getter的实现
type GetterFunc = group.GetterFunc

// ByteView 视图
type ByteView = cache.ByteView

// Node 节点
type Node struct {
	mu     sync.RWMutex
	opts   NodeOptions
	server *server.Server
	picker peer.PeerPicker
	groups map[string]*group.Group
	logger *logrus.Entry
}

// NewNode 初始化节点
func NewNode(opts ...NodeOption) (*Node, error) {
	// 1. 初始化默认选项并应用用户传入的 Option
	options := defaultNodeOptions()
	for _, opt := range opts {
		opt(&options)
	}

	// 2. 参数校验
	if options.SelfAddr == "" {
		return nil, errors.New("node option SelfAddr is required")
	}
	if len(options.EtcdEndpoints) == 0 {
		return nil, errors.New("node option EtcdEndpoints is required")
	}

	logger := options.Logger.WithFields(logrus.Fields{
		"component": "node",
		"self_addr": options.SelfAddr,
		"service":   options.ServiceName,
	})
	logger.Info("Initializing node...")

	// 3. 创建 service discovery 组件
	discoverer, err := discovery.NewEtcdDiscoverer(
		options.EtcdEndpoints,
		discovery.WithServiceName(options.ServiceName),
		discovery.WithLogger(options.Logger),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create etcd discoverer: %w", err)
	}

	// 4. 创建 peer picker
	picker, err := peer.NewConsistentHashPicker(
		options.SelfAddr,
		discoverer,
		peer.WithPickerLogger(options.Logger),
	)
	if err != nil {
		_ = discoverer.Close()
		return nil, fmt.Errorf("failed to create peer picker: %w", err)
	}

	// 5. 创建 gRPC server
	srv, err := server.NewServer(
		options.SelfAddr,
		options.ServiceName,
		server.WithEtcdEndpoints(options.EtcdEndpoints),
	)
	if err != nil {
		_ = picker.Close()
		_ = discoverer.Close()
		return nil, fmt.Errorf("failed to create server: %w", err)
	}

	// 6. 构造 Node 实例
	node := &Node{
		opts:   options,
		picker: picker,
		server: srv,
		groups: make(map[string]*group.Group),
		logger: logger,
	}

	logger.Info("Node initialized successfully")
	return node, nil
}

// RegisterGroup 注册一个新的 Group
func (n *Node) RegisterGroup(name string, getter Getter, opts GroupOptions) (*group.Group, error) {
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
	// 创建 Group
	g := group.NewGroup(name, getter,
		group.WithPicker(n.picker),
	)

	// 注册进 gRPC server
	if err := n.server.RegisterGroup(g); err != nil {
		return nil, fmt.Errorf("failed to register group with server: %w", err)
	}

	n.groups[name] = g
	return g, nil
}

// GetGroup 根据名称获取已注册的缓存组
func (n *Node) GetGroup(name string) *group.Group {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.groups[name]
}

// Run 启动节点
func (n *Node) Run() error {
	n.logger.Info("Node server starting...")
	return n.server.Start()
}

// Shutdown 关闭节点
func (n *Node) Shutdown() {
	n.logger.Info("Shutting down node...")

	if n.server != nil {
		n.logger.Info("Stopping gRPC server...")
		n.server.Stop()
		n.logger.Info("gRPC server stopped.")
	}

	if n.picker != nil {
		n.logger.Info("Closing peer picker and discovery...")
		if closer, ok := n.picker.(io.Closer); ok {
			if err := closer.Close(); err != nil {
				n.logger.WithError(err).Error("Error closing peer picker")
			}
		}
		n.logger.Info("Peer picker and discovery closed.")
	}

	n.logger.Info("Node shut down successfully.")
}
