package server

import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/rson9/kamaCache/internal/cache"
	"github.com/rson9/kamaCache/internal/gen/proto/kama/v1"
	"github.com/rson9/kamaCache/internal/group"
	"github.com/rson9/kamaCache/internal/registry"
	utils2 "github.com/rson9/kamaCache/internal/utils"
	"net"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

// Server 定义缓存服务器
type Server struct {
	kamapb.UnimplementedKamaCacheServiceServer
	addr       string                                     // 服务器地址
	svcName    string                                     // 服务名称
	groups     *utils2.TypedSyncMap[string, *group.Group] // 存储缓存组
	grpcServer *grpc.Server                               // grpc 服务
	etcdCli    *clientv3.Client
	stopCh     chan struct{}
	stopOnce   sync.Once
	opts       *ServerOptions
	logger     *logrus.Entry
}

type TLSConfig struct {
	Enabled  bool
	CertFile string
	KeyFile  string
}

// ServerOptions 服务器配置选项
type ServerOptions struct {
	EtcdConfig clientv3.Config
	MaxMsgSize int
	TLS        TLSConfig
}

func DefaultServerOptions() ServerOptions {
	return ServerOptions{
		EtcdConfig: clientv3.Config{
			Endpoints:   []string{"localhost:2379"},
			DialTimeout: 5 * time.Second,
		},
		MaxMsgSize: 4 << 20, // 4MB
		TLS: TLSConfig{
			Enabled:  false,
			CertFile: "",
			KeyFile:  "",
		},
	}
}

// ServerOption 定义选项函数类型
type ServerOption func(*ServerOptions)

func WithTLS(certFile, keyFile string) ServerOption {
	return func(o *ServerOptions) {
		o.TLS = TLSConfig{
			Enabled:  true,
			CertFile: certFile,
			KeyFile:  keyFile,
		}
	}
}

func WithEtcdEndpoints(endpoints []string) ServerOption {
	return func(o *ServerOptions) {
		o.EtcdConfig.Endpoints = endpoints
	}
}

func WithMaxMsgSize(size int) ServerOption {
	return func(o *ServerOptions) {
		o.MaxMsgSize = size
	}
}

func NewServer(addr, svcName string, opts ...ServerOption) (*Server, error) {
	options := DefaultServerOptions()
	for _, opt := range opts {
		opt(&options)
	}

	if addr == "" {
		return nil, fmt.Errorf("server address is required")
	}
	if svcName == "" {
		return nil, fmt.Errorf("service name is required")
	}

	var etcdCli *clientv3.Client
	var err error
	if len(options.EtcdConfig.Endpoints) > 0 {
		etcdCli, err = clientv3.New(options.EtcdConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to create etcd client: %w", err)
		}
	}

	var grpcOpts []grpc.ServerOption
	grpcOpts = append(grpcOpts, grpc.MaxRecvMsgSize(options.MaxMsgSize))

	// TLS 控制
	if options.TLS.Enabled {
		creds, err := loadTLSCredentials(options.TLS.CertFile, options.TLS.KeyFile)
		if err != nil {
			if etcdCli != nil {
				_ = etcdCli.Close()
			}
			return nil, fmt.Errorf("failed to load TLS credentials: %w", err)
		}
		grpcOpts = append(grpcOpts, grpc.Creds(creds))
	} else {
		grpcOpts = append(grpcOpts, grpc.Creds(insecure.NewCredentials()))
	}

	grpcServer := grpc.NewServer(grpcOpts...)

	srv := &Server{
		addr:       addr,
		svcName:    svcName,
		groups:     &utils2.TypedSyncMap[string, *group.Group]{},
		grpcServer: grpcServer,
		etcdCli:    etcdCli,
		stopCh:     make(chan struct{}),
		opts:       &options,
		logger: logrus.
			StandardLogger().
			WithField("component", "server"),
	}

	kamapb.RegisterKamaCacheServiceServer(grpcServer, srv)
	healthServer := health.NewServer()
	healthpb.RegisterHealthServer(grpcServer, healthServer)
	healthServer.SetServingStatus(svcName, healthpb.HealthCheckResponse_SERVING)

	return srv, nil
}

// RegisterGroup 注册缓存组。如果该组名已存在，将返回错误。
func (s *Server) RegisterGroup(g *group.Group) error {
	name := g.Name()

	// 检查是否已存在
	if _, exists := s.groups.Load(name); exists {
		return fmt.Errorf("group '%s' 已注册，禁止重复注册", name)
	}

	// 存入缓存组
	s.groups.Store(name, g)
	return nil
}

func (s *Server) Start() error {
	lis, err := net.Listen("tcp", s.addr)
	if err != nil {
		s.logger.Errorf("failed to listen on %s: %v", s.addr, err)
		return err
	}

	errChan := make(chan error, 1)

	// 启动 gRPC 服务（异步）
	go func() {
		s.logger.Infof("Starting gRPC server on %s...", s.addr)
		if err := s.grpcServer.Serve(lis); err != nil {
			s.logger.Errorf("gRPC server error: %v", err)
			errChan <- err
			s.Stop()
		}
		close(errChan)
	}()

	// 等待端口监听成功（最大尝试5次，每次等待200ms）
	maxRetries := 5
	for i := range maxRetries {
		conn, err := net.DialTimeout("tcp", s.addr, 200*time.Millisecond)
		if err == nil {
			conn.Close()
			break // 端口已监听
		}
		if i == maxRetries-1 {
			s.logger.Errorf("gRPC server did not start listening on %s after %d attempts", s.addr, maxRetries)
			return fmt.Errorf("gRPC server listen timeout on %s", s.addr)
		}
		time.Sleep(200 * time.Millisecond)
	}

	// 启动服务注册（异步）
	if s.etcdCli != nil {
		go func() {
			if err := registry.Register(s.svcName, s.addr, s.stopCh); err != nil {
				s.logger.Errorf("service registration failed: %v", err)
				s.Stop() // 触发优雅关闭
			}
		}()
	} else {
		s.logger.Warnf("no ETCD client, service '%s' will not register", s.svcName)
	}

	s.logger.Infof("Cache Server started at %s for service '%s'", s.addr, s.svcName)

	// 等待停止信号或 gRPC 错误
	select {
	case err := <-errChan:
		return err
	case <-s.stopCh:
		return nil
	}
}

// Stop 优雅关闭服务器
func (s *Server) Stop() {
	s.stopOnce.Do(func() {
		s.logger.Info("Stopping Cache Server...")

		close(s.stopCh)

		if s.grpcServer != nil {
			s.grpcServer.GracefulStop()
			s.logger.Info("gRPC server stopped.")
		}

		if s.etcdCli != nil {
			if err := s.etcdCli.Close(); err != nil {
				s.logger.Errorf("Error closing etcd client: %v", err)
			} else {
				s.logger.Info("ETCD client closed.")
			}
		}

		s.logger.Info("Cache Server stopped.")
	})
}

// Get 实现 Cache 服务的 Get 方法
// 当一个节点（Peer A）向另一个节点（Peer B）请求数据时，
// Peer B 需要检查自己的本地缓存。如果 Peer B 的本地缓存没有，
// 它需要触发自己的 fallback（从数据库加载），这就是 group.Get 的完整功能。
func (s *Server) Get(ctx context.Context, req *kamapb.GetRequest) (*kamapb.GetResponse, error) {
	group, ok := s.groups.Load(req.Group)
	if !ok || group == nil {
		return nil, status.Errorf(codes.NotFound, "group %s not found", req.Group)
	}

	// 调用 GetLocally，只从本地缓存中获取数据，不触发新一轮同步
	view, err := group.GetLocally(ctx, req.Key)
	if err != nil {
		s.logger.WithError(err).Warnf("Failed to get key '%s' in group '%s' for peer request", req.Key, req.Group)
		return nil, status.Errorf(codes.NotFound, "key not found or error loading")
	}

	return &kamapb.GetResponse{Value: view.ByteSlice()}, nil
}

// Set 实现 Cache 服务的 Set 方法。此方法被视为对等节点间的同步操作。
func (s *Server) Set(ctx context.Context, req *kamapb.SetRequest) (*kamapb.SetResponse, error) {
	group, ok := s.groups.Load(req.Group)
	if !ok || group == nil {
		return nil, status.Errorf(codes.NotFound, "group %s not found", req.Group)
	}

	// 调用 SetLocally，只更新本地缓存，不触发新一轮同步
	group.SetLocally(req.Key, cache.NewByteView(req.Value))

	s.logger.Debugf("Handled peer SET request for key '%s' in group '%s'", req.Key, req.Group)

	// 之前返回的是 req.Value，这里改为返回一个简单的成功响应
	return &kamapb.SetResponse{}, nil
}

// Delete 实现 Cache 服务的 Delete 方法。此方法被视为对等节点间的同步操作。
func (s *Server) Delete(ctx context.Context, req *kamapb.DeleteRequest) (*kamapb.DeleteResponse, error) {
	group, ok := s.groups.Load(req.Group)
	if !ok || group == nil {
		return nil, status.Errorf(codes.NotFound, "group %s not found", req.Group)
	}

	// 调用 DeleteLocally，只删除本地缓存，不触发新一轮同步
	group.DeleteLocally(req.Key)

	s.logger.Debugf("Handled peer DELETE request for key '%s' in group '%s'", req.Key, req.Group)

	return &kamapb.DeleteResponse{Value: true}, nil
}

func loadTLSCredentials(certFile, keyFile string) (credentials.TransportCredentials, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load keypair from %s and %s: %w", certFile, keyFile, err)
	}
	return credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert},
	}), nil
}
