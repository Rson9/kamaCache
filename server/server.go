package server

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"sync"
	"time"

	pb "github.com/rson9/KamaCache-Go/gen/proto/kama/v1"
	"github.com/rson9/KamaCache-Go/group"
	"github.com/rson9/KamaCache-Go/registry"
	"github.com/rson9/KamaCache-Go/utils"

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
	pb.UnimplementedKamaCacheServiceServer
	addr       string                                    // 服务器地址
	svcName    string                                    // 服务名称
	groups     *utils.TypedSyncMap[string, *group.Group] // 存储缓存组
	grpcServer *grpc.Server                              // grpc 服务
	etcdCli    *clientv3.Client
	stopCh     chan struct{}
	stopOnce   sync.Once
	opts       *ServerOptions
}

// ServerOptions 服务器配置选项
type ServerOptions struct {
	EtcdConfig clientv3.Config
	MaxMsgSize int
	TLS        bool
	CertFile   string
	KeyFile    string
}

var DefaultServerOptions = &ServerOptions{
	EtcdConfig: clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		Username:    "",
		Password:    "",
		DialTimeout: 5 * time.Second,
	},
	MaxMsgSize: 4 << 20, // 4MB
}

// ServerOption 定义选项函数类型
type ServerOption func(*ServerOptions)

func WithEtcdEndpoints(endpoints []string) ServerOption {
	return func(o *ServerOptions) {
		o.EtcdConfig.Endpoints = endpoints
	}
}
func WithEtcdUsername(username string) ServerOption {
	return func(o *ServerOptions) {
		o.EtcdConfig.Username = username
	}
}
func WithEtcdPassword(password string) ServerOption {
	return func(o *ServerOptions) {
		o.EtcdConfig.Password = password
	}
}
func WithDialTimeout(timeout time.Duration) ServerOption {
	return func(o *ServerOptions) {
		o.EtcdConfig.DialTimeout = timeout
	}
}
func WithMaxMsgSize(size int) ServerOption {
	return func(o *ServerOptions) {
		o.MaxMsgSize = size
	}
}
func WithTLS(certFile, keyFile string) ServerOption {
	return func(o *ServerOptions) {
		o.TLS = true
		o.CertFile = certFile
		o.KeyFile = keyFile
	}
}

// NewServer 创建新的服务器实例
func NewServer(addr, svcName string, opts ...ServerOption) (*Server, error) {
	options := *DefaultServerOptions
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
		logrus.Infof("etcd endpoints: %v", options.EtcdConfig.Endpoints)
		etcdCli, err = clientv3.New(options.EtcdConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to create etcd client: %w", err)
		}
	}

	var grpcOpts []grpc.ServerOption
	grpcOpts = append(grpcOpts, grpc.MaxRecvMsgSize(options.MaxMsgSize))

	if options.TLS {
		creds, err := loadTLSCredentials(options.CertFile, options.KeyFile)
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
		groups:     &utils.TypedSyncMap[string, *group.Group]{},
		grpcServer: grpcServer,
		etcdCli:    etcdCli,
		stopCh:     make(chan struct{}),
		opts:       &options,
	}

	pb.RegisterKamaCacheServiceServer(grpcServer, srv)
	healthServer := health.NewServer()
	healthpb.RegisterHealthServer(grpcServer, healthServer)
	healthServer.SetServingStatus(svcName, healthpb.HealthCheckResponse_SERVING)

	return srv, nil
}

// RegisterGroup 注册缓存组
func (s *Server) RegisterGroup(g *group.Group) {
	s.groups.Store(g.Name(), g)
}

func (s *Server) Start() error {
	lis, err := net.Listen("tcp", s.addr)
	if err != nil {
		logrus.Errorf("failed to listen on %s: %v", s.addr, err)
		return err
	}

	errChan := make(chan error, 1)

	// 启动 gRPC 服务（异步）
	go func() {
		logrus.Infof("Starting gRPC server on %s...", s.addr)
		if err := s.grpcServer.Serve(lis); err != nil {
			logrus.Errorf("gRPC server error: %v", err)
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
			logrus.Errorf("gRPC server did not start listening on %s after %d attempts", s.addr, maxRetries)
			return fmt.Errorf("gRPC server listen timeout on %s", s.addr)
		}
		time.Sleep(200 * time.Millisecond)
	}

	// 启动服务注册（异步）
	if s.etcdCli != nil {
		go func() {
			if err := registry.Register(s.svcName, s.addr, s.stopCh); err != nil {
				logrus.Errorf("service registration failed: %v", err)
				s.Stop() // 触发优雅关闭
			}
		}()
	} else {
		logrus.Warnf("no ETCD client, service '%s' will not register", s.svcName)
	}

	logrus.Infof("Cache Server started at %s for service '%s'", s.addr, s.svcName)

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
		logrus.Info("Stopping Cache Server...")

		close(s.stopCh)

		if s.grpcServer != nil {
			s.grpcServer.GracefulStop()
			logrus.Info("gRPC server stopped.")
		}

		if s.etcdCli != nil {
			if err := s.etcdCli.Close(); err != nil {
				logrus.Errorf("Error closing etcd client: %v", err)
			} else {
				logrus.Info("ETCD client closed.")
			}
		}

		logrus.Info("Cache Server stopped.")
	})
}

// Get 实现 Cache 服务的 Get 方法
func (s *Server) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	// 判断传入的请求group是否存在
	group, ok := s.groups.Load(req.Group)
	// 如果不存在，返回错误
	if !ok || group == nil {
		return nil, status.Errorf(codes.NotFound, "group %s not found", req.Group)
	}
	ctx = utils.WithOrigin(ctx, utils.OriginPeer)
	// 根据找到的group,通过key去获取数据
	view, ok := group.Get(ctx, req.Key)
	// 如果没找到，返回错误
	if !ok {
		logrus.Errorf("Error getting key '%s' from group '%s'", req.Key, req.Group)
		return nil, status.Errorf(codes.Internal, "failed to get key '%s'", req.Key)
	}

	return &pb.GetResponse{Value: view.ByteSlice()}, nil
}

// Set 实现 Cache 服务的 Set 方法
func (s *Server) Set(ctx context.Context, req *pb.SetRequest) (*pb.SetResponse, error) {
	// 判断传入的请求group是否存在
	group, ok := s.groups.Load(req.Group)
	// 如果不存在，返回错误
	if !ok || group == nil {
		return nil, status.Errorf(codes.NotFound, "group %s not found", req.Group)
	}

		ctx = utils.WithOrigin(ctx, utils.OriginPeer)
	// 根据找到的group,通过key去设置数据，同时必须传入ctx，因为其中包含了是其他节点的请求
	if err := group.Set(ctx, req.Key, req.Value); err != nil {
		logrus.Errorf("Error setting key '%s' in group '%s': %v", req.Key, req.Group, err)
		return nil, status.Errorf(codes.Internal, "failed to set key '%s': %v", req.Key, err)
	}

	return &pb.SetResponse{Value: req.Value}, nil
}

// Delete 实现 Cache 服务的 Delete 方法
func (s *Server) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	group, ok := s.groups.Load(req.Group)
	if !ok || group == nil {
		return nil, status.Errorf(codes.NotFound, "group %s not found", req.Group)
	}
		ctx = utils.WithOrigin(ctx, utils.OriginPeer)
	if err := group.Delete(ctx, req.Key); err != nil {
		logrus.Errorf("Error deleting key '%s' from group '%s': %v", req.Key, req.Group, err)
		return nil, status.Errorf(codes.Internal, "failed to delete key '%s': %v", req.Key, err)
	}

	return &pb.DeleteResponse{Value: true}, nil
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
