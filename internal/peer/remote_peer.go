package peer

import (
	"context"
	"fmt"
	"github.com/rson9/kamaCache/internal/gen/proto/kama/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type RemotePeer struct {
	addr    string                        // 连接的grpc地址
	conn    *grpc.ClientConn              // 用于连接初始化和关闭
	grpcCli kamapb.KamaCacheServiceClient // gRPC 客户端
}

var _ PeerGetter = (*RemotePeer)(nil) // 编译时检查

func NewRemotePeer(addr string) (*RemotePeer, error) {
	dialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	conn, err := grpc.NewClient(addr, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("gRPC dial to peer %s failed: %w", addr, err)
	}

	return &RemotePeer{
		addr:    addr,
		conn:    conn,
		grpcCli: kamapb.NewKamaCacheServiceClient(conn),
	}, nil
}

// Addr 获取grpc地址
func (c *RemotePeer) Addr() string {
	return c.addr
}

// Get 获取缓存数据
func (c *RemotePeer) Get(ctx context.Context, group, key string) ([]byte, error) {
	req := &kamapb.GetRequest{
		Group: group,
		Key:   key,
	}

	resp, err := c.grpcCli.Get(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("gRPC call to Get failed for key '%s': %w", key, err)
	}

	return resp.GetValue(), nil
}

// Set 设置缓存数据
func (c *RemotePeer) Set(ctx context.Context, group, key string, value []byte) error {
	_, err := c.grpcCli.Set(ctx, &kamapb.SetRequest{
		Group: group,
		Key:   key,
		Value: value,
	})
	if err != nil {
		return fmt.Errorf("kamacache SET failed on peer %s: %w", c.addr, err)
	}
	return nil
}

// Delete 删除缓存数据
func (c *RemotePeer) Delete(ctx context.Context, group, key string) error {
	_, err := c.grpcCli.Delete(ctx, &kamapb.DeleteRequest{
		Group: group,
		Key:   key,
	})
	if err != nil {
		return fmt.Errorf("gRPC Delete failed on peer %s for key '%s': %w", c.addr, key, err)
	}
	return nil
}

// Close 关闭 gRPC 连接
func (c *RemotePeer) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}
