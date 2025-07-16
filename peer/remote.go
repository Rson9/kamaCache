package peer

import (
	"context"
	"fmt"

	pb "github.com/rson9/kamaCache/gen/proto/kama/v1"
	peer "github.com/rson9/kamaCache/types"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type RemotePeer struct {
	addr    string                    // 连接的grpc地址
	conn    *grpc.ClientConn          // 用于连接初始化和关闭
	grpcCli pb.KamaCacheServiceClient // gRPC 客户端
}

var _ peer.Peer = (*RemotePeer)(nil) // 编译时检查

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
		grpcCli: pb.NewKamaCacheServiceClient(conn),
	}, nil
}

// Addr 获取grpc地址
func (c *RemotePeer) Addr() string {
	return c.addr
}

// Get 获取缓存数据
func (c *RemotePeer) Get(ctx context.Context, group, key string) ([]byte, bool) {
	req := &pb.GetRequest{
		Group: group,
		Key:   key,
	}

	resp, err := c.grpcCli.Get(ctx, req)
	if err != nil {
		logrus.Errorf("[Client.Get] GRPC call failed | peer=%s group=%s key=%s, err=%s", c.addr, group, key, err)
		return nil, false
	}

	// 判空保护（grpc 层面可能返回 nil 值）
	if resp == nil {
		logrus.Errorf("[Client.Get] empty response | peer=%s group=%s key=%s",
			c.addr, group, key)
		return nil, false
	}

	val := resp.GetValue()
	if len(val) == 0 {
		logrus.Errorf("[Client.Get] empty value | peer=%s group=%s key=%s",
			c.addr, group, key)
		return nil, false
	}

	return val, true
}

// Set 设置缓存数据
func (c *RemotePeer) Set(ctx context.Context, group, key string, value []byte) bool {
	resp, err := c.grpcCli.Set(ctx, &pb.SetRequest{
		Group: group,
		Key:   key,
		Value: value,
	})
	if err != nil {
		logrus.Errorf("kamacache SET failed: %s", err)
		return false
	}
	logrus.Infof("gRPC Set response: %+v", resp)
	return true
}

// Delete 删除缓存数据
func (c *RemotePeer) Delete(ctx context.Context, group, key string) bool {
	_, err := c.grpcCli.Delete(ctx, &pb.DeleteRequest{
		Group: group,
		Key:   key,
	})
	if err != nil {
		logrus.Errorf("kamacache DELETE failed: %s", err)
		return false
	}
	return true
}

// Close 关闭 gRPC 连接
func (c *RemotePeer) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}
