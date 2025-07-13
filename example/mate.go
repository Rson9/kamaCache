package example

import (
	"context"
	"errors"
	"github.com/rson9/KamaCache-Go/group"
	"testing"

	"github.com/rson9/KamaCache-Go/peer"
	"github.com/rson9/KamaCache-Go/server"
	"github.com/stretchr/testify/require"
)

type TestClient struct {
	peer *peer.RemotePeer
}

func (c *TestClient) Get(ctx context.Context, group, key string) ([]byte, bool) {
	return c.peer.Get(ctx, group, key)
}

func (c *TestClient) Set(ctx context.Context, group, key string, val []byte) error {
	if !c.peer.Set(ctx, group, key, val) {
		return errFailedSet
	}
	return nil
}

func (c *TestClient) Delete(ctx context.Context, group, key string) error {
	if !c.peer.Delete(ctx, group, key) {
		return errFailedDelete
	}
	return nil
}

func StartTestServer(t *testing.T, addr, groupName string) (*server.Server, *group.Group) {
	t.Helper()
	srv, err := server.NewServer(addr, "kama-cache")
	require.NoError(t, err)

	grp := group.NewGroup(groupName, 2<<20, group.GetterFunc(
		func(ctx context.Context, key string) ([]byte, error) {
			return nil, nil
		})) // 2MB
	picker, err := peer.NewClientPicker(addr)

	grp.RegisterPeers(picker)
	srv.RegisterGroup(grp)

	go func() {
		err := srv.Start()
		if err != nil {
			t.Logf("server stopped: %v", err)
		}
	}()
	// 等待启动完成（简单阻塞）
	// 生产环境可用更精准的同步机制
	return srv, grp
}

func StopTestServer(srv *server.Server) {
	if srv != nil {
		srv.Stop()
	}
}

var (
	errFailedSet    = errors.New("set failed")
	errFailedDelete = errors.New("delete failed")
)
