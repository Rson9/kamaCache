package kamacache_test

import (
	"context"
	"fmt"
	kamacache2 "github.com/rson9/kamaCache/kamacache"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// getFreePort 请求操作系统分配一个空闲 TCP 端口
func getFreePort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return 0, err
	}
	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

func TestKamaCache_Integration(t *testing.T) {
	// 模拟数据库，支持并发访问
	var db sync.Map
	db.Store("player1", "100")
	db.Store("player2", "200")
	db.Store("player3", "300")

	var getterLoads atomic.Int64
	getter := kamacache2.GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		log.Printf("[Source DB] loading key: %s", key)
		getterLoads.Add(1)
		if v, ok := db.Load(key); ok {
			return []byte(v.(string)), nil
		}
		return nil, fmt.Errorf("key not found in source: %s", key)
	})

	const numNodes = 3
	nodeAddrs := make([]string, numNodes)
	nodes := make([]*kamacache2.Node, numNodes)
	groupName := "player-scores"
	var wg sync.WaitGroup

	// 启动多节点缓存服务
	for i := 0; i < numNodes; i++ {
		port, err := getFreePort()
		require.NoError(t, err)
		addr := fmt.Sprintf("localhost:%d", port)
		nodeAddrs[i] = addr

		node, err := kamacache2.NewNode(
			kamacache2.WithSelfAddr(addr),
			kamacache2.WithEtcdEndpoints([]string{"http://localhost:2379"}),
			kamacache2.WithServiceName("kamacache-test"),
		)
		require.NoError(t, err)
		nodes[i] = node

		_, err = node.NewGroup(groupName, getter, kamacache2.WithMaxBytes(2<<20))
		require.NoError(t, err)

		wg.Add(1)
		go func(n *kamacache2.Node) {
			defer wg.Done()
			if err := n.Run(); err != nil {
				// 优雅关闭时忽略错误
			}
		}(node)
	}

	defer func() {
		for _, node := range nodes {
			node.Shutdown()
		}
		wg.Wait()
		log.Println("All nodes shut down.")
	}()

	log.Println("Waiting for nodes to discover each other...")
	time.Sleep(3 * time.Second) // 等待节点通过 etcd 发现彼此

	ctx := context.Background()
	group := nodes[0].GetGroup(groupName)
	require.NotNil(t, group)

	// --- 测试用例 ---

	t.Run("Get: Cache miss triggers source load", func(t *testing.T) {
		getterLoads.Store(0)

		val, err := group.Get(ctx, "player1")
		require.NoError(t, err)
		require.Equal(t, "100", val.String())
		require.Equal(t, int64(1), getterLoads.Load(), "Getter should be called once")
	})

	t.Run("Get: Local cache hit avoids source load", func(t *testing.T) {
		getterLoads.Store(0)

		val, err := group.Get(ctx, "player1")
		require.NoError(t, err)
		require.Equal(t, "100", val.String())
		require.Equal(t, int64(0), getterLoads.Load(), "Getter should not be called on local cache hit")
	})

	t.Run("Get: Remote cache hit via peer", func(t *testing.T) {
		getterLoads.Store(0)

		val, err := group.Get(ctx, "player2")
		require.NoError(t, err)
		require.Equal(t, "200", val.String())
		require.Equal(t, int64(1), getterLoads.Load(), "Getter should be called once by peer")

		// 再次访问，命中本地缓存
		val, err = group.Get(ctx, "player2")
		require.NoError(t, err)
		require.Equal(t, "200", val.String())
		require.Equal(t, int64(1), getterLoads.Load(), "Getter should not be called again")
	})

	t.Run("Set: Propagate to peers", func(t *testing.T) {
		getterLoads.Store(0)
		key := "new_player"
		val := "999"

		err := group.Set(key, []byte(val))
		require.NoError(t, err)
		time.Sleep(1 * time.Second) // 等待异步同步

		// 其它节点应能读到新值
		for i := 1; i < numNodes; i++ {
			remoteGroup := nodes[i].GetGroup(groupName)
			v, err := remoteGroup.Get(ctx, key)
			require.NoError(t, err)
			require.Equal(t, val, v.String())
		}
	})

	t.Run("Delete: Propagate to peers", func(t *testing.T) {
		key := "player3"

		getterLoads.Store(0)
		val, err := group.Get(ctx, key)
		require.NoError(t, err)
		require.Equal(t, "300", val.String())
		require.Equal(t, int64(1), getterLoads.Load())

		err = group.Delete(key)
		require.NoError(t, err)
		time.Sleep(200 * time.Millisecond) // 等待异步删除同步

		// 删除后，其他节点再访问会回源 Getter
		remoteGroup := nodes[2].GetGroup(groupName)
		v, err := remoteGroup.Get(ctx, key)
		require.NoError(t, err)
		require.Equal(t, "300", v.String())
		require.Equal(t, int64(2), getterLoads.Load())
	})
}
