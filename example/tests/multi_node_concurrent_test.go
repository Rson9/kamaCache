package tests

import (
	"context"
	"fmt"
	"github.com/rson9/KamaCache-Go/example"
	"github.com/rson9/KamaCache-Go/group"
	"github.com/rson9/KamaCache-Go/server"
	"github.com/stretchr/testify/require"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestMultiNodeConcurrentPerformance(t *testing.T) {
	ctx := context.Background()
	groupName := "distributed_group"

	type Node struct {
		srv *server.Server
		grp *group.Group
	}

	// 启动多个节点
	addrs := []string{
		"127.0.0.1:9101",
		"127.0.0.1:9102",
		"127.0.0.1:9103",
	}
	var nodes []Node
	for _, addr := range addrs {
		srv, grp := example.StartTestServer(t, addr, groupName)
		nodes = append(nodes, Node{srv, grp})
		defer example.StopTestServer(srv)
	}

	// 测试参数
	const (
		totalClients  = 1000
		opsPerClient  = 200
		testKeyPrefix = "concurrent_key"
		testValue     = "distributed_value"
	)

	var (
		wg         sync.WaitGroup
		successCnt int64
		setFailCnt int64
		getFailCnt int64
		totalOps   = int64(totalClients * opsPerClient)
		startTime  = time.Now()
	)

	// 并发执行读写
	for clientID := 0; clientID < totalClients; clientID++ {
		wg.Add(1)

		go func(cid int) {
			defer wg.Done()

			for j := 0; j < opsPerClient; j++ {
				key := fmt.Sprintf("%s_%d_%d", testKeyPrefix, cid, j)
				node := nodes[(cid+j)%len(nodes)] // 简单轮询选择节点

				// SET 操作
				if err := node.grp.Set(ctx, key, []byte(testValue)); err != nil {
					t.Logf("[SET FAIL] client-%d key=%s err=%v", cid, key, err)
					atomic.AddInt64(&setFailCnt, 1)
					continue
				}

				// GET 操作
				val, ok := node.grp.Get(ctx, key)
				if !ok || string(val.ByteSlice()) != testValue {
					t.Logf("[GET FAIL] client-%d key=%s got=%s", cid, key, val.ByteSlice())
					atomic.AddInt64(&getFailCnt, 1)
					continue
				}

				atomic.AddInt64(&successCnt, 1)
			}
		}(clientID)
	}

	wg.Wait()
	duration := time.Since(startTime)

	// 输出统计信息
	totalFail := setFailCnt + getFailCnt
	successRate := float64(successCnt) / float64(totalOps) * 100

	t.Logf("======== Distributed Cache Benchmark ========")
	t.Logf("Nodes count:         %d", len(nodes))
	t.Logf("Clients:             %d", totalClients)
	t.Logf("Ops per client:      %d", opsPerClient)
	t.Logf("Total operations:    %d", totalOps)
	t.Logf("Success count:       %d", successCnt)
	t.Logf("Set failed:          %d", setFailCnt)
	t.Logf("Get failed:          %d", getFailCnt)
	t.Logf("Total failed:        %d", totalFail)
	t.Logf("Success rate:        %.2f%%", successRate)
	t.Logf("Total time:          %v", duration)
	t.Logf("Avg latency per op:  %v", duration/time.Duration(totalOps))

	// 断言成功率和性能基准
	require.GreaterOrEqual(t, successRate, 90.0, "Success rate too low")
	require.Less(t, duration.Seconds(), 10.0, "Too slow for expected ops")
}
