package tests

import (
	"context"
	"github.com/rson9/kamaCache/example"
	pb "github.com/rson9/kamaCache/gen/proto/kama/v1"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSingleNodeBasicFunctions(t *testing.T) {
	ctx := context.Background()
	addr := "127.0.0.1:9101"
	groupName := "basic_test_group"

	server, grp := example.StartTestServer(t, addr, groupName)
	defer example.StopTestServer(server)

	// 1. Set & Get 基本功能测试
	key := "test-key"
	val := "test-value"

	err := grp.Set(ctx, key, []byte(val))
	require.NoError(t, err, "Set操作失败")

	got, ok := grp.Get(ctx, key)
	require.True(t, ok, "Get操作未找到key")
	require.Equal(t, []byte(val), got.ByteSlice(), "Get结果与Set值不一致")

	// 2. Delete 测试
	err = grp.Delete(ctx, key)
	require.NoError(t, err, "Delete操作失败")

	got, ok = grp.Get(ctx, key)
	require.False(t, ok, "Delete后仍能获取到key")

	// 3. 获取不存在的 key
	got, ok = grp.Get(ctx, "non-existent")
	require.False(t, ok, "非存在key应返回false")
	require.Zero(t, got.Len(), "非存在key的值长度应为0")

	// 4. 非法 group 测试
	_, err = server.Get(ctx, &pb.GetRequest{
		Group: "unknown-group",
		Key:   "any",
	})
	require.Error(t, err, "非存在group应返回error")

	// 5. 设置空 key 测试（看服务如何处理，按需求可以接受或报错）
	err = grp.Set(ctx, "", []byte("some"))
	require.Error(t, err, "空key写入失败")

	got, ok = grp.Get(ctx, "")
	require.False(t, ok, "空key读取失败")

	// 6. 重复写入覆盖测试（幂等性）
	key = "repeated-key"
	val1 := "v1"
	val2 := "v2"
	_ = grp.Set(ctx, key, []byte(val1))
	_ = grp.Set(ctx, key, []byte(val2))

	got, ok = grp.Get(ctx, key)
	require.True(t, ok)
	require.Equal(t, []byte(val2), got.ByteSlice(), "重复写入未生效")

	// 9. 启动 → 停止 → 再请求应失败
	example.StopTestServer(server)
	_, ok = grp.Get(ctx, "any")
	require.False(t, ok, "服务停止后仍能读取，应失败")
}
