package tests

import (
	"context"
	"testing"
	"time"

	client "github.com/rson9/KamaCache-Go/peer"
)

func TestGRPCConnectivity(t *testing.T) {
	cli, err := client.NewRemotePeer("127.0.0.1:8001")
	if err != nil {
		t.Fatalf("failed to connect to gRPC server: %v", err)
	}
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// 这里调用 Get 尝试通信，但不要求一定命中缓存
	val, ok := cli.Get(ctx, "my_cache_group", "test-key")
	if !ok {
		t.Log("key missing")
	} else {
		t.Log("Get succeeded: value=", string(val))
	}
}
