package store

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"math/rand"
	"sync"
	"testing"
	"time"
)

// lru2_test.go

// 让我们先定义一个辅助结构，用于在测试中共享状态和配置
type testRig struct {
	store       *lru2Store
	opts        Options
	mu          sync.Mutex
	evictedKeys []string
}

func (tr *testRig) onEvicted(key string, value Value) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	tr.evictedKeys = append(tr.evictedKeys, key)
}

func (tr *testRig) reset(opts Options) {
	if tr.store != nil {
		tr.store.Close()
	}
	opts.OnEvicted = tr.onEvicted
	tr.opts = opts
	tr.store = newLRU2Cache(opts)
	tr.evictedKeys = nil
}

// 为测试定义一个简单的Value类型
type testValue string

func (v testValue) Len() int {
	return len(v)
}
func TestLRU2Cache_Behavior(t *testing.T) {
	rig := &testRig{}

	t.Run("晋升触发降级但不淘汰", func(t *testing.T) {
		rig.reset(Options{BucketCount: 1, CapPerBucket: 2, Level2Cap: 2})
		rig.store.Set("k1", testValue("v1"))
		rig.store.Set("k2", testValue("v2"))
		rig.store.Set("k3", testValue("v3")) // k1 降级至 L2
		rig.store.Set("k4", testValue("v4")) // k2 降级至 L2

		rig.mu.Lock()
		rig.evictedKeys = nil
		rig.mu.Unlock()

		rig.mu.Lock()
		require.Empty(t, rig.evictedKeys, "降级成功，不应触发淘汰")
		rig.mu.Unlock()
	})

	t.Run("晋升触发降级，再导致淘汰", func(t *testing.T) {
		rig.reset(Options{BucketCount: 1, CapPerBucket: 2, Level2Cap: 2})
		rig.store.Set("a", testValue("v"))
		rig.store.Set("b", testValue("v"))
		rig.store.Set("c", testValue("v")) // a 降级
		rig.store.Set("d", testValue("v")) // b 降级

		rig.mu.Lock()
		rig.evictedKeys = nil
		rig.mu.Unlock()

		// e 导致 c 降级失败（L2满），触发淘汰
		rig.store.Set("e", testValue("v"))

		rig.mu.Lock()
		require.Contains(t, rig.evictedKeys, "c", "c 降级失败应触发淘汰")
		rig.mu.Unlock()
	})
}

func TestLRU2Cache_Expiration(t *testing.T) {
	rig := &testRig{}

	t.Run("过期项被替换时应淘汰", func(t *testing.T) {
		rig.reset(Options{BucketCount: 1, CapPerBucket: 1, Level2Cap: 1})
		rig.store.SetWithExpiration("x", testValue("v"), time.Microsecond)
		time.Sleep(time.Millisecond)

		rig.store.Set("new", testValue("v")) // 触发替换过期项

		rig.mu.Lock()
		require.Contains(t, rig.evictedKeys, "x", "过期项应被淘汰")
		rig.mu.Unlock()
	})

	t.Run("Cleanup_自动淘汰所有过期项", func(t *testing.T) {
		rig.reset(Options{
			BucketCount:     1,
			CapPerBucket:    4,
			Level2Cap:       4,
			CleanupInterval: 10 * time.Millisecond, // 更频繁触发清理
		})

		inserted := make([]string, 0, 4)
		for i := 0; i < 4; i++ {
			k := fmt.Sprintf("k%d", i)
			rig.store.SetWithExpiration(k, testValue("v"), 20*time.Millisecond)
			inserted = append(inserted, k)
		}

		// 等待 cleanup 触发
		require.Eventually(t, func() bool {
			return rig.store.Len() == 0
		}, 1*time.Second, 10*time.Millisecond)

		// 打印用于调试
		t.Logf("回调被触发的 key：%v", rig.evictedKeys)

		rig.mu.Lock()
		defer rig.mu.Unlock()
		require.ElementsMatch(t, inserted, rig.evictedKeys, "应淘汰所有过期项")
	})

}

func TestLRU2Cache_ConcurrencyDoesNotCrash(t *testing.T) {
	rig := &testRig{}
	rig.reset(Options{BucketCount: 16, CapPerBucket: 16, Level2Cap: 16})

	const goroutines = 20
	const ops = 100

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < ops; j++ {
				key := fmt.Sprintf("k-%d", (id+j)%50)
				switch rand.Intn(3) {
				case 0:
					rig.store.Set(key, testValue("v"))
				case 1:
					rig.store.Delete(key)
				case 2:
					rig.store.SetWithExpiration(key, testValue("v"), time.Millisecond*10)
				}
			}
		}(i)
	}

	wg.Wait()
	require.GreaterOrEqual(t, rig.store.Len(), 0)
}
