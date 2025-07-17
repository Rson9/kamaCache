package store

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"math"
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

// calculateStdDev 用于计算一组数据（桶中元素数量）的标准差
func calculateStdDev(data []int) (mean, stddev float64) {
	if len(data) == 0 {
		return 0, 0
	}

	var sum, sumSq float64
	for _, count := range data {
		sum += float64(count)
		sumSq += float64(count * count)
	}

	n := float64(len(data))
	mean = sum / n
	variance := sumSq/n - mean*mean
	return mean, math.Sqrt(variance)
}

// hashTestResult 用于存储单个哈希函数的测试结果
type hashTestResult struct {
	name       string
	mean       float64 // 平均每个桶的元素数
	stdDev     float64 // 桶元素数的标准差
	minEntries int     // 最空桶的元素数
	maxEntries int     // 最满桶的元素数
}

func TestHashFunctionDistribution(t *testing.T) {
	const (
		totalKeys   = 1000000 // 使用更多的key来放大差异
		bucketCount = 256     // 模拟一个更真实的分桶数量
	)

	keys := make([]string, totalKeys)
	for i := 0; i < totalKeys; i++ {
		keys[i] = fmt.Sprintf("a-long-prefix-for-key-%d-with-random-suffix-%d", i, rand.Int())
	}

	hashFuncs := map[string]HashFunc{
		"BKDR":         hashBKRD,
		"FNV-1a":       hashFNV,
		"Murmur3":      hashMurmur3,
		"Open-Murmur3": HashWithMurmur3Library,
	}

	results := make([]hashTestResult, 0, len(hashFuncs))

	t.Logf("开始哈希分布测试... (Keys: %d, Buckets: %d)", totalKeys, bucketCount)
	t.Log("------------------------------------------------------------------")

	for name, hf := range hashFuncs {
		t.Run(fmt.Sprintf("Distribution_%s", name), func(t *testing.T) {
			bucketDistribution := make([]int, bucketCount)
			for _, key := range keys {
				hash := hf(key)
				// 使用位运算取模，要求 bucketCount 是 2 的幂
				bucketIndex := hash & uint32(bucketCount-1)
				bucketDistribution[bucketIndex]++
			}

			mean, stdDev := calculateStdDev(bucketDistribution)
			min, max := totalKeys, 0
			for _, count := range bucketDistribution {
				if count < min {
					min = count
				}
				if count > max {
					max = count
				}
			}

			results = append(results, hashTestResult{
				name:       name,
				mean:       mean,
				stdDev:     stdDev,
				minEntries: min,
				maxEntries: max,
			})
		})
	}

	// --- 打印直观的对比报告 ---
	t.Log("\n--- 哈希函数分布均匀度对比报告 ---")
	t.Logf("理想情况: 标准差(StdDev)越小越好，代表分布越均匀。")
	t.Logf("%-10s | %-12s | %-12s | %-10s | %-10s", "哈希函数", "标准差(↓)", "平均负载", "最小负载", "最大负载")
	t.Logf("-----------|--------------|--------------|------------|------------")
	for _, r := range results {
		t.Logf("%-10s | %-12.2f | %-12.2f | %-10d | %-10d", r.name, r.stdDev, r.mean, r.minEntries, r.maxEntries)
	}
	t.Log("------------------------------------------------------------------")
	t.Log("结论: 标准差越高的函数，越容易导致部分桶过载，从而造成缓存“热点”，降低实际可用容量和命中率。")
}

func BenchmarkHashFunctions(b *testing.B) {
	// 准备一组有代表性的测试key
	testKeys := make([]string, 1000000)
	for i := range testKeys {
		testKeys[i] = fmt.Sprintf("benchmark-key-for-testing-performance-%d", i)
	}
	keyCount := len(testKeys)

	hashFuncs := map[string]HashFunc{
		"BKDR":         hashBKRD,
		"FNV-1a":       hashFNV,
		"Murmur3":      hashMurmur3,
		"Open-Murmur3": HashWithMurmur3Library,
	}

	b.Log("\n--- 哈希函数性能基准测试 ---")
	b.Log("指标(ns/op): 每次哈希操作消耗的纳秒数，越小越好。")

	for name, hf := range hashFuncs {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs() // 报告内存分配情况
			b.ResetTimer()   // 重置计时器，忽略准备阶段的开销

			// b.N 是由测试框架自动调整的循环次数
			for i := 0; i < b.N; i++ {
				// 使用 & (keyCount-1) 代替 %，更高效
				// 目的是防止编译器过度优化，并模拟随机key的访问
				_ = hf(testKeys[i&(keyCount-1)])
			}
		})
	}
}
