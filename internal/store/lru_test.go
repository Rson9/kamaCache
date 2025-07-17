// 文件: lru_qps_test.go
package store

import (
	"fmt"
	"math/rand"
	"runtime"
	"testing"
	"time"
)

// ======================================================================
// 在此包含完整的 lruCache 代码，以便测试文件可以独立运行
// (在实际项目中，这部分代码应该在另一个文件中，如 lru.go)
// ======================================================================

type String string

func (s String) Len() int { return len(s) }

// ======================================================================
// 单元测试部分 - 确保正确性
// ======================================================================

func TestLRU_SetGet(t *testing.T) {
	cache := newLRUCache(Options{MaxBytes: 100})
	cache.Set("key1", String("value1"))
	val, ok := cache.Get("key1")
	if !ok || val.(String) != "value1" {
		t.Fatalf("Set/Get failed, expected 'value1', got %v", val)
	}
	if _, ok := cache.Get("key2"); ok {
		t.Fatal("Expected key2 to not exist")
	}
}

func TestLRU_Eviction(t *testing.T) {
	var evictedKey string
	var evictedValue Value
	onEvicted := func(key string, value Value) {
		evictedKey = key
		evictedValue = value
	}

	// key(4) + val(6) = 10 bytes
	cache := newLRUCache(Options{MaxBytes: 20, OnEvicted: onEvicted})
	cache.Set("key1", String("val_01")) // 使用 10 bytes
	cache.Set("key2", String("val_02")) // 使用 10 bytes

	if cache.usedBytes != 20 {
		t.Fatalf("Expected usedBytes 20, got %d", cache.usedBytes)
	}

	cache.Set("key3", String("val_03")) // 添加此项会触发淘汰

	if _, ok := cache.Get("key1"); ok {
		t.Fatal("Expected key1 to be evicted")
	}
	if evictedKey != "key1" || evictedValue.(String) != "val_01" {
		t.Fatalf("Eviction callback failed. Evicted key: %s, val: %s", evictedKey, evictedValue)
	}
}

func TestLRU_Expiration(t *testing.T) {
	cache := newLRUCache(Options{MaxBytes: 100})
	cache.SetWithExpiration("expiring_key", String("data"), 10*time.Millisecond)

	time.Sleep(20 * time.Millisecond)

	if _, ok := cache.Get("expiring_key"); ok {
		t.Fatal("Expected expiring_key to be expired and gone")
	}
}

func TestLRU_GetWithExpiration_RemovesExpired(t *testing.T) {
	cache := newLRUCache(Options{MaxBytes: 100})
	cache.SetWithExpiration("key", String("data"), 10*time.Millisecond) // 设置

	if cache.Len() != 1 {
		t.Fatal("Expected cache length to be 1")
	}

	time.Sleep(20 * time.Millisecond) // 等待过期

	// GetWithExpiration 应该能发现它过期了
	_, _, ok := cache.GetWithExpiration("key")
	if ok {
		t.Fatal("Expected GetWithExpiration to return false for expired item")
	}

	// 最重要的是，这次访问应该已经将它清除了
	if cache.Len() != 0 {
		t.Fatalf("Expected expired item to be removed after GetWithExpiration, len is %d", cache.Len())
	}
}

// ======================================================================
// 基准测试与QPS计算部分
// ======================================================================

const (
	benchmarkCacheSize = 10000 // 缓存中预填充的条目数
	keySpace           = 20000 // 生成的key的总范围，模拟命中和未命中
)

// benchmarkSetup 创建并预填充一个用于基准测试的缓存实例
func benchmarkSetup(b *testing.B) *lruCache {
	b.Helper()
	cache := newLRUCache(Options{
		// 设置一个足够大的maxBytes，避免在测试中发生非预期的淘汰
		MaxBytes: int64(benchmarkCacheSize * 20),
	})

	// 预填充数据
	for i := 0; i < benchmarkCacheSize; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := String("value")
		cache.Set(key, value)
	}

	b.ReportAllocs()
	b.ResetTimer()
	return cache
}

// BenchmarkReadHeavy_80_20 模拟80%读，20%写的负载
func BenchmarkReadHeavy_80_20(b *testing.B) {
	cache := benchmarkSetup(b)
	defer cache.Close()

	b.RunParallel(func(pb *testing.PB) {
		// 每个并发的 goroutine 使用自己的随机数源，避免锁竞争
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		for pb.Next() {
			op := r.Intn(100)
			keyIdx := r.Intn(keySpace)
			key := fmt.Sprintf("key-%d", keyIdx)

			if op < 80 { // 80% 的概率是读操作
				cache.Get(key)
			} else { // 20% 的概率是写操作
				cache.Set(key, String("new_value"))
			}
		}
	})
}

// BenchmarkWriteHeavy_20_80 模拟20%读，80%写的负载
func BenchmarkWriteHeavy_20_80(b *testing.B) {
	cache := benchmarkSetup(b)
	defer cache.Close()

	b.RunParallel(func(pb *testing.PB) {
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		for pb.Next() {
			op := r.Intn(100)
			keyIdx := r.Intn(keySpace)
			key := fmt.Sprintf("key-%d", keyIdx)

			if op < 20 { // 20% 读
				cache.Get(key)
			} else { // 80% 写
				cache.Set(key, String("another_value"))
			}
		}
	})
}

// BenchmarkReadHit_100 模拟100%读且全部命中的负载
func BenchmarkReadHit_100(b *testing.B) {
	cache := benchmarkSetup(b)
	defer cache.Close()

	b.RunParallel(func(pb *testing.PB) {
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		for pb.Next() {
			// keyIdx 范围是 [0, benchmarkCacheSize-1]，确保100%命中
			keyIdx := r.Intn(benchmarkCacheSize)
			key := fmt.Sprintf("key-%d", keyIdx)
			cache.Get(key)
		}
	})
}

// TestQPS 是一个特殊的“测试”，它不是用来验证正确性的，
// 而是用来运行基准测试并以友好的方式展示QPS结果。
func TestQPS(t *testing.T) {
	fmt.Println("==========================================================")
	fmt.Printf("开始在本机上运行基准测试以计算QPS... (CPU核心数: %d)\n", runtime.NumCPU())
	fmt.Println("注意: 结果与您的主机配置(CPU, 内存)密切相关。")
	fmt.Println("==========================================================")

	runAndReportQPS(t, "80%读/20%写 (真实世界典型负载)", BenchmarkReadHeavy_80_20)
	runAndReportQPS(t, "20%读/80%写 (写入密集型负载)", BenchmarkWriteHeavy_20_80)
	runAndReportQPS(t, "100% 读(全部命中, 理想情况)", BenchmarkReadHit_100)
}

func runAndReportQPS(t *testing.T, name string, benchFunc func(b *testing.B)) {
	t.Helper()
	fmt.Printf("\n--- 正在运行负载模型: %s ---\n", name)

	// testing.Benchmark 会运行多次以得到一个稳定的结果
	result := testing.Benchmark(benchFunc)

	// 从结果中获取正确的“纳秒/每操作”
	nsPerOp := result.NsPerOp()

	// QPS = 1秒 / 平均每次操作耗时
	// 1秒 = 1,000,000,000 纳秒
	qps := 1e9 / float64(nsPerOp)

	// 【修正点】打印正确的单次操作平均耗时
	fmt.Printf("基准测试总运行次数: %d\n", result.N)              // 显示总共跑了多少次操作
	fmt.Printf("基准测试总耗时: %v\n", result.T)                // 显示总耗时
	fmt.Printf("平均每次操作耗时: %v\n", time.Duration(nsPerOp)) // 正确的平均耗时
	fmt.Printf("内存分配次数/操作: %d\n", result.AllocsPerOp())
	fmt.Printf("内存分配字节/操作: %d B\n", result.AllocedBytesPerOp())
	fmt.Println("----------------------------------------------------------")
	fmt.Printf(">> 计算得出的 QPS: %.2f ops/sec\n", qps)
	fmt.Println("----------------------------------------------------------")
}
