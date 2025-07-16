package store

import "time"

// Value 缓存值接口
type Value interface {
	Len() int // 返回数据大小
}

// Store 缓存接口
type Store interface {
	Get(key string) (Value, bool)
	Set(key string, value Value) error
	SetWithExpiration(key string, value Value, expiration time.Duration) error
	Delete(key string) bool
	Clear()
	Len() int
	Close()
}

// CacheType 缓存类型
type CacheType string

const (
	LRU  CacheType = "lru"
	LRU2 CacheType = "lru2"
)

// Options 通用缓存配置选项
type Options struct {
	// MaxBytes 最大的缓存字节数（用于 lru）
	MaxBytes int64
	// BucketCount 缓存的桶数量（用于 lru-2）
	BucketCount uint16
	// CapPerBucket 每个桶的容量（用于 lru-2）
	CapPerBucket uint16
	// Level2Cap lru-2 中二级缓存的容量（用于 lru-2）
	Level2Cap uint16
	// CleanupInterval 清理间隔（用于 lru-2）
	CleanupInterval time.Duration
	// OnEvicted 指定缓存项被移除时的回调函数
	OnEvicted func(key string, value Value)
	// OnLenChange 当缓存长度变化时的回调函数
	OnLengthChange func(delta int)
}

func NewOptions() Options {
	return Options{
		MaxBytes:        8192,
		BucketCount:     16,
		CapPerBucket:    512,
		Level2Cap:       256,
		CleanupInterval: time.Minute,
		OnEvicted:       nil,
		OnLengthChange:  nil,
	}
}

// NewStore 创建缓存存储实例
func NewStore(cacheType CacheType, opts Options) Store {
	switch cacheType {
	case LRU2:
		return newLRU2Cache(opts)
	case LRU:
		return newLRUCache(opts)
	default:
		return newLRUCache(opts)
	}
}
