package utils

import "sync"

// TypedSyncMap 泛型类型安全封装 sync.Map
type TypedSyncMap[K comparable, V any] struct {
	m sync.Map
}

// Load 获取 key 对应的值
func (m *TypedSyncMap[K, V]) Load(key K) (V, bool) {
	value, ok := m.m.Load(key)
	if !ok {
		var zero V
		return zero, false
	}
	return value.(V), true
}

// Store 设置 key 的值
func (m *TypedSyncMap[K, V]) Store(key K, value V) {
	m.m.Store(key, value)
}

// Delete 删除 key
func (m *TypedSyncMap[K, V]) Delete(key K) {
	m.m.Delete(key)
}

// LoadOrStore 如果 key 存在则返回旧值，否则存储并返回新值
func (m *TypedSyncMap[K, V]) LoadOrStore(key K, value V) (V, bool) {
	actual, loaded := m.m.LoadOrStore(key, value)
	return actual.(V), loaded
}

// LoadOrStoreFunc 如果 key 存在则返回旧值；否则执行 fn 创建新值并存储
func (m *TypedSyncMap[K, V]) LoadOrStoreFunc(key K, fn func() V) (V, bool) {
	if val, ok := m.Load(key); ok {
		return val, true
	}
	val := fn()
	return m.LoadOrStore(key, val)
}

// Range 遍历 map，f 返回 false 时中止
func (m *TypedSyncMap[K, V]) Range(f func(key K, value V) bool) {
	m.m.Range(func(k, v any) bool {
		return f(k.(K), v.(V))
	})
}
