package store

import (
	"container/heap"
	"time"
)

type expireItem struct {
	key       string    // 缓存键
	expiresAt time.Time // 过期时间
	index     int       // 在堆中的索引
}

// expirationHeap 是一个由 *expireItem 构成的最小堆
// 它实现了 heap.Interface
type expirationHeap []*expireItem

var _ heap.Interface = (*expirationHeap)(nil)

func (h expirationHeap) Len() int { return len(h) }

// Less 比较函数，时间早的排在前面
func (h expirationHeap) Less(i, j int) bool {
	return h[i].expiresAt.Before(h[j].expiresAt)
}

func (h expirationHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

// Push 将元素推入堆
func (h *expirationHeap) Push(x any) {
	n := len(*h)
	item := x.(*expireItem)
	item.index = n
	*h = append(*h, item)
}

// Pop 从堆中弹出元素
func (h *expirationHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // 避免内存泄露
	item.index = -1 // 表示已不在堆中
	*h = old[0 : n-1]
	return item
}
