package cache

import (
	"slices"
	"unsafe"
)

// ByteView 提供对缓存数据的只读视图
type ByteView struct {
	// 使用不可变字节切片，通过克隆确保安全性
	data []byte
}

func NewByteView(data []byte) ByteView {
	return ByteView{data: data}
}

// Len 返回字节视图的长度
func (b ByteView) Len() int {
	return len(b.data)
}

// ByteSlice 返回字节数据的副本，确保调用方无法修改原始数据
func (b ByteView) ByteSlice() []byte {
	return cloneBytes(b.data)
}

// String 以字符串形式返回数据，不进行拷贝(高性能只读)
func (b ByteView) String() string {
	// 使用unsafe避免拷贝，前提是ByteView不可变且生命周期管理得当
	return unsafe.String(unsafe.SliceData(b.data), len(b.data))
}

// UnsafeBytes 返回原始字节切片的不可变引用（高性能，慎用）
// 仅在信任调用方不会修改数据时使用
func (b ByteView) UnsafeBytes() []byte {
	return b.data
}

// CopyString 返回字符串的拷贝（安全但有一定性能开销）
func (b ByteView) CopyString() string {
	return string(b.data)
}

// Equal 比较两个ByteView是否相同
func (b ByteView) Equal(other ByteView) bool {
	if len(b.data) != len(other.data) {
		return false
	}
	return string(b.data) == string(other.data) // 编译器会优化为内存比较
}

// Clone 创建ByteView的深拷贝
func (b ByteView) Clone() ByteView {
	return ByteView{data: cloneBytes(b.data)}
}

// cloneBytes 内部使用的字节克隆函数
func cloneBytes(data []byte) []byte {
	return slices.Clone(data)
}
