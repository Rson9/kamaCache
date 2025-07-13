package cache

import (
	"slices"
	"unsafe"
)

// ByteView 提供对缓存数据的只读视图
type ByteView struct {
	data []byte // 存储字节数据
}

// NewByteView 创建新的ByteView实例，始终复制传入的数据
func NewByteView(data []byte) ByteView {
	if data == nil {
		data = []byte{} // 避免存储nil切片
	}
	return ByteView{data: slices.Clone(data)}
}

// Len 返回字节视图的长度
func (b ByteView) Len() int {
	return len(b.data)
}

// ByteSlice 返回字节数据的副本，确保调用方无法修改原始数据
func (b ByteView) ByteSlice() []byte {
	return slices.Clone(b.data)
}

// String 以字符串形式返回数据，不进行拷贝(高性能只读)
func (b ByteView) String() string {
	if len(b.data) == 0 {
		return "" // 避免对空切片使用unsafe
	}
	return unsafe.String(unsafe.SliceData(b.data), len(b.data))
}

// UnsafeBytes 返回原始字节切片的不可变引用（高性能，慎用）
// 注意：调用者必须保证不修改返回的切片
func (b ByteView) UnsafeBytes() []byte {
	return b.data // 虽然返回可变类型，但文档明确禁止修改
}

// Equal 比较两个ByteView是否相同
func (b ByteView) Equal(other ByteView) bool {
	return slices.Equal(b.data, other.data)
}

// Clone 创建ByteView的深拷贝
func (b ByteView) Clone() ByteView {
	return NewByteView(b.data) // 复用NewByteView确保数据安全
}
