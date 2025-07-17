package store

import (
	"encoding/binary"
	"github.com/spaolacci/murmur3"
	"unsafe"
)

type HashFunc func(key string) uint32

const (
	murmurC1 uint32 = 0xcc9e2d51
	murmurC2 uint32 = 0x1b873593
	murmurR1 uint32 = 15
	murmurR2 uint32 = 13
	murmurM  uint32 = 5
	murmurN  uint32 = 0xe6546b64
)

// 实现了 BKDR 哈希算法，用于计算键的哈希值
func hashBKRD(s string) uint32 {
	var hash uint32 = 0
	for i := 0; i < len(s); i++ {
		hash = hash*131 + uint32(s[i])
	}
	return hash
}

func hashFNV(key string) uint32 {
	const (
		offset32 = 2166136261
		prime32  = 16777619
	)
	hash := uint32(offset32)
	for i := 0; i < len(key); i++ {
		hash ^= uint32(key[i])
		hash *= prime32
	}
	return hash
}

func hashMurmur3(key string) uint32 {
	// --- 优化点 1: 零内存分配的 string -> []byte 转换 ---
	// 使用 unsafe 包直接获取 string 底层 byte 数组的指针，避免了内存分配和数据复制。
	// 这是性能攸关代码中的标准做法。
	data := unsafe.Slice(unsafe.StringData(key), len(key))

	var hash uint32 = 0 // 使用种子(seed)初始化，这里为0
	length := len(data)
	nblocks := length / 4

	// --- 优化点 2: 使用 encoding/binary 高效读取4字节块 ---
	// 每次处理4字节（一个uint32）
	for i := 0; i < nblocks; i++ {
		// binary.LittleEndian.Uint32 通常会被编译器优化为一条单独的机器指令，
		// 远比四次单独的字节读取和位移要快。
		k := binary.LittleEndian.Uint32(data[i*4:])

		k *= murmurC1
		k = (k << murmurR1) | (k >> (32 - murmurR1)) // ROTL32
		k *= murmurC2

		hash ^= k
		hash = (hash << murmurR2) | (hash >> (32 - murmurR2)) // ROTL32
		hash = hash*murmurM + murmurN
	}

	// 处理末尾的1-3个剩余字节 (Tail)
	tailIndex := nblocks * 4
	var k1 uint32
	switch length & 3 {
	case 3:
		k1 ^= uint32(data[tailIndex+2]) << 16
		fallthrough
	case 2:
		k1 ^= uint32(data[tailIndex+1]) << 8
		fallthrough
	case 1:
		k1 ^= uint32(data[tailIndex])
		k1 *= murmurC1
		k1 = (k1 << murmurR1) | (k1 >> (32 - murmurR1))
		k1 *= murmurC2
		hash ^= k1
	}

	// 最终混淆 (Finalization mix)
	hash ^= uint32(length)
	hash ^= hash >> 16
	hash *= 0x85ebca6b
	hash ^= hash >> 13
	hash *= 0xc2b2ae35
	hash ^= hash >> 16

	return hash
}

// HashWithMurmur3Library 使用 spaolacci/murmur3 库来计算哈希值
// 这是一个高性能、零分配（对于 string）的推荐实现
func HashWithMurmur3Library(key string) uint32 {
	// spaolacci/murmur3 库内部已经处理了从 string 到 []byte 的零拷贝转换
	// 你无需手动使用 unsafe 包，就能获得最佳性能。
	// 第二个参数是种子(seed)，你可以保持为0，或选择一个固定的值。
	return murmur3.Sum32WithSeed([]byte(key), 0)
}
