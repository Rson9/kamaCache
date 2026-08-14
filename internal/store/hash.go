package store

import (
	"encoding/binary"
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
	data := []byte(key)

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

// HashWithMurmur3Library 保留原有函数名，使用本地实现计算 Murmur3。
// 这样可以避免第三方实现的 unsafe checkptr 问题。
func HashWithMurmur3Library(key string) uint32 {
	return hashMurmur3(key)
}
