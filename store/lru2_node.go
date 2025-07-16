package store

type node struct {
	k        string
	v        Value
	expireAt int64 // 过期时间戳 (纳秒), 0 表示永不过期
}

// isExpired 是一个辅助函数，用于检查节点是否过期
func (n *node) isExpired() bool {
	if n.expireAt == 0 {
		return false // 0 表示永不过期
	}
	return Now() > n.expireAt
}
