package peer

import "context"

type PeerGetter interface {
	Get(ctx context.Context, group string, key string) ([]byte, error)
	Set(ctx context.Context, group string, key string, value []byte) error
	Delete(ctx context.Context, group string, key string) error
	Addr() string // 提供地址信息用于日志记录
}

// PeerPicker 是对节点选择器能力的抽象。
// 它的唯一职责就是根据 key 选择一个合适的节点（PeerGetter）。
type PeerPicker interface {
	PickPeer(key string) (peer PeerGetter, isSelf bool)
	Addr() string
}
