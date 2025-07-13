package peer

import "context"

// Peer 定义了缓存节点的接口
type Peer interface {
	Get(ctx context.Context, group string, key string) ([]byte, bool)
	Set(ctx context.Context, group string, key string, value []byte) bool
	Delete(ctx context.Context, group string, key string) bool
	Close() error
}

// PeerPicker 定义了peer选择器的接口
type PeerPicker interface {
	PickPeer(key string) (peer Peer, ok bool, self bool)
	Close() error
}
