package discovery

import "context"

// Update 是服务状态的变更通知
type Update struct {
	Op   Op     // 操作类型
	Addr string // 变更的节点地址
}

// Op 定义了节点变更的操作类型
type Op int

const (
	Add Op = iota // 新增节点
	Del           // 删除节点
)

// Discoverer 定义了服务发现者的接口
type Discoverer interface {
	// Watch 返回一个通道，用于接收服务更新通知
	Watch(ctx context.Context) (<-chan Update, error)
	// Close 关闭发现者并释放资源
	Close() error
}
