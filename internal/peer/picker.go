// file: internal/peer/picker.go
package peer

import (
	"context"
	"github.com/rson9/kamaCache/internal/consistenthash"
	"github.com/rson9/kamaCache/internal/peer/discovery"
	"github.com/sirupsen/logrus"
	"io"
	"sync"
)

// ConsistentHashPicker 是 PeerPicker 接口的一个具体实现。
// 它使用一致性哈希算法来选择节点。
type ConsistentHashPicker struct {
	selfAddr   string
	mu         sync.RWMutex
	peers      map[string]*RemotePeer
	hashRing   *consistenthash.Map
	discoverer discovery.Discoverer
	cancel     context.CancelFunc
	logger     *logrus.Entry
	wg         sync.WaitGroup
}

// ConsistentHashPicker 实现了 PeerPicker 接口。
var _ PeerPicker = (*ConsistentHashPicker)(nil)

// 确保 *ConsistentHashPicker 实现了 io.Closer 接口
var _ io.Closer = (*ConsistentHashPicker)(nil)

// pickerOptions 存储了 ConsistentHashPicker  的所有可配置项。
type pickerOptions struct {
	logger *logrus.Logger
	// virtualNodes int // 未来可添加：一致性哈希虚拟节点数
}

// PickerOption 是一个用于配置 ConsistentHashPicker  的函数。
type PickerOption func(*pickerOptions)

// WithPickerLogger 为 ConsistentHashPicker  设置日志记录器。
func WithPickerLogger(logger *logrus.Logger) PickerOption {
	return func(o *pickerOptions) {
		o.logger = logger
	}
}

// NewConsistentHashPicker  创建一个新的 ConsistentHashPicker 。
func NewConsistentHashPicker(selfAddr string, discoverer discovery.Discoverer, opts ...PickerOption) (*ConsistentHashPicker, error) {
	//创建一个默认选项的副本
	options := &pickerOptions{
		logger: logrus.StandardLogger(),
	}
	for _, opt := range opts {
		opt(options)
	}

	ctx, cancel := context.WithCancel(context.Background())
	picker := &ConsistentHashPicker{
		selfAddr:   selfAddr,
		discoverer: discoverer,
		peers:      make(map[string]*RemotePeer),
		hashRing:   consistenthash.New(), // 可在此处使用 options.virtualNodes
		cancel:     cancel,
		logger:     options.logger.WithField("component", "peer_picker"),
	}

	// 启动后台 goroutine 来同步节点信息
	picker.wg.Add(1)
	go picker.syncPeers(ctx)

	return picker, nil
}

// syncPeers 是一个后台 goroutine，用于从服务发现同步节点信息。
func (p *ConsistentHashPicker) syncPeers(ctx context.Context) {
	defer p.wg.Done()
	updateCh, err := p.discoverer.Watch(ctx)
	if err != nil {
		p.logger.WithError(err).Error("Failed to start peer discovery watcher")
		return
	}

	for {
		select {
		case <-ctx.Done():
			p.logger.Info("Context cancelled, stopping peer sync.")
			return
		case update, ok := <-updateCh:
			if !ok {
				p.logger.Info("Discovery channel closed, stopping peer sync.")
				return
			}

			// 过滤掉自己
			if update.Addr == "" || update.Addr == p.selfAddr {
				continue
			}

			switch update.Op {
			case discovery.Add:
				p.addPeer(update.Addr)
			case discovery.Del:
				p.removePeer(update.Addr)
			}
		}
	}
}

func (p *ConsistentHashPicker) addPeer(addr string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if _, ok := p.peers[addr]; ok {
		return // 已经存在
	}

	client, err := NewRemotePeer(addr)
	if err != nil {
		p.logger.WithError(err).WithField("addr", addr).Error("Failed to create remote peer client")
		return
	}

	p.peers[addr] = client
	p.hashRing.Add(addr)
	p.logger.WithField("addr", addr).Info("Peer added")
}

func (p *ConsistentHashPicker) removePeer(addr string) {
	p.mu.Lock()
	peer, ok := p.peers[addr]
	if !ok {
		p.mu.Unlock()
		return
	}
	delete(p.peers, addr)
	p.hashRing.Remove(addr)
	p.logger.WithField("addr", addr).Info("Peer removed")
	p.mu.Unlock() // 2. 立即释放锁！让其他请求可以继续

	// 3. 在锁外执行阻塞的 Close 操作
	if peer != nil {
		peer.Close()
	}
}

// PickPeer 根据 key 选择一个远程节点。
// 如果选择的节点是自身，则返回 (nil, true)，表示应在本地处理。
func (p *ConsistentHashPicker) PickPeer(key string) (peer PeerGetter, isSelf bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if len(p.peers) == 0 {
		return nil, true // 没有可用的远程节点，只能是自己处理
	}

	addr := p.hashRing.Get(key)
	if addr == "" || addr == p.selfAddr {
		return nil, true // 哈希环选择了自己
	}

	// 理论上 addr 应该总是在 p.peers 中，除非有竞态或延迟
	selectedPeer, ok := p.peers[addr]
	if !ok {
		// 这种情况可能在节点刚被移除但哈希环尚未完全更新时发生，
		// 把它当做本地处理是安全的降级策略。
		p.logger.WithField("addr", addr).Warn("Peer selected by hash ring not found in map, fallback to self")
		return nil, true
	}

	return selectedPeer, false
}

// Addr 返回当前节点地址。
func (p *ConsistentHashPicker) Addr() string {
	return p.selfAddr
}

// Close 优雅地关闭 ConsistentHashPicker  和所有相关资源。

// Close 停止服务发现并关闭所有与对等节点的连接
func (p *ConsistentHashPicker) Close() error {
	p.logger.Info("Closing peer picker...")
	p.cancel()  // 停止 syncPeers goroutine
	p.wg.Wait() // 等待 goroutine 真正退出

	p.mu.Lock()
	defer p.mu.Unlock()

	// 关闭所有远程连接
	for addr, peer := range p.peers {
		if err := peer.Close(); err != nil {
			p.logger.Warnf("Failed to close connection to peer %s: %v", addr, err)
		}
	}
	p.peers = nil
	p.hashRing = nil

	// 关闭发现者
	if p.discoverer != nil {
		if err := p.discoverer.Close(); err != nil {
			p.logger.WithError(err).Error("Failed to close discoverer")
			return err // 如果需要，可以向上传递错误
		}
	}
	p.logger.Info("Peer picker closed.")
	return nil
}
