package peer

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/rson9/kamaCache/consistenthash"
	"github.com/rson9/kamaCache/registry"
	"github.com/sirupsen/logrus"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type ClientPicker struct {
	svcName  string
	addr     string
	mu       sync.RWMutex
	consHash *consistenthash.Map
	remotes  map[string]*RemotePeer
	etcdCli  *clientv3.Client
	ctx      context.Context
	cancel   context.CancelFunc
	logger   *logrus.Entry
}

type PickerOption func(*ClientPicker)

func WithServiceName(name string) PickerOption {
	return func(p *ClientPicker) {
		p.svcName = name
	}
}

func WithLogger(logger *logrus.Logger) PickerOption {
	return func(p *ClientPicker) {
		p.logger = logger.WithField("component", "peer_picker")
	}
}

func NewClientPicker(addr string, opts ...PickerOption) (*ClientPicker, error) {
	ctx, cancel := context.WithCancel(context.Background())
	picker := &ClientPicker{
		addr:     addr,
		svcName:  "kama-cache",
		remotes:  make(map[string]*RemotePeer),
		consHash: consistenthash.New(),
		ctx:      ctx,
		cancel:   cancel,
		logger:   logrus.NewEntry(logrus.StandardLogger()),
	}
	for _, opt := range opts {
		opt(picker)
	}

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   registry.DefaultConfig.Endpoints,
		DialTimeout: registry.DefaultConfig.DialTimeout,
	})
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create etcd client: %w", err)
	}
	picker.etcdCli = cli

	if err := picker.startServiceDiscovery(); err != nil {
		picker.Close()
		return nil, fmt.Errorf("service discovery failed: %w", err)
	}

	return picker, nil
}

func (p *ClientPicker) Addr() string {
	return p.addr
}
func (p *ClientPicker) startServiceDiscovery() error {
	if err := p.fetchAllServices(); err != nil {
		return fmt.Errorf("initial service sync failed: %w", err)
	}
	go p.watchServiceChanges()
	return nil
}

// watchServiceChanges 监听服务变化
func (p *ClientPicker) watchServiceChanges() error {
	watcher := clientv3.NewWatcher(p.etcdCli)
	defer watcher.Close()

	watchChan := watcher.Watch(p.ctx, p.servicePrefix(), clientv3.WithPrefix())
	for {
		select {
		case <-p.ctx.Done():
			return nil
		case resp, ok := <-watchChan:
			if !ok || resp.Canceled {
				return fmt.Errorf("etcd watch canceled: %w", resp.Err())
			}
			p.handleWatchEvents(resp.Events)
		}
	}
}

func (p *ClientPicker) fetchAllServices() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := p.etcdCli.Get(ctx, p.servicePrefix(), clientv3.WithPrefix())
	if err != nil {
		return fmt.Errorf("etcd get failed: %w", err)
	}

	for _, kv := range resp.Kvs {
		addr := parseAddrFromKey(string(kv.Key), p.svcName)
		if addr != "" && addr != p.addr { // 过滤自己地址
			p.addRemote(addr)
		}
	}
	return nil
}

// handleWatchEvents 事件处理
func (p *ClientPicker) handleWatchEvents(events []*clientv3.Event) {

	for _, event := range events {
		if event.Kv == nil {
			continue
		}
		addr := parseAddrFromKey(string(event.Kv.Key), p.svcName)
		if addr == "" || addr == p.addr {
			continue
		}

		switch event.Type {
		case clientv3.EventTypePut:
			if _, exists := p.remotes[addr]; !exists {
				p.addRemote(addr)
				p.logger.WithField("addr", addr).Info("Added peer")
			}
		case clientv3.EventTypeDelete:
			if cli, exists := p.remotes[addr]; exists {
				p.removeRemote(addr)
				cli.Close()
				p.logger.WithField("addr", addr).Info("Removed peer")
			}
		}
	}
}

func (p *ClientPicker) addRemote(addr string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	// 如果已经存在，不重复添加
	if _, exists := p.remotes[addr]; exists {
		return
	}
	// 增加远程节点
	cli, err := NewRemotePeer(addr)
	if err != nil {
		p.logger.WithError(err).WithField("addr", addr).Error("Failed to create client")
		return
	}
	p.consHash.Add(addr)
	p.remotes[addr] = cli
}

func (p *ClientPicker) removeRemote(addr string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.consHash.Remove(addr)
	delete(p.remotes, addr)
}

// PickPeer 根据key选择对应peer
// 在没有特定错误情况下，根据一致性哈希一定能返回一个客户端地址
func (p *ClientPicker) PickPeer(key string) (*RemotePeer, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	// 如果没有可用的peer，返回错误
	if len(p.remotes) == 0 {
		return nil, fmt.Errorf("no available peers")
	}

	// 通过一致性哈希选择节点
	addr := p.consHash.Get(key)
	// 防止选到自己和无效节点
	if addr == "" || addr == p.addr {
		return nil, fmt.Errorf("find one not valid peer")
	}

	// 二次验证节点，只有自己存储的节点才有效
	if cli, ok := p.remotes[addr]; ok {
		return cli, nil
	}
	return nil, fmt.Errorf("peer %s not found", addr)
}

func (p *ClientPicker) Close() error {
	if p.cancel == nil {
		return nil
	}
	p.cancel()

	p.mu.Lock()
	defer p.mu.Unlock()

	multiErr := newMultiError()
	for addr, cli := range p.remotes {
		if err := cli.Close(); err != nil {
			multiErr.Add(fmt.Errorf("client %s: %w", addr, err))
		}
	}
	if err := p.etcdCli.Close(); err != nil {
		multiErr.Add(fmt.Errorf("etcd client: %w", err))
	}
	return multiErr.Err()
}

// Peers 将picker存储的所有client返回
func (p *ClientPicker) Peers() []string {
	p.mu.RLock()
	defer p.mu.RUnlock()

	var addrs []string
	for addr := range p.remotes {
		addrs = append(addrs, addr)
	}
	return addrs
}

func (p *ClientPicker) servicePrefix() string {
	return fmt.Sprintf("/services/%s/", p.svcName)
}

func parseAddrFromKey(key, svcName string) string {
	prefix := fmt.Sprintf("/services/%s/", svcName)
	return strings.TrimPrefix(key, prefix)
}

type multiError struct {
	errs []error
}

func newMultiError() *multiError {
	return &multiError{}
}

func (m *multiError) Add(err error) {
	if err != nil {
		m.errs = append(m.errs, err)
	}
}

func (m *multiError) Err() error {
	if len(m.errs) == 0 {
		return nil
	}
	return fmt.Errorf("multiple errors: %v", m.errs)
}
