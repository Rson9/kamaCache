package discovery

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// EtcdDiscoverer 是 Discoverer 接口基于 etcd 的具体实现。
type EtcdDiscoverer struct {
	cli        *clientv3.Client
	serviceKey string
	logger     *logrus.Entry
}

// etcdOptions 存储了 EtcdDiscoverer 的所有可配置项。
type etcdOptions struct {
	serviceName string
	logger      *logrus.Logger
}

// EtcdOption 是一个用于配置 EtcdDiscoverer 的函数。
type EtcdOption func(*etcdOptions)

// WithServiceName 设置在 etcd 中注册的服务名称。
func WithServiceName(name string) EtcdOption {
	return func(o *etcdOptions) {
		o.serviceName = name
	}
}

// WithLogger 为 EtcdDiscoverer 设置日志记录器。
func WithLogger(logger *logrus.Logger) EtcdOption {
	return func(o *etcdOptions) {
		o.logger = logger
	}
}

// NewEtcdDiscoverer 创建一个基于 etcd 的服务发现实例。
func NewEtcdDiscoverer(endpoints []string, opts ...EtcdOption) (*EtcdDiscoverer, error) {
	// 默认选项
	options := &etcdOptions{
		serviceName: "kama-cache",
		logger:      logrus.StandardLogger(),
	}
	// 应用用户传入的选项
	for _, opt := range opts {
		opt(options)
	}

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create etcd client: %w", err)
	}

	return &EtcdDiscoverer{
		cli:        cli,
		serviceKey: fmt.Sprintf("/services/%s/", options.serviceName),
		logger:     options.logger.WithField("component", "etcd_discoverer"),
	}, nil
}

func (d *EtcdDiscoverer) Watch(ctx context.Context) (<-chan Update, error) {
	updateCh := make(chan Update, 16) // 使用带缓冲的channel

	// 1. 先获取所有现有节点
	resp, err := d.cli.Get(ctx, d.serviceKey, clientv3.WithPrefix())
	if err != nil {
		close(updateCh)
		return nil, fmt.Errorf("failed to fetch initial services from etcd: %w", err)
	}

	for _, kv := range resp.Kvs {
		addr := parseAddrFromKey(string(kv.Key), d.serviceKey)
		if addr != "" {
			updateCh <- Update{Op: Add, Addr: addr}
		}
	}

	// 2. 启动一个 goroutine 来监听后续变化
	go d.watchLoop(ctx, updateCh, resp.Header.Revision+1)

	return updateCh, nil
}

func (d *EtcdDiscoverer) watchLoop(ctx context.Context, updateCh chan<- Update, startRev int64) {
	defer close(updateCh)
	watchChan := d.cli.Watch(ctx, d.serviceKey, clientv3.WithPrefix(), clientv3.WithRev(startRev))

	d.logger.Info("Starting to watch for service changes...")

	for {
		select {
		case <-ctx.Done():
			d.logger.Info("Watch context cancelled, stopping watch loop.")
			return
		case resp, ok := <-watchChan:
			if !ok {
				d.logger.Warn("Etcd watch channel closed unexpectedly.")
				return
			}
			if err := resp.Err(); err != nil {
				d.logger.WithError(err).Error("Etcd watch returned an error.")
				// 可以在这里实现重连逻辑
				time.Sleep(1 * time.Second)
				continue
			}

			for _, event := range resp.Events {
				addr := parseAddrFromKey(string(event.Kv.Key), d.serviceKey)
				if addr == "" {
					continue
				}
				switch event.Type {
				case clientv3.EventTypePut:
					updateCh <- Update{Op: Add, Addr: addr}
				case clientv3.EventTypeDelete:
					updateCh <- Update{Op: Del, Addr: addr}
				}
			}
		}
	}
}

func (d *EtcdDiscoverer) Close() error {
	return d.cli.Close()
}

// parseAddrFromKey 是一个辅助函数，它现在呆在最适合它的地方。
func parseAddrFromKey(key, prefix string) string {
	return strings.TrimPrefix(key, prefix)
}
