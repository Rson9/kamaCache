package kamacache

import (
	"github.com/sirupsen/logrus"
	"time"
)

// Default values for Node configuration.
const (
	// DefaultEtcdLeaseTTL specifies the default time-to-live for the node's registration in etcd.
	DefaultEtcdLeaseTTL = 10 * time.Second

	// DefaultServiceName is the default service name prefix used in etcd for service discovery.
	DefaultServiceName = "kamacache"
)

func defaultNodeOptions() NodeOptions {
	return NodeOptions{
		EtcdLeaseTTL: DefaultEtcdLeaseTTL,
		ServiceName:  DefaultServiceName,
		Logger:       logrus.StandardLogger(), // 允许用户 override
	}
}
