package kamacache

import (
	"github.com/rson9/kamaCache/internal/cache"
	"github.com/sirupsen/logrus"
	"time"
)

// NodeOptions holds all the configuration for a kamaCache Node.
// It is configured using the Functional Options Pattern with `With...` functions.
type NodeOptions struct {
	// SelfAddr is the address (e.g., "127.0.0.1:8001") that this node will listen on.
	// This is a required field.
	SelfAddr string

	// EtcdEndpoints is a list of etcd cluster endpoints.
	// This is a required field for service discovery.
	EtcdEndpoints []string

	// ServiceName is the prefix used in etcd for service discovery.
	// Defaults to "kamacache" (see config.go).
	ServiceName string

	// EtcdLeaseTTL is the time-to-live for the node's registration in etcd.
	// Defaults to 10 seconds (see config.go).
	EtcdLeaseTTL time.Duration

	// Logger is the logger instance for the node.
	// Defaults to a configured logrus.StandardLogger().
	Logger *logrus.Logger
}

type GroupOptions struct {
	CacheOpts cache.CacheOptions
	Logger    *logrus.Entry
}
type NodeOption func(*NodeOptions)

// WithSelfAddr sets the address for the current node. This address is used for both
// listening for incoming gRPC requests and for registering with the discovery service.
func WithSelfAddr(addr string) NodeOption {
	return func(o *NodeOptions) {
		o.SelfAddr = addr
	}
}

// WithEtcdEndpoints sets the etcd endpoints for service discovery. At least one
// endpoint is required for the node to join the cluster.
func WithEtcdEndpoints(endpoints []string) NodeOption {
	return func(o *NodeOptions) {
		o.EtcdEndpoints = endpoints
	}
}

// WithServiceName sets the service name prefix used in etcd. All nodes using the
// same service name will belong to the same cluster.
func WithServiceName(name string) NodeOption {
	return func(o *NodeOptions) {
		o.ServiceName = name
	}
}

// WithEtcdLeaseTTL sets the time-to-live for the etcd registration lease. If the node
// fails to renew the lease within this duration, it will be considered offline.
func WithEtcdLeaseTTL(ttl time.Duration) NodeOption {
	return func(o *NodeOptions) {
		o.EtcdLeaseTTL = ttl
	}
}

// WithLogger sets a custom logger for the node and all its internal components.
// If not set, a default logrus logger will be used.
func WithLogger(logger *logrus.Logger) NodeOption {
	return func(o *NodeOptions) {
		o.Logger = logger
	}
}
