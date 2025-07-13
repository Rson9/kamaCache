package utils

import (
	"context"
)

type contextKey string

const (
	// RequestOriginKey 用于在 Context 中存储请求的来源标识。
	RequestOriginKey contextKey = "request-origin"
)
const (
	// OriginPeer 表示请求是由集群中的另一个 Peer 转发过来的。
	OriginPeer = "peer"
)

func IsFromPeer(ctx context.Context) bool {
	val, ok := ctx.Value(RequestOriginKey).(string)
	return ok && val == OriginPeer
}

func WithOrigin(ctx context.Context, origin string) context.Context {
	return context.WithValue(ctx, RequestOriginKey, origin)
}
