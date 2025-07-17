package kamacache

import (
	"context"
	"github.com/rson9/kamaCache/internal/cache"
	"github.com/rson9/kamaCache/internal/group"
)

// Getter 是加载数据的回调接口。当缓存未命中时，系统会调用此接口的方法。
type Getter interface {
	Get(ctx context.Context, key string) ([]byte, error)
}

// GetterFunc 是一个适配器，允许使用普通函数作为 Getter 接口。
type GetterFunc func(ctx context.Context, key string) ([]byte, error)

// Get 实现了 Getter 接口。
func (f GetterFunc) Get(ctx context.Context, key string) ([]byte, error) {
	return f(ctx, key)
}

// ByteView 是缓存值的只读视图，防止外部代码修改缓存内容。
type ByteView = cache.ByteView

// Group 是一个缓存命名空间。它是对外暴露的结构体，封装了内部实现。
type Group struct {
	internalGroup *group.Group
}

// Get 从缓存中获取一个值。
// 如果缓存未命中，它将使用注册的 Getter 从数据源加载数据，并填充到缓存中。
func (g *Group) Get(ctx context.Context, key string) (ByteView, error) {
	return g.internalGroup.Get(ctx, key)
}

func (g *Group) Delete(key string) error {
	return g.internalGroup.Delete(key)
}

func (g *Group) Set(key string, val []byte) error {
	return g.internalGroup.Set(key, val)
}
