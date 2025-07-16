package singleflight

import (
	"context"

	"github.com/rson9/kamaCache/cache"
	"github.com/rson9/kamaCache/utils"
)

// 代表正在进行或已结束的请求
type call struct {
	done chan struct{}
	val  cache.ByteView
	err  error
}
type result struct {
	val cache.ByteView
	err error
}

// Group manages all kinds of calls
type Group struct {
	m utils.TypedSyncMap[string, *call] // 使用自定义sync.Map实现安全值访问
}

// Do 暂时支持ByteView调用
func (g *Group) Do(ctx context.Context, key string, fn func() (cache.ByteView, error)) (any, error) {
	// 先检查是否已经有进行中的请求
	if c, ok := g.m.Load(key); ok {

		// 等待请求完成或上下文取消
		select {
		case <-ctx.Done():
			return nil, ctx.Err() // 返回上下文错误
		case <-c.done: // 使用channel而不是WaitGroup来实现更灵活的等待
			return c.val, c.err
		}
	}

	// 创建新的调用
	c := &call{done: make(chan struct{})}
	c, loaded := g.m.LoadOrStore(key, c)
	if loaded {
		// 如果其他goroutine同时存储了，使用已存储的call
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-c.done:
			return c.val, c.err
		}
	}

	// 执行函数
	go func() {
		defer func() {
			close(c.done)   // 通知所有等待者
			g.m.Delete(key) // 清理
		}()

		// 创建一个可以取消的上下文，如果外部上下文取消则取消
		fnCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		// 使用channel来接收结果
		resultCh := make(chan result)
		go func() {
			val, err := fn()
			resultCh <- result{val, err}
		}()

		select {
		case res := <-resultCh:
			c.val, c.err = res.val, res.err
		case <-fnCtx.Done():
			c.err = fnCtx.Err()
		}
	}()

	// 等待结果
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.done:
		return c.val, c.err
	}
}
