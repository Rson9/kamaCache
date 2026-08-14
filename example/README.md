# kamaCache 集成测试说明

## 测试目标

- 多节点缓存系统的基础功能验证，包括：
  - 多节点缓存数据同步和一致性
  - 本地缓存命中、远程缓存命中、回源 Getter 加载三条访问路径
  - Set 和 Delete 操作在集群间的同步
  - 并发读写安全性和正确性验证

## 测试环境

- 使用 `sync.Map` 模拟后端数据源
- 多个缓存节点通过嵌入式 etcd 服务注册和发现
- 测试代码启动若干缓存节点，每个节点独立监听随机端口
- 依赖 testify 进行断言和测试管理

## 主要测试用例

| 用例名称                    | 说明                                              |
|-----------------------------|---------------------------------------------------|
| Get: Cache miss triggers load | 缓存未命中时调用 Getter 回源加载数据             |
| Get: Local cache hit          | 缓存命中本地缓存，避免调用 Getter                  |
| Get: Remote cache hit         | 本地未命中，向远程节点请求缓存，远程命中          |
| Set: Propagate to peers       | Set 同步写入负责人节点，验证其它节点可通过路由读取    |
| Delete: Propagate to peers    | 删除缓存同步，验证其它节点缓存被清理                |
| Concurrent Set and Get        | 并发写入和读取，测试缓存安全性及性能                |

## 运行方式

测试会自动启动临时的嵌入式 etcd，无需预先运行外部服务：

```bash
go test ./example
```
