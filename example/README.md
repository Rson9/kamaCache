# kamaCache 集成测试说明

## 测试目标

- 多节点缓存系统的基础功能验证，包括：
  - 多节点缓存数据同步和一致性
  - 本地缓存命中、远程缓存命中、回源 Getter 加载三条访问路径
  - Set 和 Delete 操作在集群间的异步同步
  - 缓存项过期机制的正确性
  - 并发读写安全性和正确性验证

## 测试环境

- 使用 `sync.Map` 模拟后端数据源
- 多个缓存节点通过 etcd 服务注册和发现
- 测试代码启动若干缓存节点，每个节点独立监听随机端口
- 依赖 testify 进行断言和测试管理

## 主要测试用例

| 用例名称                    | 说明                                              |
|-----------------------------|---------------------------------------------------|
| Get: Cache miss triggers load | 缓存未命中时调用 Getter 回源加载数据             |
| Get: Local cache hit          | 缓存命中本地缓存，避免调用 Getter                  |
| Get: Remote cache hit         | 本地未命中，向远程节点请求缓存，远程命中          |
| Set: Propagate to peers       | Set 操作异步同步至其他节点，确保数据一致            |
| Delete: Propagate to peers    | 删除缓存同步，验证其它节点缓存被清理                |
| Concurrent Set and Get        | 并发写入和读取，测试缓存安全性及性能                |

## 运行方式

确保 etcd 服务在本地运行：

```bash
etcd
