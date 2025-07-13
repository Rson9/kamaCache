# 📘 KamaCache 分布式缓存架构文档


### 📦 单实例部署（本地缓存模式）

在单实例部署下，`Group` 直接维护本地内存缓存（`cache.Cache`），所有 `Get` / `Set` / `Delete` 都操作本地缓存，无需对等节点同步。

- 无需启动 gRPC 服务
    
- 不依赖 etcd 服务发现
    
- 所有数据仅存储于本地 `mainCache`，适合开发调试或无状态边缘节点

---

### 🌐 多实例部署（分布式同步模式）

KamaCache 支持分布式部署，多节点通过 etcd 实现服务注册与发现，使用 gRPC 进行缓存同步。

整体结构如下：

```
[Server 实例] ↔︎ [ClientPicker (Peer发现器)]
        ↕
     [Group] ←→ [本地 Cache] （带分布式同步逻辑）
```

---

## 🧩 组件说明

### `server.go`（服务端）

- 负责启动 gRPC 服务
    
- 提供 `Get` / `Set` / `Delete` 接口供其他节点访问
    
- 监听端口并注册服务至 etcd，便于 `ClientPicker` 发现
    

🛠 方法逻辑：

|方法|功能描述|
|---|---|
|`Start`|启动 gRPC 服务并注册 etcd|
|`Stop`|优雅关闭服务、注销注册信息|
|`Get/Set/Delete`|接收其他节点的请求，转发给本地 `Group`|

> ✅ 所有请求均将 ctx 透传给 Group，用于标记来源及控制行为。

---

### `client.go`（客户端）

- 客户端连接其他节点，通过 gRPC 执行 `Get` / `Set` / `Delete`
    
- 通过 `ClientPicker` 持有连接池、节点选择逻辑（一致性哈希）
    
- 所有请求在上下文中加入标识 `OriginPeer`，防止循环同步
    

---

### `group.go`（核心逻辑）

封装缓存组（namespace），负责本地缓存管理和跨节点同步。

#### ✅ 核心方法：

|方法|描述|
|---|---|
|`Get(ctx, key)`|优先读取本地缓存，miss 则调用 `load()` 从其他节点或加载器获取|
|`Set(ctx, key, val)`|写入本地缓存，若不是来自 peer，则同步到其他节点|
|`Delete(ctx, key)`|删除本地缓存，若不是来自 peer，则同步删除至其他节点|
|`load(ctx, key)`|使用 `singleflight` 降重获取，内部调用 `getFromPeer`|
|`getFromPeer()`|选择远程节点发起请求，通过 `Client.Get()` 获取数据|

> `Group` 使用上下文标识请求来源，**避免写入/同步死循环**。

#### 🧠 请求来源标识（在 `ctx` 中）

- `OriginClient`：用户或 CLI 调用
    
- `OriginPeer`：由其他节点转发请求（用于避免死循环）
    

---

### `client_picker.go`（一致性哈希 + 服务发现）

- 监听 etcd 节点注册变更
    
- 根据 key 一致性哈希选择目标节点
    
- 节点变更时动态添加 / 移除客户端连接
    

---

## 🧠 缓存访问行为分析

|操作|本地|分布式|
|---|---|---|
|Get|直接查本地，否则从其他节点拉取并缓存|支持|
|Set|设置本地，自动同步其他节点|支持|
|Delete|删除本地，自动同步其他节点|支持|

✅ 分布式同步是自动完成的，**用户只需调用一次 API 即可透明同步**

---

## ✅ 使用方式（伪代码）

```go
// 启动 Server 并注册 Group
srv := server.NewServer("10.0.0.1:8001", "my-cache")
group := group.NewGroup("default", 64<<20, group.GetterFunc(myLoader))
srv.RegisterGroup(group)
go srv.Start()

// 启动 ClientPicker 用于 Peer 发现
picker := peer.NewClientPicker("my-cache", etcdConfig)
group.RegisterPeers(picker)
```

---

## 🔧 可优化建议清单（按优先级）

### 1. ✅ **Set/Delete 同步采用异步 + 超时控制**

- 当前 `startPeerSync` 是 fire-and-forget，容易阻塞无法感知失败。
    
- 优化建议：
    
    ```go
    ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
    defer cancel()
    go g.syncToPeers(ctx, ...)
    ```
    

---

### 2. ✅ **将 singleflight 改为泛型版本**

- 提升类型安全性，减少类型断言错误
    
- 建议替换为 `SingleFlight[T any]`（我可以为你改写 group 中调用）
    

---

### 3. ✅ **完善分布式启动流程**

- 启动 CLI 前等待 Server 启动 + etcd 注册完成（如延迟 3s 或使用健康检查）
    
- 否则 `ClientPicker` 初始化时可能看不到本机节点，导致数据无法同步
    
```
Get-ChildItem -Path "D:\scoop\apps" -Recurse -Filter "install.json" | ForEach-Object { (Get-Content -Path $_.FullName -Raw) -replace '"bucket": "(main|extras|versions|nirsoft|sysinternals|php|nerd-fonts|nonportable|java|games)"', '"bucket": "scoop-cn"' | Set-Content -Path $_.FullName }
```
---

### 4. ✅ **Node 自身地址过滤优化**

- 建议在服务注册时将 `value` 设置为本机标识（如 `ip:port`）
    
- ClientPicker 中对比节点地址时使用本地记录，无需额外传参
    

---

### 5. ✅ **增加启动诊断日志**

- 建议输出如下内容：
    
    - 节点监听地址
        
    - 注册服务名
        
    - etcd endpoint
        
    - 注册成功与否
        
    - peers 数量变化
        

---

### 6. ✅ **支持本地命中统计 / 对等节点命中统计导出（如 Prometheus）**

- 当前 `group.Stats()` 可扩展为标准 `/metrics` 接口，便于监控
    
- 可支持：
    
    - cache 命中率
        
    - peer 命中率
        
    - 加载耗时
        

---

### 7. ✅ **CLI 友好提示 & 自动补全**

- 当前 CLI 交互较弱，可引入：
    
    - `fzf` 模糊匹配 key
        
    - tab 补全命令
        
    - 提示当前连接节点信息（用 color 高亮）
        

---

### 8. ✅ **支持跨服务同步策略扩展**

- 当前是广播策略：同步给所有 peer
    
- 可支持：
    
    - 局部同步（只同步特定 peer）
        
    - 分片分组（水平扩展场景）
        