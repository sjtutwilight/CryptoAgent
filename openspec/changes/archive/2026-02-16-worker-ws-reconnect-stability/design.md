## 背景

`datainjector-worker` 通过 `internal/protocol/websocket.go` 统一管理 Binance spot/futures 的 websocket 连接。当前线上日志显示 `ws.read.error`、`ws.reconnect.start/success`、`ws.heartbeat.error` 高频交替出现。根因是 `Connect()` 与 `reconnect()` 路径可并发重叠，导致生命周期 worker 重复启动与重连互相踩踏；同时当前网络路径下还出现解压/帧异常，需做传输层保守化配置。

## 目标 / 非目标

**目标：**
- 单个 websocket client 实例仅维护一套活动生命周期（单 read loop + 单 heartbeat loop）。
- 只允许“当前失败的活动连接”触发重连，避免旧连接误伤新连接。
- 在可预期的重连窗口抑制无效心跳写入与噪声告警。
- 通过关闭压缩与 ping/pong liveness 改善连接稳定性。
- 在不改变现有调用方接口的前提下完成修复。

**非目标：**
- 不改造 role 调度或控制面 API 的角色编排逻辑。
- 不修改订阅协议语义、消息格式或业务处理链路。
- 不引入新的外部依赖，不替换 gorilla/websocket。

## 关键决策

1. `Connect()` 幂等化并成为生命周期启动唯一入口。
- 决策：当连接或生命周期已处于活动状态时，`Connect()` 直接成功返回，不重复启动 goroutine。
- 原因：消除 `ensureConnected()` 重入导致的重复 read/heartbeat 循环。
- 备选：完全移除 `ensureConnected()` 的补连行为。未选：调用方仍需要受控自愈能力。

2. 重连按“失败连接身份”校验归属。
- 决策：仅当失败连接与当前活动连接指针一致时，才允许执行关闭与重连。
- 原因：防止旧 read loop 在延迟报错时关掉新连接。
- 备选：全局重连队列串行化。未选：复杂度更高且归属关系不够直接。

3. 引入 reconnecting 状态并在重连期间跳过心跳写入。
- 决策：heartbeat tick 期间若处于重连态则直接跳过，不发送心跳帧。
- 原因：避免重连窗口内预期性的 `websocket未连接` 告警洪泛。
- 备选：仅降级日志级别。未选：仍会产生无意义写入与锁竞争。

4. 传输层采用保守默认值。
- 决策：关闭 `EnableCompression`，并启用 read deadline + pong 续期。
- 原因：降低 `flate`/帧异常风险，提升半开连接识别能力。
- 备选：保留压缩并增加复杂兼容处理。未选：收益不确定，复杂度上升。

## 风险 / 权衡

- [风险] 重连归属校验更严格，极端竞态下可能延迟恢复。→ 缓解：当前活动连接报错仍可触发重连，补充顺序失败测试。
- [风险] 关闭压缩可能增加带宽占用。→ 缓解：优先稳定性，发布后监控网络开销。
- [风险] read deadline 过紧可能误判超时。→ 缓解：deadline 高于心跳周期并由 pong 持续续期。

## 迁移计划

1. 在保持现有接口不变的前提下改造 websocket client。
2. 增加 Connect 幂等、重连归属、重连期心跳行为的回归测试。
3. 在本地/预发验证 `ws.read.error`、`ws.heartbeat.error`、`ws.reconnect.*` 事件趋势。
4. 回滚方案：回退 `internal/protocol/websocket.go` 改动并重启 worker。

## 待确认问题

- read deadline 与 heartbeat interval 的最终比例（建议 heartbeat * 2），需结合预发延迟分布确认。
