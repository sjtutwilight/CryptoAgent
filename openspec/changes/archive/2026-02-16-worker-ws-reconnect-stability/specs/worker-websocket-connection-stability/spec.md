## ADDED Requirements

### Requirement: WebSocket 连接生命周期 MUST 在单实例内保持幂等
同一个 worker websocket client 实例在重复调用 `Connect()` 时，系统 MUST 保证不会创建重复的 read loop、heartbeat loop 或并行活动连接。

#### Scenario: 已连接状态下重复调用 Connect
- **WHEN** 对已经存在活动连接生命周期的 client 再次调用 `Connect()`
- **THEN** 调用必须成功返回，且不得新增 read/heartbeat goroutine

#### Scenario: 短暂断连期间的 ensureConnected 重试
- **WHEN** 调用层 `ensureConnected()` 在 reconnect 正在恢复连接时再次触发 `Connect()`
- **THEN** client 必须避免重复启动生命周期 worker，并保持单一活动连接所有权

### Requirement: 重连 MUST 由当前失败的活动连接触发
websocket client MUST 仅在“报错连接 == 当前活动连接”时执行 teardown + reconnect。来自已被替换旧连接的错误，必须不能关闭或替换更新的活动连接。

#### Scenario: 旧读循环延迟报错
- **WHEN** 旧连接 read loop 在新连接已安装后才上报读错误
- **THEN** reconnect 逻辑必须忽略该旧错误，并保持新活动连接不受影响

#### Scenario: 当前活动连接读失败
- **WHEN** 当前活动连接的 read loop 返回错误
- **THEN** reconnect 逻辑必须关闭当前连接、执行退避重连并回放订阅

### Requirement: 心跳 MUST 感知重连状态
heartbeat loop MUST 在 reconnect 进行期间跳过心跳写入；reconnect 完成后必须恢复正常心跳发送。

#### Scenario: 重连窗口内心跳 tick
- **WHEN** heartbeat ticker 触发时 reconnect 状态为 active
- **THEN** 系统不得发送心跳帧，且不得产出该 tick 的 heartbeat failed 告警

#### Scenario: 重连成功后的心跳
- **WHEN** reconnect 完成并已安装新活动连接
- **THEN** 后续 heartbeat tick 必须恢复正常发送

### Requirement: WebSocket 传输默认值 MUST 采用保守存活策略
websocket dialer MUST 关闭 per-message compression，并通过 read deadline 与 pong 续期实现连接存活检测。

#### Scenario: 创建 dialer 时
- **WHEN** client 构建新的 websocket dialer
- **THEN** per-message compression 必须为关闭状态

#### Scenario: 收到 pong 帧
- **WHEN** 活动连接收到 pong 帧
- **THEN** 系统必须按 liveness 策略刷新 read deadline
