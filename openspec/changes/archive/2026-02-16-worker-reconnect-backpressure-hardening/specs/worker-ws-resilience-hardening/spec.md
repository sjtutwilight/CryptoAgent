## ADDED Requirements

### Requirement: Controlled WebSocket Reconnect
Worker MUST 对 WebSocket 连接失败执行受控重连，包括指数退避、随机抖动、最小重连间隔与错误分类冷静期。

#### Scenario: Protocol read error triggers gated reconnect
- **WHEN** 发生 `ws.read.error` 且连接状态为 active
- **THEN** Worker MUST 进入重连控制流程，并按退避+jitter 计算下一次重连时间，且不早于最小重连间隔

#### Scenario: Repeated 1008 enters cooldown window
- **WHEN** 连续触发达到阈值的 `1008 policy violation`
- **THEN** Worker MUST 进入冷静期并在冷静期结束前拒绝新重连尝试

### Requirement: Singleflight Reconnect Execution
同一连接实例在任意时刻 MUST 只有一个重连流程在执行。

#### Scenario: Concurrent reconnect triggers are coalesced
- **WHEN** 心跳超时与读错在短时间内同时触发重连
- **THEN** Worker MUST 合并为一次重连流程，其余触发被记录为合并事件而非并发重连

### Requirement: Idempotent Subscription Recovery
重连恢复阶段 MUST 基于期望订阅集合执行差量恢复，并在短时间窗口内去重相同订阅请求。

#### Scenario: Reconnect success restores only missing streams once
- **WHEN** 连接重建成功且存在目标 stream 集合
- **THEN** Worker MUST 仅发送缺失 stream 的恢复订阅，且每个 stream 在本次恢复中最多发送一次

#### Scenario: Duplicate subscribe request is suppressed
- **WHEN** 同一 stream 在去重窗口内被重复请求订阅
- **THEN** Worker MUST 抑制重复发送并记录去重计数

### Requirement: Endpoint-level Multiplexing
Worker MUST 支持按 endpoint 复用连接，在单连接上承载多个 stream 订阅。

#### Scenario: Multiple stream types share one endpoint connection
- **WHEN** depth 与 aggTrade 属于同一 endpoint 且连接可用
- **THEN** Worker MUST 复用同一连接完成订阅，而不是新建额外连接
