# worker-websocket-bounded-backpressure Specification

## Purpose
TBD - created by archiving change fix-nf-01-nf-02-nf-03. Update Purpose after archive.
## Requirements
### Requirement: WebSocket 各层缓冲必须有界
系统 MUST 对 WebSocket 协议层接收通道、共享分发通道、caller 聚合缓冲同时设置上限，并支持按消息数与字节数配置。

#### Scenario: 慢消费时内存保持封顶
- **WHEN** 下游处理速度持续低于 WebSocket 到达速率
- **THEN** 系统 SHALL 在已配置上限内运行，且不出现无界缓冲增长

#### Scenario: caller 聚合缓冲达到阈值
- **WHEN** caller 缓冲达到 `max_messages` 或 `max_bytes`
- **THEN** 系统 MUST 按配置的 `drop_policy` 执行丢弃而不是继续扩容

### Requirement: 缓冲溢出行为必须可观测
系统 MUST 对每次溢出丢弃记录指标与结构化日志，至少包含 `role_id`、`stream/subscriber`、`drop_reason`、`buffer_layer`。

#### Scenario: 协议层通道溢出
- **WHEN** 协议层消息通道已满且有新消息到达
- **THEN** 系统 SHALL 增加 `websocket_drop_total` 计数并记录溢出事件

#### Scenario: 共享分发层通道溢出
- **WHEN** shared hub 某订阅者通道已满
- **THEN** 系统 SHALL 记录该订阅者维度的丢弃指标与日志

### Requirement: 背压状态必须触发降载与补偿动作
系统 MUST 在达到高水位阈值后进入背压状态，并在持续背压下触发降载动作（如降频订阅或流级限流）以及补偿策略（如 backfill）。

#### Scenario: 达到高水位后标记背压
- **WHEN** 缓冲占用比例达到配置高水位
- **THEN** 系统 SHALL 对后续消息标记背压状态并上报背压指标

#### Scenario: 背压恢复后退出背压态
- **WHEN** 缓冲占用从高水位回落到低水位阈值以下
- **THEN** 系统 SHALL 退出背压状态并记录恢复事件

