# worker-ingestion-backpressure-control Specification

## Purpose
TBD - created by archiving change worker-reconnect-backpressure-hardening. Update Purpose after archive.
## Requirements
### Requirement: Queue Watermark Protection
Worker MUST 为 WS 消息缓冲队列提供高低水位控制，并在超过高水位时触发背压保护。

#### Scenario: High watermark triggers protection
- **WHEN** 队列长度超过配置的高水位阈值
- **THEN** Worker MUST 启用背压保护动作并记录 `ws.buffer.drop` 或降级事件

### Requirement: Bounded Parallel Processing and Batch Sink
Worker MUST 使用有限并发消费与批量写入 sink 的策略提升稳定吞吐。

#### Scenario: Messages are processed by bounded worker pool
- **WHEN** 消息流量上升到常态峰值
- **THEN** Worker MUST 将解析与写入解耦，并通过有限 worker 池处理，避免无界并发

#### Scenario: Sink writes are batched
- **WHEN** sink 写入队列达到批量条件（条数或时间窗）
- **THEN** Worker MUST 按批提交写入以降低单条写入开销

### Requirement: Backpressure Signal for Gap Logic
背压事件 MUST 反馈给缺口检测与补数调度模块，防止由消费滞后导致的假缺口连锁触发。

#### Scenario: Gap trigger is rate-limited under backpressure
- **WHEN** 系统处于背压保护状态
- **THEN** Worker MUST 对缺口触发进行限频或延迟，并记录抑制次数

