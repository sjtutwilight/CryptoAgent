## ADDED Requirements

### Requirement: Worker SHALL emit orderbook diff and snapshot into separated topics
Worker MUST 将 orderbook 实时 diff 与 snapshot 事件分流下沉到独立 Kafka topic，且不得再输出本地重建簿状态事件。

#### Scenario: Diff events are routed to diff topic
- **WHEN** worker 接收到 Binance depth diff websocket 消息
- **THEN** 消息 MUST 写入 `*.orderbook.diff`，并保持原始 diff 语义字段

#### Scenario: Snapshot events are routed to snapshot topic
- **WHEN** worker 产生周期快照或 backfill 快照事件
- **THEN** 消息 MUST 写入 `*.orderbook.snapshot`，并包含 `snapshot=true`

### Requirement: Worker SHALL publish periodic orderbook snapshots every 10 seconds
Worker MUST 为 spot 与 perp 分别提供 `10s` 周期快照采集 role，并持续下沉 snapshot topic。

#### Scenario: Perp periodic snapshot is emitted every 10 seconds
- **WHEN** perp snapshot polling role 运行
- **THEN** worker MUST 以 `10s` 间隔调用 depth REST 并发出 snapshot 消息

#### Scenario: Spot periodic snapshot is emitted every 10 seconds
- **WHEN** spot snapshot polling role 运行
- **THEN** worker MUST 以 `10s` 间隔调用 depth REST 并发出 snapshot 消息

### Requirement: Worker SHALL trigger snapshot backfill when diff integrity is broken
Worker MUST 在检测到 diff 缺失、跳号或超时时触发 snapshot backfill，并将补回快照输出到 snapshot topic。

#### Scenario: Sequence gap triggers snapshot backfill
- **WHEN** integrity 检测到 `U/u/pu` 不连续
- **THEN** worker MUST 触发 snapshot backfill 请求并输出 `snapshot_source=backfill`

#### Scenario: Snapshot backfill is side-channel output
- **WHEN** backfill snapshot 返回成功
- **THEN** worker MUST 旁路输出 snapshot，不得依赖本地订单簿回调才能继续处理 diff

### Requirement: Worker SHALL not maintain in-memory local orderbook state
Worker MUST NOT 在 orderbook 接入链路维护本地订单簿状态，也 MUST NOT 依赖订单簿状态机完成 diff 放行。

#### Scenario: Diff pipeline runs without orderbook state store
- **WHEN** orderbook diff pipeline 处理实时流
- **THEN** pipeline MUST 在未创建本地 BookState 的情况下完成缺失检测与消息下沉
