## ADDED Requirements

### Requirement: Reliability Metrics Emission
Worker MUST 输出覆盖重连、丢弃、补数与限流的关键指标，并包含 endpoint 与 role 标签。

#### Scenario: Reconnect lifecycle metrics are emitted
- **WHEN** 发生重连开始、成功、失败事件
- **THEN** Worker MUST 分别上报 `ws.reconnect.start`、`ws.reconnect.success`、`ws.reconnect.failure`

#### Scenario: Drop and backfill metrics are emitted
- **WHEN** 发生缓冲丢弃或 backfill 重试耗尽
- **THEN** Worker MUST 上报 `ws.buffer.drop` 与 `backfill.exhausted`

### Requirement: Alert Thresholds for Key Failure Modes
系统 MUST 为高风险指标配置可执行告警阈值与严重级别。

#### Scenario: Excessive reconnect frequency alerts
- **WHEN** `ws.reconnect.start` 在窗口内超过阈值
- **THEN** 系统 MUST 触发链路抖动告警

#### Scenario: Policy violation bursts alert
- **WHEN** `ws.policy.1008` 在窗口内连续超阈
- **THEN** 系统 MUST 触发限流风险告警并标记建议冷静期

### Requirement: Diagnostic Classification
告警事件 MUST 可区分链路异常、策略限流与消费背压三类主因。

#### Scenario: Alert includes root-cause class
- **WHEN** 触发任一可靠性告警
- **THEN** 告警负载 MUST 包含主因分类字段用于值班分流
