## MODIFIED Requirements

### Requirement: Reliability Metrics Emission
Worker MUST 输出覆盖重连、丢弃、补数与完整性状态的关键指标，并包含 endpoint 与 role 标签。

#### Scenario: Reconnect lifecycle metrics are emitted
- **WHEN** 发生重连开始、成功、失败事件
- **THEN** Worker MUST 分别上报 `ws.reconnect.start`、`ws.reconnect.success`、`ws.reconnect.failure`

#### Scenario: Drop and backfill metrics are emitted
- **WHEN** 发生缓冲丢弃或 backfill 重试耗尽
- **THEN** Worker MUST 上报 `ws.buffer.drop` 与 `backfill.exhausted`

#### Scenario: Integrity core state metrics are emitted
- **WHEN** 完整性状态发生推进、重锚或等待变化
- **THEN** Worker MUST 上报 `expected_seq`、`seen_max`、`head_lag`、`awaiting_snapshot` 等状态指标

#### Scenario: Gap window metrics are emitted
- **WHEN** gap 窗口集合发生新增、收敛或超时
- **THEN** Worker MUST 上报窗口数量、总缺失长度与最老缺口年龄指标

### Requirement: Alert Thresholds for Key Failure Modes
系统 MUST 为高风险指标配置可执行告警阈值与严重级别。

#### Scenario: Excessive reconnect frequency alerts
- **WHEN** `ws.reconnect.start` 在窗口内超过阈值
- **THEN** 系统 MUST 触发链路抖动告警

#### Scenario: Policy violation bursts alert
- **WHEN** `ws.policy.1008` 在窗口内连续超阈
- **THEN** 系统 MUST 触发限流风险告警并标记建议冷静期

#### Scenario: Integrity stall alert
- **WHEN** `head_lag` 或最老缺口年龄持续超过阈值
- **THEN** 系统 MUST 触发完整性停滞告警并标记受影响 `role_id/stream_key`
