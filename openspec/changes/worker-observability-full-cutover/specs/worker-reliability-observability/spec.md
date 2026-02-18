## MODIFIED Requirements

### Requirement: Reliability Metrics Emission
Worker MUST 输出覆盖重连、丢弃、补数会话结果与产出漏斗的关键指标，并包含 role 及必要上下文字段。

#### Scenario: Reconnect lifecycle metrics are emitted
- **WHEN** 发生重连开始、成功、失败事件
- **THEN** Worker MUST 分别上报重连生命周期相关指标与事件

#### Scenario: Drop and backfill metrics are emitted
- **WHEN** 发生缓冲丢弃、补数去重、补数结果回写或 pending 时长统计
- **THEN** Worker MUST 上报 `worker_websocket_drops_total`、`worker_integrity_backfill_schedule_dedup_total`、`worker_integrity_backfill_result_total`、`worker_integrity_backfill_pending_duration_seconds`

#### Scenario: Pipeline yield metrics are emitted
- **WHEN** caller 有非零输入并进入 pipeline
- **THEN** Worker MUST 可观测 `caller -> pipeline -> sink` 阶段产出率所需指标

### Requirement: Alert Thresholds for Key Failure Modes
系统 MUST 基于新一代闭环指标配置可执行告警阈值与严重级别，不得依赖已淘汰指标。

#### Scenario: Backfill session pending alert
- **WHEN** `worker_integrity_backfill_pending_duration_seconds` 在窗口内持续超阈
- **THEN** 系统 MUST 触发 backfill session 卡滞告警

#### Scenario: Backfill result failure burst alert
- **WHEN** `worker_integrity_backfill_result_total{status=~"fail|timeout"}` 在窗口内突增
- **THEN** 系统 MUST 触发补数退化告警并标注错误分类

#### Scenario: Pipeline yield degradation alert
- **WHEN** `caller_nonzero -> pipeline_finish` 或 `pipeline_finish -> sink_success` 转化率持续低于阈值
- **THEN** 系统 MUST 触发数据产出退化告警

### Requirement: Diagnostic Classification
告警事件 MUST 可区分链路异常、策略限流、消费背压与补数会话退化四类主因。

#### Scenario: Alert includes root-cause class
- **WHEN** 触发任一可靠性告警
- **THEN** 告警负载 MUST 包含 `root_cause_class` 与 `error_class` 字段用于值班分流
