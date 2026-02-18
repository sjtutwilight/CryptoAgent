## ADDED Requirements

### Requirement: Worker 可观测发布必须采用单代指标语义
系统 MUST 在 Worker 可观测发布后仅保留新一代指标语义，不得同时保留旧指标兼容路径。

#### Scenario: 旧指标引用被清理
- **WHEN** 发布完成后检查 Prometheus 告警规则与 Grafana Worker 仪表盘
- **THEN** 系统 MUST 不再引用旧指标（例如 `worker_integrity_backfills_total`）

#### Scenario: 新指标闭环可用
- **WHEN** Worker 正常运行并产生完整性事件
- **THEN** 系统 MUST 可观测到 `worker_integrity_backfill_result_total`、`worker_integrity_backfill_sessions_inflight`、`worker_integrity_backfill_pending_duration_seconds`、`worker_integrity_backfill_schedule_dedup_total`

### Requirement: Worker 抓取目标必须与部署拓扑一致
系统 MUST 使 `job=worker` 的抓取目标与容器拓扑可达地址一致，确保可用性指标真实反映运行状态。

#### Scenario: 容器部署抓取成功
- **WHEN** Worker 以容器方式部署在 compose 网络内
- **THEN** Prometheus MUST 能持续抓取 Worker `/metrics` 且 `up{job="worker"}` 为 1

#### Scenario: 抓取目标不可达时可被快速识别
- **WHEN** 抓取目标配置错误或网络不可达
- **THEN** 系统 MUST 在告警与看板中直接暴露 Worker 抓取不可用状态

### Requirement: Worker 日志检索必须具备事件级维度
系统 MUST 对 Worker 结构化日志提供事件级检索能力，支持按关键维度快速过滤与关联。

#### Scenario: 事件维度检索
- **WHEN** 值班人员在 Logs 看板查询 Worker 异常
- **THEN** 系统 MUST 支持按 `event`、`role_id`、`error_class` 进行快速过滤

#### Scenario: 会话链路追踪
- **WHEN** 排查 backfill 会话卡住或重复触发问题
- **THEN** 系统 MUST 支持基于 `session_key` 与 `cmd_id` 在日志中关联会话生命周期
