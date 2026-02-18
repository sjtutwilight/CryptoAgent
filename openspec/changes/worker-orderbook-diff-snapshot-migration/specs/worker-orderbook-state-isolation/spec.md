## REMOVED Requirements

### Requirement: Orderbook state SHALL be isolated by role scope
**Reason**: worker 不再维护本地订单簿状态，作用域内 BookState 隔离要求失效。
**Migration**: 订单簿状态隔离迁移至研究域重建组件，worker 仅输出 diff/snapshot 事件与 role 级 metadata。

### Requirement: Snapshot and diff SHALL be applied within the same scope
**Reason**: worker 不再在接入链路内执行 snapshot/diff 合并应用。
**Migration**: 由研究域消费 `*.orderbook.diff` 与 `*.orderbook.snapshot` 后在自身状态机中完成同 scope 应用与重锚。

## MODIFIED Requirements

### Requirement: System SHALL expose detectable signals for dropped orderbook updates
当 orderbook 事件因序列缺失、投递异常或 backfill 失败导致完整性退化时，系统 MUST 记录包含 `role_id`、`symbol`、`reason` 的结构化日志，并 MUST 维护可聚合计数指标；该要求 MUST 独立于本地订单簿状态存在。

#### Scenario: Integrity degradation is observable without local book state
- **WHEN** 某 role 连续出现 gap 或 snapshot backfill 失败
- **THEN** 监控可通过 dropped/gap/backfill 指标与日志字段定位到具体 role 与原因
