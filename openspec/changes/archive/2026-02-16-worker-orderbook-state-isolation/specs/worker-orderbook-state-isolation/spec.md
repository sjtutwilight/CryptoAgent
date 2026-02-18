## ADDED Requirements

### Requirement: Orderbook state SHALL be isolated by role scope
系统 MUST 使用作用域键（至少包含 `role_id` 与 `symbol`，并可扩展 `market/exchange`）来创建和读取订单簿状态。系统 MUST NOT 再以 `symbol` 作为全局共享状态键。

#### Scenario: Spot and perp with same symbol do not share state
- **WHEN** `rec-binance-spot-aave-orderbook-full` 与 `rec-binance-perp-aave-orderbook-full` 同时处理 `AAVEUSDT`
- **THEN** 两个 role 使用不同作用域键并维护独立 `BookState`

### Requirement: Snapshot and diff SHALL be applied within the same scope
系统 MUST 确保 backfill/snapshot 与实时 diff 在同一作用域内应用，且一个作用域的 snapshot MUST NOT 覆盖另一个作用域的 `lastUpdateId`。

#### Scenario: Perp backfill does not reset spot sequence
- **WHEN** perp role 触发 snapshot backfill 且 spot role 正在消费 diff
- **THEN** spot role 的 `lastUpdateId` 与 depth 状态保持连续，不因 perp snapshot 被重置

### Requirement: System SHALL expose detectable signals for dropped orderbook updates
当订单簿更新因 stale/sequence-gap/no-snapshot 被丢弃时，系统 MUST 记录包含 `role_id`、`scope_key`、`symbol`、`reason` 的结构化日志，并 MUST 维护可聚合计数指标。

#### Scenario: Silent stop becomes observable
- **WHEN** 某 role 连续出现 stale update 被丢弃
- **THEN** 监控可通过 dropped 指标与日志字段定位到具体 role 与作用域
