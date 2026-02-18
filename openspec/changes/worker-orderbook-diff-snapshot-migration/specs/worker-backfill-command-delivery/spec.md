## MODIFIED Requirements

### Requirement: 不同 backfill 类型必须支持隔离限额
系统 MUST 提供至少 snapshot 与 range 的独立队列或配额，避免单类型拥塞阻塞全部补数；在 orderbook 场景下，snapshot backfill MUST 支持旁路输出至 snapshot topic，不得因主 diff 流处理路径阻塞而丢失。

#### Scenario: range 拥塞不阻塞 snapshot
- **WHEN** range backfill 队列持续拥塞
- **THEN** snapshot backfill 仍 SHALL 在其独立配额内继续投递

#### Scenario: Orderbook snapshot backfill is delivered as side-channel output
- **WHEN** orderbook diff 缺失触发 snapshot backfill 且请求成功
- **THEN** 系统 MUST 投递并下沉 snapshot 结果，即使 diff 主流仍在处理乱序缓存

### Requirement: 投递失败必须进入持久化补偿并可重放
系统 MUST 在 backfill 投递超时后将指令写入持久化补偿队列，并支持后台重放直到成功或显式终止；对于 snapshot backfill 重放，系统 MUST 保留可追踪来源字段。

#### Scenario: Snapshot backfill replay preserves traceability
- **WHEN** snapshot backfill 从持久化补偿队列重放成功
- **THEN** 下游可观测事件 MUST 包含可追踪该补偿来源的标识字段
