## Context

当前 `orderbook` 内存状态通过全局 `store[symbol]` 共享，spot/perp 在同符号（如 `AAVEUSDT`）下会复用同一 `BookState`。在 backfill 与 snapshot 交错触发时，perp 链路可覆盖 spot 链路状态，导致 spot 角色仍持续 `caller.response` 但不再产生 `pipeline.finish` 与 Kafka 产出，形成静默故障。

约束：
- 不能破坏现有 role 生命周期与 apply/reconcile 语义。
- 需要兼容当前 `orderbook_diff -> orderbook_validator -> kafka sink` 流水线。
- 需要在不中断其他 topic 的前提下上线，支持快速回滚。

## Goals / Non-Goals

**Goals:**
- 让订单簿状态按 role/market/symbol 隔离，杜绝跨 role 状态污染。
- 明确并固化 `orderbook` 作用域键的构造规则，确保在 reconnect/backfill/reconcile 下稳定。
- 增加可观测性，能够在“有输入无产出”时快速定位到 dropped/stale/sequence 异常。
- 提供回归测试，覆盖 spot/perp 同符号并发场景。

**Non-Goals:**
- 不重写 integrity/backfill 算法。
- 不变更外部 topic 名称、消息 schema 与控制面 API 协议。
- 不在本次改造中引入新中间件或持久化状态存储。

## Decisions

### 决策 1：引入 `OrderbookScopeKey`，替代 `symbol` 全局键
- 方案：将 `store` key 从 `symbol` 升级为 `scope_key = role_id + market + exchange + symbol`（最少包含 `role_id` 与 `symbol`）。
- 原因：`role_id` 是运行时最稳定的隔离边界，叠加 market/exchange 可避免未来 role 复用风险。
- 备选方案：
  - 仅按 `market+symbol`：不足以隔离同 market 多 role。
  - 每次 apply 全量 reset 全局 store：无法防止运行中串扰，且会引入不必要抖动。

### 决策 2：`orderbook_diff` 显式接收并传递 scope_key
- 方案：在 handler 初始化与消息 metadata 中统一传递 scope_key，`orderbook.NewEngine` 按 scope_key 创建状态。
- 原因：避免隐式依赖 symbol，保证 backfill snapshot 与 diff 进入同一作用域。
- 备选方案：在 `store.Shared` 内部动态猜测 role：耦合高且不可测试。

### 决策 3：为静默丢弃增加结构化可观测性
- 方案：对 `ErrStaleUpdate`、`ErrSequenceGap`、`ErrNoSnapshot` 增加计数器和结构化日志（含 role_id、scope_key、symbol、reason）。
- 原因：当前仅日志或静默 return，无法通过指标及时发现“输入有但产出停”。
- 备选方案：全部升级为 ERROR 并中断：会放大瞬时抖动，不符合流式容错目标。

### 决策 4：reconcile 阶段做作用域级清理
- 方案：role remove/update commit 前后，按 `role_id` 清理对应 orderbook scope，防止旧实例状态泄漏到新实例。
- 原因：避免“热更新后继承脏状态”。
- 备选方案：仅在进程重启时清理：无法覆盖在线热更新。

## Risks / Trade-offs

- [Risk] scope_key 设计不当导致状态分片过细、内存膨胀  
  → Mitigation：限定 key 字段并提供 `role_id` 级清理与监控。

- [Risk] 新日志/指标增加开销  
  → Mitigation：仅对异常路径计数，正常路径保持轻量。

- [Risk] reconcile 清理时机错误导致短暂无快照  
  → Mitigation：在 commit 边界执行，并配合 backfill 快照兜底。

## Migration Plan

1. 在代码中引入 scope_key 与新 store API，但保留旧接口（短期兼容）。
2. `orderbook_diff` 与相关 handler 全部切换到新接口，并补齐日志/指标。
3. 增加单测与并发集成测试（spot/perp 同符号）。
4. 灰度发布：先观察 `spot.orderbook` 连续产出与 dropped 指标。
5. 稳定后移除旧 `symbol` 全局共享路径。

回滚策略：
- 保留 feature flag（或最小化回滚 patch）切回旧 store 键实现。
- 回滚后重启 worker 以清理隔离 key 残留状态。

## Open Questions

- scope_key 是否需要纳入 `datasource_id` 以支持未来同 role 多源并行？
- dropped 指标是否需要进入统一告警阈值（例如 1 分钟持续 >0）？
