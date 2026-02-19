## Context

当前 `datainjector/worker/internal/handler/integrity/sequence_engine.go` 承担了顺序状态机、补数会话、快照等待、gate 放行、缓冲清理与部分可观测职责，单文件复杂度和变更耦合过高。生产配置已大量使用 `snapshot_sidechannel`，但与 `OnSnapshotApplied` 的重锚语义存在分叉，导致恢复路径和排障链路不一致。现有指标中 `IntegrityBufferSize`、`IntegrityDuplicates` 已定义但未接入，关键状态（`ExpectedNext/SeenMax/AwaitingSnapshot`）与缺失窗口不可观测，无法支撑快速根因定位。

## Goals / Non-Goals

**Goals:**
- 将完整性模块拆分为“纯状态机核心（Sequencer Core）+ 编排执行层（Orchestrator）”，降低耦合并提高可测试性。
- 统一 snapshot 重锚行为，确保 `snapshot_gate` 与 `snapshot_sidechannel` 在状态推进上的一致性。
- 修正 timeout 判定优先级，保证 `hard_timeout` 不被 `max_delay` 长期饿死。
- 固化 backfill 单飞会话、不变量与结果闭环，降低重复触发与冷却抖动。
- 建立完整性关键指标、结构化事件与告警阈值建议，消除关键黑盒。

**Non-Goals:**
- 不引入新的外部存储（如 Redis/DB）作为第一阶段必选依赖。
- 不改变业务消息 payload 契约与 topic 语义（diff/snapshot 仍维持现有输出模式）。
- 不在本次设计中改造非完整性模块的调用方框架。

## Decisions

### Decision 1: 引入 Sequencer Core 纯状态机内核
- 方案：新增核心接口 `Step(state, input) -> (newState, actions)`，只负责状态迁移与动作决策，不执行 IO。
- 原因：将复杂策略分支从副作用路径中隔离，可直接做表驱动与属性测试。
- 备选：在现有 `SequenceEngine` 内继续按函数拆分。
- 不选原因：仍共享可变状态与副作用，无法真正隔离回归面。

### Decision 2: 统一 snapshot 重锚入口为 `ApplyAnchor(lastSeq, source)`
- 方案：无论 snapshot 事件还是 `OnSnapshotApplied` 回调，最终都走同一重锚动作（更新 `ExpectedNext/SeenMax` 并清理旧 buffer）。
- 原因：消除 sidechannel 与 gate 路径语义漂移，确保恢复行为可预测。
- 备选：保留 sidechannel“仅透传、不推进”语义。
- 不选原因：会长期保留两套状态解释，增加运维与排障歧义。

### Decision 3: timeout 判定采用 `hard_timeout` 优先
- 方案：每次事件或 tick 先检查 `hard_timeout`（必要时 `advance`），再检查 `max_delay`（触发 backfill 尝试）。
- 原因：保证推进上界，避免大缺口+持续补数时长期卡在旧 `ExpectedNext`。
- 备选：维持现有 `max_delay` 优先并在尾部补判 `hard_timeout`。
- 不选原因：在高频 gap 场景仍可能被前序返回路径吞掉推进机会。

### Decision 4: Backfill 编排从状态机剥离为独立 Orchestrator
- 方案：将 session 状态（idle/pending/cooldown）、intent merge、pending timeout、compensation replay 下沉到编排层；核心仅产出 `TriggerBackfill` 动作。
- 原因：完整性恢复可观测与调度治理属于控制平面，不应与顺序推进耦合。
- 备选：保留在 `SequenceEngine` 里并继续扩展。
- 不选原因：会继续放大单体复杂度，增加策略演进成本。

### Decision 5: 以缺失窗口模型补齐观测面
- 方案：新增 gap-window 视图（窗口数、总缺失长度、最老缺口年龄、head lag），并接入已定义未使用指标。
- 原因：仅有 `gaps_total` 无法解释当前恢复压力与积压规模。
- 备选：仅增强日志。
- 不选原因：日志无法稳定做聚合告警与趋势诊断。

## Risks / Trade-offs

- [Risk] 状态机拆分初期可能引入行为回归  
  → Mitigation: 先补充现有行为回归测试，再以金丝雀角色灰度切换。

- [Risk] 统一重锚后 sidechannel 行为变化影响下游节奏  
  → Mitigation: 通过 feature flag 分阶段开启，观测 snapshot-anchor 事件与下游 lag。

- [Risk] 指标维度扩充导致 Prometheus 基数上涨  
  → Mitigation: 仅使用低基数标签（`role_id`,`stream_key`,`mode`,`reason`），禁止 `session_id/cmd_id` 入标签。

- [Risk] 调度剥离后跨模块调试成本上升  
  → Mitigation: 统一 action trace_id 与结构化事件字段，保证链路可串联。

## Migration Plan

1. Phase 0（观测先行）：接入 `SetIntegrityBufferSize`、`RecordIntegrityDuplicate`，新增状态与窗口指标，不改行为。
2. Phase 1（内核抽离）：引入 `Sequencer Core`，保持旧 `SequenceEngine` 作为执行壳，行为对齐现网。
3. Phase 2（语义收敛）：落地统一 `ApplyAnchor`，收敛 `snapshot_gate`/`snapshot_sidechannel` 差异。
4. Phase 3（恢复治理）：切换 `hard_timeout` 优先与 Orchestrator 单飞编排路径。
5. Phase 4（收尾下线）：移除旧分叉逻辑与重复状态字段，固化新测试与告警基线。

Rollback:
- 通过 feature flag 回退到旧路径（旧 `SequenceEngine` 分支），保留新增观测不回滚。

## Open Questions

- `ApplyAnchor` 在 sidechannel 模式下是否要求 snapshot 必须携带可解析 `lastSeq`，还是允许“仅记录事件不重锚”的兼容降级路径。
- 缺失窗口指标是否需要 role 级聚合（跨 stream）用于值班总览。
- 是否在二阶段引入轻量持久化 pending session 以提升进程重启后的恢复连续性。
