## Why

当前 `integrity` 模块将顺序推进、gap 检测、backfill 会话、snapshot 语义、gate 放行与可观测逻辑集中在 `SequenceEngine` 单体实现中，导致变更回归风险高、定位链路长、关键状态不可观测。随着 `snapshot_sidechannel` 成为生产主路径，这种耦合已成为持续交付与故障恢复的主要瓶颈，需要尽快进行结构化重构。

## What Changes

- 拆分完整性引擎为“纯状态机核心 + 编排执行层”，将顺序推进决策从 IO 路径中解耦。
- 引入统一的 snapshot 重锚语义（`ApplyAnchor`），收敛 `snapshot_gate` 与 `snapshot_sidechannel` 分叉行为。
- 明确 timeout 处理优先级：`hard_timeout` 先于 `max_delay`，避免长期无法推进。
- 在 backfill 编排层固化单飞会话与意图合并，统一 pending/cooldown 状态转移与结果闭环。
- 补齐完整性关键指标与结构化事件：buffer、duplicate、expected/seen、gap-window、snapshot-anchor 等。
- 建立状态机与可观测回归测试基线（核心状态迁移、断网大缺口恢复、sidechannel 重锚）。

## Capabilities

### New Capabilities
- `worker-integrity-core-orchestration`: 定义完整性模块的分层架构、状态机输入输出契约、统一 snapshot 重锚与超时推进规则。

### Modified Capabilities
- `worker-backfill-command-delivery`: 扩展为按 `session_key` 单飞调度与 pending 意图合并，确保同 key 同时仅一个 in-flight。
- `worker-integrity-structured-events`: 增补状态迁移、snapshot 重锚、timeout 推进与 gap-window 事件字段，形成可检索闭环。
- `worker-reliability-observability`: 新增完整性核心 gauge/histogram 指标与告警阈值建议，覆盖 head lag、窗口年龄和缓冲状态。

## Impact

- Affected code:
  - `datainjector/worker/internal/handler/integrity/sequence_engine.go`
  - `datainjector/worker/internal/handler/integrity/buffer.go`
  - `datainjector/worker/internal/handler/integrity/gate.go`
  - `datainjector/worker/internal/handler/integrity/dedupe.go`
  - `datainjector/worker/internal/handler/integrity/handler.go`
  - `datainjector/worker/internal/observability/metrics/metrics.go`
  - `datainjector/worker/internal/observability/logging/events.go`
- APIs/contracts:
  - 内部完整性引擎接口将从“直接副作用”演进为“状态迁移 + 动作执行”模型。
  - backfill 调度契约保持兼容，但会强化 session_key 单飞与结果驱动约束。
- Systems:
  - 影响 orderbook diff/snapshot 主链路的恢复语义与观测面板。
- Dependencies:
  - 不引入新的外部基础设施依赖；主要为模块内架构调整与指标扩展。
