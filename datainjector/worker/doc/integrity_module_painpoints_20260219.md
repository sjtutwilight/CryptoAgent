# Integrity 模块痛点剖析（简要）

更新时间：2026-02-19  
范围：`datainjector/worker/internal/handler/integrity`

## 1. 当前状态概览

- 模块总体代码量约 `2809` 行，其中 `sequence_engine.go` 单文件 `1449` 行，核心复杂度过高。
- 当前职责集中在 `SequenceEngine`：顺序控制、gap 检测、backfill 触发、session 状态机、cooldown、compensation、gate 放行、部分可观测埋点。
- 这导致“策略演进”和“问题定位”都高度依赖单文件上下文，修改风险高。

关键位置：
- `datainjector/worker/internal/handler/integrity/sequence_engine.go:31`
- `datainjector/worker/internal/handler/integrity/sequence_engine.go:193`
- `datainjector/worker/internal/handler/integrity/sequence_engine.go:326`

## 2. 主要痛点

### 2.1 状态机耦合过重，演进成本高

- `SequenceEngine` 同时处理多策略分支（`snapshot_gate`/`snapshot_sidechannel`、result-driven session、cooldown、compensation）。
- 同一状态（`ExpectedNext/SeenMax/AwaitingSnapshot/WaitStart`）在多个路径被读写，行为不变量难以一眼确认。
- `checkTimeout`、`triggerBackfillWithSession`、`OnBackfillResult` 等路径相互影响，新增场景容易引入回归。

典型位置：
- `datainjector/worker/internal/handler/integrity/sequence_engine.go:124`
- `datainjector/worker/internal/handler/integrity/sequence_engine.go:326`
- `datainjector/worker/internal/handler/integrity/sequence_engine.go:670`

### 2.2 Snapshot 相关语义存在分叉

- `snapshot_sidechannel` 模式下，snapshot 事件仅透传，不推进本地顺序状态（`ExpectedNext`）也不清理旧 buffer。
  - `datainjector/worker/internal/handler/integrity/sequence_engine.go:158`
- 能做“按 snapshot 重锚 + 清理旧 diff”的逻辑在 `OnSnapshotApplied`，但 sidechannel 分支直接返回。
  - `datainjector/worker/internal/handler/integrity/sequence_engine.go:193`
  - `datainjector/worker/internal/handler/integrity/sequence_engine.go:196`
  - `datainjector/worker/internal/handler/integrity/sequence_engine.go:205`
- 配置中 spot/perp diff role 已使用 `orderbook_mode: snapshot_sidechannel`，因此该分叉是实战路径。
  - `datainjector/worker/configs/aave/roles_aave_full_hotfix_20260218.json:304`

### 2.3 Timeout 处理顺序对大缺口恢复不友好

- `checkTimeout` 先判断 `max_delay` 并 `return`，`hard_timeout` 可能长期无法执行推进（`advance`）。
  - `datainjector/worker/internal/handler/integrity/sequence_engine.go:335`
  - `datainjector/worker/internal/handler/integrity/sequence_engine.go:343`
- 在高频 gap + 持续 backfill 场景，容易长期维持旧 `ExpectedNext`，产生重复触发与冷却循环。

## 3. 可观测性现状与黑盒

## 3.1 已有能力（有价值但不完整）

- 日志事件较全：`gap.detected`、`backfill.trigger/dedup/session/result/skipped`。
  - `datainjector/worker/internal/observability/logging/events.go:22`
- 指标已定义：
  - `worker_integrity_gaps_total`
  - `worker_integrity_backfill_*`（enqueue、dedup、result、pending、inflight、compensation）
  - 定义位置：`datainjector/worker/internal/observability/metrics/metrics.go:163`

## 3.2 关键黑盒（当前定位困难的根因）

- **buffer 内部状态不可见**：
  - 已定义 `IntegrityBufferSize`，但没有任何调用点更新，实际永远看不到实时 buffer 大小。
  - 定义：`datainjector/worker/internal/observability/metrics/metrics.go:175`
  - 无调用：`SetIntegrityBufferSize` 仅定义，未被业务代码使用（仅函数定义位置 `:541`）。
- **dedupe 过滤不可见**：
  - 已定义 `RecordIntegrityDuplicate`，但未在 `dedupe.ShouldDrop` 或 `deliver` 路径调用。
  - 定义：`datainjector/worker/internal/observability/metrics/metrics.go:546`
  - 无调用：全仓库仅函数定义。
- **缺失窗口结构不可见**：
  - 当前仅有 gap 计数，没有“当前缺失区间数量/长度分布/最老缺口年龄/head lag”等指标。
- **状态机关键状态不可见**：
  - 无指标暴露 `ExpectedNext`、`SeenMax`、`AwaitingSnapshot`、`WaitStart`。
  - 只能靠日志推测，排障成本高。
- **OnSnapshotApplied 闭环透明度不足**：
  - 接口存在，但全仓库没有清晰业务触发链路（主要是定义/转发位置），运维视角难判断“是否真的发生了 snapshot 应用重锚”。
  - 参考：`datainjector/worker/internal/handler/integrity/handler.go:67`、`datainjector/worker/internal/handler/integrity_handler.go:57`

## 4. 单元测试不足

当前 `integrity` 目录仅 4 个测试文件：
- `handler_test.go`
- `scheduler_test.go`
- `compensation_test.go`
- `sequence_engine_session_test.go`

## 4.1 已覆盖（基础）

- range evaluator（含 `prev_final_update_id` fallback）
- scheduler 无目标/超时/队列满/dedup
- compensation 持久化重放基本路径
- session dedup/cooldown/sidechannel 基础行为

## 4.2 未覆盖（高风险）

- `buffer.go` 无专门测试：
  - `drain/cleanup/sweep` 边界（TTL、maxBuckets、重复 seq）未覆盖。
- `gate.go` 无专门测试：
  - `snapshotHoldGate`/`finalityGate` 状态迁移和释放语义未覆盖。
- `config_parser.go` 无专门测试：
  - profile 默认值、`orderbook_mode` 兼容与覆盖关系缺乏保护。
- `SequenceEngine` 主流程关键分支缺覆盖：
  - `OnSnapshotApplied` 对 `ExpectedNext`/buffer 清理行为；
  - `checkTimeout` 中 `max_delay` 与 `hard_timeout` 相互作用；
  - 断网后大缺口恢复（大量历史 diff + snapshot backfill）场景。
- 可观测性回归无测试：
  - 没有断言关键指标是否被更新（尤其 buffer/duplicate）。

## 5. 结论与建议（简要）

结论：模块已达到“需要结构化重构”的阶段，否则后续功能迭代和故障定位成本会持续上升。

建议优先级：

1. **先补可观测性缺口（低风险高收益）**
- 接入并验证：
  - `SetIntegrityBufferSize`（add/drain/cleanup/sweep 后更新）
  - `RecordIntegrityDuplicate`（dedupe drop 时）
- 新增 gauge/counter：
  - `expected_seq`、`seen_max`、`awaiting_snapshot`
  - 缺失窗口大小/最老缺口年龄

2. **补关键测试（先护栏后改造）**
- 为 `buffer.go`、`gate.go`、`config_parser.go` 增加专门单测。
- 增加“断网大缺口恢复”集成风格测试（至少在 `sequence_engine` 级别模拟）。

3. **分阶段重构 `SequenceEngine`**
- 将“顺序推进状态机”和“backfill/session 编排”拆分；
- 明确 sidechannel 下 snapshot 对本地状态的重锚策略（统一入口，避免语义漂移）。

