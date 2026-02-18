## Context

当前 worker 在 AAVE orderbook 链路中承担了“接入 + 本地订单簿重建 + 输出重建簿状态”三类职责。该模型在 Binance 无 diff 区间补数接口的前提下，导致完整性链路依赖 snapshot gate 与本地簿回调，提升了接入层复杂度，也限制了研究域按原始事件流重建与修复。

本次变更目标是将 worker 收敛为接入层：输出 diff 主流与 snapshot 辅流，保留缺失检测与补数调度能力，但移除本地订单簿状态管理。

## Goals / Non-Goals

**Goals:**
- 在 worker 内实现统一的 orderbook 双流输出：`*.orderbook.diff` 与 `*.orderbook.snapshot`。
- 支持 `10s` 周期快照与 diff 缺失触发快照补数，并统一写入 snapshot topic。
- 去除 worker 本地订单簿逻辑与其对完整性放行路径的耦合。
- 保持缺失检测、补数投递、可观测性链路在新模型下可用。

**Non-Goals:**
- 不在本次变更中实现研究域订单簿重建逻辑。
- 不做旧 topic / 旧 payload 向后兼容。
- 不新增交易执行策略逻辑。

## Decisions

### Decision 1: 采用“diff 主流 + snapshot 辅流”双 topic 契约
- 方案：orderbook 数据拆分为 `perp/spot.orderbook.diff` 与 `perp/spot.orderbook.snapshot`。
- 原因：将“连续事件流”与“重锚事件”解耦，简化研究域修复流程。
- 备选方案：继续单 topic 混写 diff/snapshot。
- 不选原因：下游需要额外过滤和分流，恢复语义不清晰。

### Decision 2: 完整性引擎保留 gap 检测与 snapshot backfill，但取消本地簿 gate 依赖
- 方案：在 orderbook 场景下，gap 触发 backfill snapshot；snapshot 作为旁路事件下沉，不再依赖 `OnSnapshotApplied` 才放行 diff。
- 原因：worker 不再维护本地簿，继续使用 snapshot gate 会造成无意义阻塞。
- 备选方案：保留 `snapshot_hold` 并补充虚拟回调。
- 不选原因：增加隐式状态机，且与“无本地簿”目标冲突。

### Decision 3: 周期快照独立为 polling role（10s）
- 方案：spot/perp 各新增一个 `polling + native_call(http)` 角色。
- 原因：职责清晰、与 diff 主链路解耦，故障域独立。
- 备选方案：将周期快照内嵌到 diff role 内部定时器。
- 不选原因：会把 caller/handler 职责耦合，难以观测和灰度。

### Decision 4: 删除本地订单簿代码并同步更新 capability 规范
- 方案：删除 `orderbook_diff/orderbook_validator` 与 `resource/orderbook`，对应能力规范改为“worker 不维护订单簿状态”。
- 原因：避免死代码与双语义并存，落实全量迁移。
- 备选方案：保留代码但不引用。
- 不选原因：长期增加维护成本并影响架构边界清晰度。

## Risks / Trade-offs

- [Risk] 下游仍消费旧 orderbook topic 导致解析失败  
  → Mitigation: 一次性切换前完成 topic/契约清单审计，并在发布窗口内冻结下游 schema。

- [Risk] snapshot 旁路后，研究域若未按状态机处理会误算跨 gap 因子  
  → Mitigation: 在 snapshot 消息增加 `snapshot_source/snapshot_reason`，并要求下游按 gap 窗口截断。

- [Risk] 周期快照频率固定 10s 在极端行情不足  
  → Mitigation: 保留缺失触发 backfill snapshot 作为快速重锚兜底。

- [Risk] 删除本地簿后，原有“簿状态级”观测项消失  
  → Mitigation: 以 diff 连续性、backfill 结果、snapshot 发射量替代并更新告警基线。

## Migration Plan

1. 实现并合入新 handler 与 integrity 策略（双 topic 路由、snapshot 旁路语义）。
2. 重写 AAVE roles：diff role（spot/perp）+ snapshot polling role（spot/perp）+ 保留 aggtrade role。
3. 删除本地订单簿代码与相关注册/校验逻辑。
4. 发布前通过 `openspec` 工件与 roles 校验接口确认配置可加载。
5. 生产切换时同步切换研究域消费到新 topic，停用旧 topic 依赖。
6. 观测窗口内重点检查：diff 连续性、gap 触发率、snapshot_backfill 成功率。

## Open Questions

- 是否需要在 worker 中统一提供 `snapshot_reason` 枚举（`periodic/gap/timeout/manual`）并作为强校验字段。
- 周期快照 `10s` 是否保持静态配置，还是允许按 symbol 覆写。
