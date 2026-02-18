## Context

当前 worker 的完整性链路在触发 snapshot backfill 时，存在“检测器持续触发 + 调度器持续入队 + 执行器滞后”的耦合回路。对于高频订单簿流，当 backfill 通道拥塞或 HTTP 快照变慢时，会出现大量 `enqueue_timeout`，并在 `AwaitingSnapshot` 期间继续重复调度，形成告警洪泛与吞吐退化。

受影响模块跨越 `integrity/sequence_engine`、`integrity/scheduler`、`role` backfill worker 与 `types/backfill` 协议层，属于跨模块架构问题，适合先固化设计再实施。

## Goals / Non-Goals

**Goals:**
- 建立 backfill 单飞会话模型：同一 key（`role_id + stream_key + backfill_type`）同一时刻只允许一个 in-flight。
- 建立结果驱动闭环：pending 期间禁止重复调度，状态仅由 backfill result（success/fail）推进。
- 提供可去重的调度语义：相同 key 的新请求合并，不重复堆积到 channel。
- 提供可观测且可验证的不变量：并发数、重试次数、会话状态、结果延迟可观测。

**Non-Goals:**
- 不修改业务字段解析、撮合/订单簿计算逻辑。
- 不引入外部持久化依赖（例如 Redis/DB）作为第一版必选项。
- 不在本变更中统一所有非 snapshot 类回补策略（先覆盖当前痛点路径）。

## Decisions

### 1. 引入 Backfill Session 状态机（核心）
- 决策：在 `SequenceEngine` 内引入 `sessionKey -> sessionState`，状态最小集合：`idle / pending / cooldown`。
- 规则：
  - `idle` 才允许 `Schedule`。
  - `pending` 时新的触发请求仅更新“待处理意图”（merge），不再入队。
  - 收到 `success/fail` 结果后从 `pending` 迁移到 `idle` 或 `cooldown`。
- 原因：把“是否能触发下一次 backfill”的控制权从定时器转为状态机，消除风暴。
- 替代方案：仅调大 timeout/queue。结论：只能止损，不能阻止重复触发。

### 2. Backfill 命令与结果协议化
- 决策：扩展 `types.BackfillCmd` 与新增 `BackfillResult`（或等价结构），至少包含：`cmd_id/session_id/key/attempt/result/error_class/snapshot_seq/finished_at`。
- 原因：当前只有命令无结果关联，检测器无法“等待具体哪一次 backfill 完成”。
- 替代方案：靠日志字符串匹配结果。结论：不可靠且无法做幂等。

### 3. 调度器改为 Keyed 去重队列
- 决策：`scheduler` 从“写 channel 成功即算调度”改为“按 key 去重 + 队列项可覆盖 + worker 消费后回执”。
- 原因：同 key 大量重复命令本质冗余，应在调度层剪枝，而不是靠大 channel 硬扛。
- 替代方案：保留 channel，仅在 `SequenceEngine` 限流。结论：会降低但无法避免并发路径上的重复堆积。

### 4. 重试与退避下沉到执行器
- 决策：`role.handleBackfillCmd` 负责 attempt 内重试与指数退避；`SequenceEngine` 不再主动“计时重发”。
- 原因：重试策略应由实际执行结果驱动，避免检测器和执行器双重重试叠加。
- 替代方案：检测器继续 retry，执行器只单次。结论：仍会在 pending 期间制造重复命令。

### 5. 失败冷却与恢复判定
- 决策：对同一 key 的连续失败进入 cooldown；cooldown 到期后允许下一次调度。success 立即清空失败计数。
- 原因：避免下游不可用时持续打满系统。
- 替代方案：无限重试。结论：高风险。

### 6. 可观测性不变量
- 决策：增加指标/事件：
  - `backfill_sessions_inflight{role_id,stream_key,type}`
  - `backfill_schedule_dedup_total`
  - `backfill_result_total{status,error_class}`
  - `backfill_pending_duration_ms`
- 原因：没有不变量监控，无法确认“根因已消失”。

## Risks / Trade-offs

- [状态机复杂度上升] → 通过最小状态集 + 单元测试矩阵（状态迁移、并发、重复结果）控制复杂度。
- [结果事件丢失导致 pending 卡死] → 增加 pending 超时保护与补偿重放机制（超时后进入受控 fail/cooldown）。
- [去重过强导致合法补数被吞] → key 设计包含 `type + stream_key`，并保留 merge 元数据用于覆盖而非静默丢弃。
- [改造范围跨模块] → 分阶段落地：先协议与状态机，再调度器，再指标与灰度。

## Migration Plan

1. 增加协议字段与结果通道（向后兼容，老字段保留）。
2. 在 `SequenceEngine` 接入 session 状态机，但先保留旧路径开关（feature flag）。
3. 切换调度器到 keyed 去重实现，开启结果驱动路径。
4. 灰度到 AAVE perp/spot orderbook 角色，观测 `enqueue_timeout`、inflight 数、topic 增量。
5. 稳定后移除旧的高频重试路径。

回滚策略：关闭 feature flag，回退到旧调度路径（不改数据格式兼容字段）。

## Open Questions

- `BackfillResult` 是否直接走内存通道，还是统一走 status reporter/kafka 以便跨进程扩展？
- keyed 去重队列是否需要持久化（进程重启后保留 pending）作为二阶段能力？
- cooldown 参数是否按 role 固定，还是按 error_class 动态调整？
