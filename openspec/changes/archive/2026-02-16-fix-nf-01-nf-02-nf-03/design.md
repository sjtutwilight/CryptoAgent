## Context

当前 worker 在三条关键链路存在一致性缺口：
- 队列模式把“入队成功”当成“任务成功”，与真实处理结果脱钩。
- WebSocket 从协议层到 caller 层均存在缓冲丢弃路径，且 caller 聚合缓冲无上限。
- backfill 调度使用非阻塞发送，通道拥塞时会直接返回失败并在上层被弱化处理。

改造约束：
- 需要兼容当前 role 配置与现有 emitter/caller/handler/sink 组合。
- 不能把吞吐回退到不可接受水平，必须在可靠性和吞吐之间可配置权衡。
- 观测指标与日志字段要可用于生产定位（按 role_id/task_id/run_id/stage 聚合）。

## Goals / Non-Goals

**Goals:**
- 建立队列模式的最终确认语义，确保最终状态与 sink 落地结果一致。
- 将 WebSocket 链路所有缓冲点改为有界并可观测，避免内存无界增长。
- 将 backfill 指令投递改为“可失败可追踪可补偿”，消除静默丢弃。
- 提供可渐进发布的配置开关，支持灰度与回滚。

**Non-Goals:**
- 不在本次变更中重写 integrity 算法本身（如 gap 判定策略）。
- 不引入新的外部控制面协议。
- 不改变业务消息 payload 格式。

## Decisions

### 1. 队列链路采用“阶段化状态 + 最终聚合确认”
- 决策：在 `role` 内引入 `TaskTracker`，以 `task_id + run_id` 为 key 追踪 `expected/enqueued/processed/failed/retried/dlq`；仅当全部消息处理完成且失败数为 0 时发送最终 `SUCCESS`。
- 原因：直接在 `fireFunc` 上报成功无法覆盖异步消费失败；聚合确认可对齐真实完成语义。
- 备选方案：
  - 方案A：仅把 `reportSuccess` 延后到 `consume` 成功后。问题是多消息 task 无法得知“全部完成”。
  - 方案B：把每条消息都单独映射 task。问题是破坏现有 task 语义。

### 2. 消费失败统一进入“有限重试 + DLQ”
- 决策：对 handler/sink 错误执行指数回退重试，超过阈值写入 DLQ 并标记任务最终失败。
- 原因：当前失败 `continue` 导致数据隐性丢失；DLQ 提供可追责与回放入口。
- 备选方案：仅记录日志不重试，风险不可接受。

### 3. WebSocket 缓冲统一有界化（消息数+字节数双阈值）
- 决策：
  - 协议层 `msgChan` 维持有界并记录丢弃计数。
  - shared hub subscriber channel 维持有界并按订阅方记录丢弃。
  - caller `msgBuffer` 改为 ring buffer，配置 `max_messages`、`max_bytes`、`drop_policy`。
- 原因：单一层面限流不足以控制整体内存；caller 层无界是主要爆点。
- 备选方案：只扩大缓冲上限。问题是仍然无界风险，且无法定义稳态行为。

### 4. Backfill 调度接口从 `bool` 升级为 `error`
- 决策：`Schedule/Handle` 返回 `error`（如 `ErrQueueFull`、`ErrTimeout`、`ErrNoTarget`），并支持 `enqueue_timeout_ms`。
- 原因：布尔值无法表达失败类型，导致上层无法做差异化处理与告警。
- 备选方案：保留 `bool` 并靠日志区分，信息损失且难监控。

### 5. Backfill 投递失败进入持久化补偿队列
- 决策：当内存队列超时，写入持久化补偿（优先 Kafka 主题；无 Kafka 时本地 SQLite 文件队列），后台 worker 重放。
- 原因：保证高峰时不丢指令，同时可运维排障。
- 备选方案：阻塞直到成功。问题是可能放大上游阻塞并触发级联超时。

### 6. 状态/指标统一扩展字段并保持向后兼容
- 决策：状态事件新增可选字段 `stage`、`error_class`、`attempt`、`role_id`、`run_id`；旧消费者不识别时可忽略。
- 原因：需要阶段级可观测性，同时避免破坏现网消费端。

## Risks / Trade-offs

- [风险] 额外状态跟踪增加内存与锁竞争。→ Mitigation: 使用分片 map + TTL 回收，限定最大并发 task 数并暴露指标。
- [风险] 有界缓冲会提高丢弃概率。→ Mitigation: 明确丢弃策略并联动 backfill 补偿，确保最终一致。
- [风险] DLQ/补偿队列可能堆积。→ Mitigation: 增加积压告警阈值与重放限流，支持按 role 暂停重放。
- [风险] 接口签名变更影响调用方。→ Mitigation: 提供兼容适配层，先内部迁移后删除旧签名。

## Migration Plan

1. 增加新配置项并设置保守默认值（默认行为与旧版接近）。
2. 部署包含新指标/日志但不启用严格失败策略的版本，观察 24 小时。
3. 灰度开启：先开 backfill error 语义，再开 WS caller 有界 ring，最后开队列最终确认与 DLQ 终态。
4. 全量后执行故障注入回归（sink fail、WS 背压、backfill 队列满）。

回滚策略：
- 通过配置关闭 `strict_task_finalization`、`ws_bounded_buffer`、`backfill_persistent_compensation`，恢复旧路径。
- 保留新增日志与指标，便于回滚后继续定位问题。

## Open Questions

- 持久化补偿默认介质是否统一为 Kafka（需确认运维环境是否全覆盖）。
- 任务终态是否需要控制面显式 ACK（当前设计为 worker 侧单向上报）。
- WS 丢弃策略默认值是否按数据源类型差异化（例如订单簿优先丢旧、链上事件优先丢新）。
