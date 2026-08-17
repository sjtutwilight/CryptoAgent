# Integrity 模块提取说明

## 建模粒度

本工件只把 `datainjector/worker/internal/handler/integrity` 视为一个模块级领域：`流数据完整性治理`。

连续性判断、下游放行和恢复协调是该领域内的三个能力，不再拆成子领域。Schema 中与本模块无关的 Family、Process、Assertion、Knowledge Base、Canonical Individual 等结构均未使用。

领域工件只保留：

- 一个核心 Entity：按完整流身份隔离的 `流完整性会话`；
- 五个必要的 Value Object：流身份、序列区间、带序列观察、完整性策略、恢复计划；
- 四个必要 Predicate：坐标可解释、状态按流隔离、观察可下发、恢复责任已有去向；
- 三项能力：决定观察处置、应用权威锚点、协调恢复责任；
- 两项设施需求：完整性状态与期限支持、恢复责任交接与保全；
- 五条不能留给代码自行决定的不变量。

## 关键棕地裁决

### 状态必须按流隔离

当前 `SequenceEngine` 只有一份 state、buffer 和 gap 状态，而 `streamName` 会随观察改写：[sequence_engine.go:32](../../datainjector/worker/internal/handler/integrity/sequence_engine.go#L32)、[sequence_engine.go:611](../../datainjector/worker/internal/handler/integrity/sequence_engine.go#L611)。领域模型规定完整性会话身份为 `(responsibility_id, stream_key)`，所有状态必须随之隔离。

### 快照收到不等于快照应用

只有下游确认快照已成功应用，才能解除 snapshot gate。快照消息到达、已下发、恢复执行 success 和 `SnapshotSeq` 都不能替代该事实：[handler.go:67](../../datainjector/worker/internal/handler/integrity/handler.go#L67)、[sequence_engine.go:822](../../datainjector/worker/internal/handler/integrity/sequence_engine.go#L822)。

### finality 由后继观察建立

当前 finality gate 只在已下发后推进确认窗口，而是否下发又依赖该窗口，存在循环闭锁：[gate.go:92](../../datainjector/worker/internal/handler/integrity/gate.go#L92)、[gate.go:104](../../datainjector/worker/internal/handler/integrity/gate.go#L104)。模型规定先保管目标观察，再由同流后继观察形成确认资格。

### 前跳必须声明损失

hard timeout、max gap、TTL 和容量淘汰都不能表现为正常连续成功；必须记录被跳过范围和剩余恢复责任：[sequencer_core.go:110](../../datainjector/worker/internal/handler/integrity/sequencer_core.go#L110)、[buffer.go:82](../../datainjector/worker/internal/handler/integrity/buffer.go#L82)。

### 恢复执行成功不等于连续性恢复

必须区分派发接受、请求执行 success、数据重新注入、缺口闭合或快照应用。当前 `BackfillResult` 只用于收敛恢复会话：[backfill_orchestrator.go:135](../../datainjector/worker/internal/handler/integrity/backfill_orchestrator.go#L135)。

### pending 意图不能被静默去重

结果驱动路径会合并新区间：[backfill_orchestrator.go:55](../../datainjector/worker/internal/handler/integrity/backfill_orchestrator.go#L55)。普通 Scheduler 只按 lane key 去重，可能吞掉不同范围：[scheduler.go:49](../../datainjector/worker/internal/handler/integrity/scheduler.go#L49)。模型规定必须覆盖、合并或明确拒绝并保留责任。

## Schema v6 旁路缺口

只记录会影响本模块准确实现的缺口：

1. Capability 和 Outcome 没有 typed outputs，无法正式表达 `Handle` 返回的有序消息集合。
2. Outcome 没有 `succeeded/partial/blocked/failed/unknown` 完成分类，降级前跳只能依赖 meaning 与 remaining responsibility 表达。
3. Effect 不能更新 Entity 数值属性，无法形式表达 expected、seen 和 gap 区间演进。
4. Strategy candidate 没有 typed value，因此本工件用 `IntegrityPolicy` 输入承载成组策略参数。
5. Expression 缺少集合量词，无法自动验证“每流最多一个 pending”和“输出严格连续”。
6. Process 缺少 reactive stream 语义，因此未用 Process 重写消息循环。

这些缺口不阻止模块级工件使用，但意味着部分语义只能由规则定义、Outcome evidence 和 deterministic behavior contract 承载。

## 边界

HTTP、WebSocket、RPC、channel、文件路径、缓存结构、锁、goroutine、Prometheus、endpoint、method、header 和 query 均未进入领域模型。它们属于后续 System Model 或描述性知识。
