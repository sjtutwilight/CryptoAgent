## Why

当前 `datainjector/worker` 在队列链路、WebSocket 链路、backfill 指令链路都存在“可见成功但真实失败”或“静默丢弃”问题：队列模式会在仅入队后上报成功，WebSocket 在慢消费时可能出现无界内存增长并伴随丢包，backfill 通道满时会直接丢弃且缺少强反馈。这些问题会直接破坏数据完整性与可追责性，必须优先收敛。

## What Changes

- 引入队列模式阶段化状态语义：区分 caller 接收成功、入队成功、pipeline 成功/失败、最终完成状态；禁止 sink/handler 失败时上报最终成功。
- 为队列消费失败建立统一的重试与死信（DLQ）机制，失败原因结构化上报，并可按 task_id/run_id 回放定位。
- 将 WebSocket caller 缓冲改为有界（按消息数与字节数双阈值），并定义明确丢弃策略（丢新/丢旧/按 stream）。
- 为 WebSocket 各层缓冲增加背压指标与告警，并在高水位状态下触发降载与补偿动作。
- 将 backfill 指令发送改为“阻塞写 + 超时 + 明确错误”，超时后进入持久化补偿队列，避免静默丢弃。
- 将 backfill 调度接口从布尔返回升级为错误语义，确保调度失败可观测、可审计、可重放。

## Capabilities

### New Capabilities
- `worker-queue-ack-semantics`: 定义队列模式下任务状态的阶段化确认、最终确认规则，以及失败重试与 DLQ 语义。
- `worker-websocket-bounded-backpressure`: 定义 WebSocket 链路有界缓冲、丢弃策略、背压观测与降载要求。
- `worker-backfill-command-delivery`: 定义 backfill 指令可靠投递、超时补偿、失败告警与重放要求。

### Modified Capabilities
- （无）

## Impact

- 受影响代码：`internal/role/role.go`、`internal/observability/status/reporter.go`、`internal/caller/native_call_websocket.go`、`internal/protocol/websocket.go`、`internal/caller/ws_shared_pool.go`、`internal/handler/integrity/scheduler.go`、`internal/handler/integrity/sequence_engine.go`。
- 受影响运行面：任务状态上报 Kafka 主题、WebSocket 内存/丢包行为、backfill 可靠性与运维告警。
- 兼容性影响：状态事件字段将扩展；backfill 调度接口签名变更需同步调用方。
