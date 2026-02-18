# Worker Logging Spec

目标：统一事件名与字段，保证日志可读、可检索、可被 probe 稳定消费。

## 基本约定

- 所有关键链路日志必须使用结构化 JSON（observability/logging）。
- event 名称固定，新增事件必须在本文登记。
- 禁止在关键链路中使用 `log.Printf` 输出业务日志。

## 字段规范

必填字段（所有结构化日志必须包含）：
- ts
- level
- event
- message
- service

建议字段（能提供就写）：
- role_id
- run_id
- task_id
- trace_id
- span_id
- parent_span_id
- elapsed_ms
- msg_count
- pipeline
- queue_mode

错误字段（仅错误事件写入）：
- error_type
- error_detail

## 事件字典（单一事实源）

### Role 生命周期

- role.start
  - 触发：`internal/role/role.go` Start
  - 字段：role_id/emitter/caller/pipeline_mode/queue_mode
- role.stop
  - 触发：`internal/role/role.go` Start defer
  - 字段：role_id
- role.startup
  - 触发：`cmd/worker/main.go` 无启动 roles 时

### Emitter

- emitter.fire
  - 触发：`internal/role/role.go` fireFunc
  - 字段：role_id/run_id/task_id

### Caller

- caller.request
  - 触发：`internal/role/role.go` caller.CallOnce 前
  - 字段：role_id/run_id/task_id
- caller.response
  - 触发：`internal/role/role.go` caller.CallOnce 后
  - 字段：role_id/run_id/task_id/elapsed_ms/msg_count/pipeline/queue_mode
- caller.error
  - 触发：`internal/role/role.go` caller.CallOnce error
  - 字段：role_id/run_id/task_id/retryable/status_code/error_*

### Handler / Sink

- handler.error
  - 触发：`internal/role/role.go` handleDirect/consume
  - 字段：role_id/handler/run_id/task_id/pipeline/error_*
- sink.error
  - 触发：`internal/role/role.go` handleDirect/consume
  - 字段：role_id/run_id/error_*

### Pipeline

- pipeline.finish
  - 触发：`internal/role/role.go` handleDirect/consume
  - 字段：role_id/run_id/msg_count
- pipeline.error
  - 触发：`internal/role/role.go` direct pipeline error
  - 字段：role_id/run_id/error_*
- queue.enqueue
  - 触发：`internal/role/role.go` enqueue error
  - 字段：role_id/run_id/task_id/error_*

### Integrity / Backfill

- integrity.gap.detected
  - 触发：`internal/handler/integrity/sequence_engine.go` onGap
  - 字段：role_id/stream_key/expected_seq/seen_seq/gap
- integrity.advance
  - 触发：`internal/handler/integrity/sequence_engine.go` advance
  - 字段：role_id/stream_key/expected_prev/expected_new/reason
- integrity.backfill.trigger
  - 触发：`internal/handler/integrity/sequence_engine.go` triggerBackfill、`internal/role/role.go` handleBackfillCmd
  - 字段：role_id/stream_key/backfill_type/start/end/attempts/cmd_id/session_id/session_key
- integrity.backfill.retry
  - 触发：`internal/handler/integrity/sequence_engine.go` retrySnapshotBackfill
  - 字段：role_id/stream_key/expected_seq/seen_max/elapsed_ms
- integrity.backfill.skipped
  - 触发：`internal/handler/integrity/sequence_engine.go` triggerBackfill（无可执行 attempt）、`internal/role/role.go` handleBackfillCmd（空 attempts）
  - 字段：role_id/stream_key/start/end/attempts
- integrity.backfill.dedup
  - 触发：`internal/handler/integrity/sequence_engine.go` triggerBackfillWithSession（pending 期间合并意图）
  - 字段：role_id/stream_key/backfill_type/session_key/intent_start/intent_end
- integrity.backfill.session
  - 触发：`internal/handler/integrity/sequence_engine.go` triggerBackfillWithSession（会话进入 pending）
  - 字段：role_id/stream_key/backfill_type/session_key/session_id/cmd_id/attempt/start/end
- integrity.backfill.attempt.error
  - 触发：`internal/role/role.go` executeBackfillAttempt request error、handleBackfillCmd attempt fail
  - 字段：role_id/worker_id/backfill_type/attempt/request_step/status_code/retryable/error
- integrity.backfill.enqueue.error
  - 触发：`internal/role/role.go` executeBackfillAttempt enqueue error
  - 字段：role_id/worker_id/backfill_type/attempt/request_step/error_*
- integrity.backfill.success
  - 触发：`internal/role/role.go` handleBackfillCmd
  - 字段：role_id/worker_id/backfill_type/start/end/attempt/cmd_id/session_id
- integrity.backfill.exhausted
  - 触发：`internal/role/role.go` handleBackfillCmd
  - 字段：role_id/worker_id/backfill_type/start/end/attempts/error_*
- integrity.backfill.result
  - 触发：`internal/handler/integrity/sequence_engine.go` OnBackfillResult
  - 字段：role_id/stream_key/backfill_type/session_key/session_id/cmd_id/status/error_class/pending_ms/state

### Orderbook Diff/Snapshot

- orderbook.snapshot.emit
  - 触发：`internal/handler/orderbook_handlers.go` orderbook_topic_router
  - 字段：role_id/symbol/exchange/snapshot_source/snapshot_reason/last_update_id

### WebSocket（Protocol / Caller）

- ws.connect
  - 触发：`internal/protocol/websocket.go` Connect
  - 字段：ws_url
- ws.close
  - 触发：`internal/protocol/websocket.go` Close
  - 字段：ws_url
- ws.read.error
  - 触发：`internal/protocol/websocket.go` readLoop
  - 字段：ws_url/error
- ws.heartbeat.error
  - 触发：`internal/protocol/websocket.go` heartbeatLoop
  - 字段：ws_url/error
- ws.subscribe.sent
  - 触发：`internal/protocol/websocket.go` Subscribe/SendRawSubscribe
  - 字段：ws_url/payload/payload_len/raw
- ws.unsubscribe.sent
  - 触发：`internal/protocol/websocket.go` Unsubscribe
  - 字段：ws_url/payload/payload_len
- ws.reconnect.start / ws.reconnect.error / ws.reconnect.success
  - 触发：`internal/protocol/websocket.go` reconnect
  - 字段：ws_url/attempt/backoff_ms/error
- ws.subscribe.retry_error / ws.subscribe.retry_success
  - 触发：`internal/protocol/websocket.go` reconnect resubscribe
  - 字段：ws_url/error
- ws.buffer.drop
  - 触发：`internal/protocol/websocket.go` readLoop drop
  - 字段：ws_url/buffer

- ws.init
  - 触发：`internal/caller/native_call_websocket.go` NewWebSocketCall
  - 字段：message_format
- ws.init.connect_error
  - 触发：`internal/caller/native_call_websocket.go` NewWebSocketCall connect error
  - 字段：error
- ws.connect.pending
  - 触发：`internal/caller/native_call_websocket.go` CallOnce ensureConnected
  - 字段：error
- ws.subscribe.requested
  - 触发：`internal/caller/native_call_websocket.go` CallOnce sendSubscribe
  - 字段：subscription
- ws.subscribe.parse_error
  - 触发：`internal/caller/native_call_websocket.go` refreshSubscribeRequest
  - 字段：error
- ws.subscribe.build_error
  - 触发：`internal/caller/native_call_websocket.go` refreshSubscribeRequest
  - 字段：error
- ws.message.process_error
  - 触发：`internal/caller/native_call_websocket.go` receiveMessages
  - 字段：error
- ws.subscribe.ack_parse_error
  - 触发：`internal/caller/native_call_websocket.go` handleJSONRPCMessage
  - 字段：error
- ws.subscribe.ack
  - 触发：`internal/caller/native_call_websocket.go` handleJSONRPCMessage
  - 字段：subscription_id
- ws.heartbeat.payload_error
  - 触发：`internal/caller/native_call_websocket.go` extractHeartbeatPayload
  - 字段：error

### Metrics / Status / API

- metrics.starting / metrics.start / metrics.error / metrics.stop
  - 触发：`internal/observability/metrics/server.go` + `cmd/worker/main.go`
  - 字段：port/error
- status.disabled / status.init / status.close_error
  - 触发：`internal/observability/status/reporter.go`
  - 字段：topic/brokers/error
- api.shutdown
  - 触发：`cmd/worker/main.go`

## 变更流程

新增/变更事件时：
1) 更新本文件的事件字典
2) 添加/更新 `observability/logging/events.go`
3) 若被 probe 使用，更新 probe 解析规则
