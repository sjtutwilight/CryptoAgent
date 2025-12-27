package logging

const (
	EventRoleStart   = "role.start"
	EventRoleStop    = "role.stop"
	EventRoleStartup = "role.startup"

	EventEmitterFire = "emitter.fire"

	EventCallerRequest  = "caller.request"
	EventCallerResponse = "caller.response"
	EventCallerError    = "caller.error"

	EventHandlerError = "handler.error"
	EventSinkError    = "sink.error"

	EventPipelineFinish = "pipeline.finish"
	EventPipelineError  = "pipeline.error"
	EventQueueEnqueue   = "queue.enqueue"

	EventWSConnect            = "ws.connect"
	EventWSClose              = "ws.close"
	EventWSReadError          = "ws.read.error"
	EventWSHeartbeatError     = "ws.heartbeat.error"
	EventWSSubscribeSent      = "ws.subscribe.sent"
	EventWSUnsubscribeSent    = "ws.unsubscribe.sent"
	EventWSReconnectStart     = "ws.reconnect.start"
	EventWSReconnectError     = "ws.reconnect.error"
	EventWSReconnectSuccess   = "ws.reconnect.success"
	EventWSSubscribeRetryErr  = "ws.subscribe.retry_error"
	EventWSSubscribeRetryOK   = "ws.subscribe.retry_success"
	EventWSBufferDrop         = "ws.buffer.drop"
	EventWSInit               = "ws.init"
	EventWSInitConnectError   = "ws.init.connect_error"
	EventWSConnectPending     = "ws.connect.pending"
	EventWSSubscribeRequested = "ws.subscribe.requested"
	EventWSSubscribeParseErr  = "ws.subscribe.parse_error"
	EventWSSubscribeBuildErr  = "ws.subscribe.build_error"
	EventWSMessageProcessErr  = "ws.message.process_error"
	EventWSSubscribeAckParse  = "ws.subscribe.ack_parse_error"
	EventWSSubscribeAck       = "ws.subscribe.ack"
	EventWSHeartbeatPayload   = "ws.heartbeat.payload_error"

	EventMetricsStarting = "metrics.starting"
	EventMetricsStart    = "metrics.start"
	EventMetricsError    = "metrics.error"
	EventMetricsStop     = "metrics.stop"

	EventStatusDisabled  = "status.disabled"
	EventStatusInit      = "status.init"
	EventStatusCloseErr  = "status.close_error"

	EventWorkerShutdown = "worker.shutdown"
	EventAPIShutdown    = "api.shutdown"
)
