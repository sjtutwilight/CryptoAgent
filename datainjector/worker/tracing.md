Tracing 方案

目标：以 worker 为全链路起点，做到“可按 run_id/role 追踪一段测试/任务”，并能与下游 Kafka/ClickHouse/Flink 处理链路关联；满足低开销、可采样、可配置、可扩展的业界最佳实践。
标准：采用 OpenTelemetry（OTel）+ W3C Trace Context（traceparent/tracestate）+ baggage；默认 parent-based 采样，按 run_id/role_id 可强制采样。
Trace 模型与传播

根 Trace 产生规则

若入站事件/命令携带 traceparent（KafkaCommand emitter 或 RPC），则 延续该 trace。
否则在 每次 emitter.fire()（或每次 WebSocket 拉取批次）创建 root span，并生成新 trace。
run_id 不作为 trace_id，而作为 baggage 与 span attribute（避免 trace_id 冲突/不规范）。
传播载体

内存消息：在 types.Message.Metadata 中存放 traceparent, tracestate, baggage, run_id。
Kafka：写入 Kafka Headers，键值示例：traceparent, tracestate, baggage, x-run-id。
Payload：继续保留 run_id 字段，便于非追踪系统也能定位测试数据（你们的 probe 已依赖它）。
Span 划分（面向当前 worker 组件）

role 生命周期（低频）

role.start / role.stop：记录 role 基本信息与配置版本。
每次触发 / 批次（高频可采样）

emitter.fire：根 span（或 parent span），属性包含 role.id, emitter.type, run_id, task_id。
caller.call：外部调用/订阅拉取 span；WS/HTTP 使用语义化属性（net.peer.name, http.method 等）。
queue.enqueue / queue.dequeue：异步边界建议使用 Span Links（producer span link → consumer span），避免超长链路。
handler.<name>：每个 handler 作为子 span；错误时 recordException + Status=ERROR。
sink.write：写入 Kafka/Console；Kafka 采用 messaging.* 语义属性。
关键属性（建议统一命名）

任务类：role.id, role.emitter, role.caller, role.sink
测试类：test.run_id（或 run_id）、test.scenario
消息类：messaging.system=kafka, messaging.destination=binance.kline, messaging.operation=send
数据类：exchange, symbol, interval, ingest_time, event_time
运行环境：service.name=datainjector-worker, service.instance.id, deployment.environment
采样策略

默认：parentbased_traceidratio（例如 1%）。
强制：若 run_id 存在或 role_id 属于测试/灰度清单，提升到 100%。
兜底：高吞吐 WS 场景可设 “只在 handler/sink 出错时采样”。
日志/指标联动

日志：统一注入 trace_id, span_id, run_id；在错误日志与 span 之间互相跳转。
指标：建议同时产出 queue_depth, caller_latency, handler_latency, sink_error_rate 等，作为 tracing 的补充。
配置建议（示例）

tracing:
  enabled: true
  service_name: datainjector-worker
  exporter: otlp
  otlp_endpoint: http://otel-collector:4317
  sampler:
    type: parentbased_traceidratio
    ratio: 0.01
  force_sample:
    run_id: true
    role_ids: ["binance-kline-e2e"]
  propagation:
    - tracecontext
    - baggage
对现有实现的最小侵入点

Role.fire()/consume()：创建/提取 context；将 traceparent 注入到 Message.Metadata。
Caller：每次外部请求创建 span；从 args.metadata 提取 run_id。
Handler Chain：为每个 handler 创建子 span；对异步/过滤场景可用 span event。
Sink.Kafka：写入 Kafka Headers（新增 header 支持）。
probe/测试：继续通过 payload run_id 验证，同时可加入 trace 过滤。
与下游系统对接

Kafka → Flink：Flink 侧可用 Java OTel Kafka instrumentation，读取 headers 即可恢复 trace。
ClickHouse：写入结果表可追加 trace_id/run_id 维度（可选），便于离线回溯。
风险与边界

高吞吐 WS：必须采样/聚合，避免每条消息都创建 span。
run_id 只用于关联，不替代标准 trace_id。
