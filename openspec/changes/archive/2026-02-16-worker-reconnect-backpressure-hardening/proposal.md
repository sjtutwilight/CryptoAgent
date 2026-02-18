## Why

当前 `datainjector/worker` 的 spot WebSocket 链路出现持续协议级读错（`Pong timeout`、`flate: corrupt input`、`RSV2/RSV3 set`、`bad opcode`），触发“重连成功后立即再次读错”的抖动循环，同时伴随高频 `ws.buffer.drop`，导致链路稳定性和数据完整性下降。需要在不引入过度架构改造的前提下，先完成止血级可靠性加固。

## What Changes

- 为 WS 重连增加退避抖动、最小重连间隔、单飞重连门控与 1008 冷静期，抑制重连风暴。
- 为订阅恢复增加幂等与短窗去重，避免重连后重复订阅触发限流。
- 优化连接复用策略：同 endpoint 合并订阅流，减少连接数与握手压力。
- 为 backfill 增加失败退避、重试上限与状态机反馈，避免无限补数抖动。
- 加入背压治理路径（队列水位、批量 sink、有限并发）并提供降级保护。
- 增加可观测指标与告警阈值，区分链路抖动、限流与消费瓶颈。

## Capabilities

### New Capabilities
- `worker-ws-resilience-hardening`: 为 worker 提供可控重连、订阅幂等、限流冷静期与连接复用策略，降低 WS 协议错误下的抖动。
- `worker-backfill-failure-guard`: 为 backfill 流程提供失败闭环（有限重试、退避、熔断/冷静期、状态回写），避免无限重试。
- `worker-ingestion-backpressure-control`: 为消息消费链路提供背压监测与削峰能力，减少 `ws.buffer.drop` 与假缺口。
- `worker-reliability-observability`: 为重连、丢包、补数耗尽、策略违规建立统一指标与告警。

### Modified Capabilities
- 无

## Impact

- 受影响代码：`DataPlatform/datainjector/worker` 下 websocket 连接管理、订阅恢复、backfill 状态机、消息消费与 sink 写入路径。
- 受影响运行面：spot 与 perp WS 连接拓扑、重连时序、告警配置与运维排障流程。
- 外部依赖：交易所 WS 限流策略、现有监控系统（指标与告警规则）。
