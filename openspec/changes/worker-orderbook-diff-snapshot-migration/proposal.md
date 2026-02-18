## Why

当前 worker 的 orderbook 链路在接入侧维护本地订单簿并输出重建后的簿状态，导致接入职责与研究职责耦合，且在 Binance 无 diff 区间补数接口的前提下，缺失恢复策略复杂且受限。需要将 worker 重构为“事件采集与恢复触发层”，统一输出 `diff + snapshot` 原始语义数据，把订单簿重建下沉到研究域。

## What Changes

- **BREAKING**：移除 worker 内本地订单簿维护与输出逻辑（`orderbook_diff`、`orderbook_validator` 及状态存储相关实现）。
- 新增 orderbook 双流输出：`*.orderbook.diff`（实时 diff）与 `*.orderbook.snapshot`（周期快照与缺失补快照）。
- 新增 `10s` 周期快照 role（spot/perp 各一条），通过 REST 拉取快照并下沉 Kafka。
- 调整完整性链路：保留 diff 缺失检测与 backfill 调度能力，缺失时触发 snapshot backfill 并旁路下沉 snapshot topic，不再依赖本地簿回调放行。
- 调整 AAVE 微观结构角色配置为新链路，不保留旧 topic 与旧 payload 兼容。

## Capabilities

### New Capabilities
- `worker-orderbook-diff-snapshot-pipeline`: worker 以 diff 主流 + snapshot 辅流方式采集、路由与下沉 orderbook 数据，并支持周期快照与缺失补快照统一输出。

### Modified Capabilities
- `worker-aave-kafka-microstructure`: AAVE orderbook 从“重建簿状态输出”改为“diff/snapshot 双 topic 输出”，并更新字段契约与 topic 约定。
- `worker-backfill-command-delivery`: 在 orderbook 场景明确 snapshot backfill 的旁路投递语义与可观测结果，确保缺失检测触发后可稳定投递 snapshot。
- `worker-orderbook-state-isolation`: 该能力从“worker 内订单簿作用域隔离”转为“不在 worker 内维护订单簿状态”，保留可观测丢弃/缺失信号但移除本地簿状态要求。

## Impact

- 代码模块：`internal/handler/orderbook_*`、`internal/resource/orderbook/*`、`internal/handler/integrity/*`、AAVE roles 配置。
- 数据契约：orderbook Kafka topic 与 payload 发生 BREAKING 变更；下游研究域需按 diff+snapshot 重建。
- 运维与观测：新增 snapshot topic 与缺失补快照观测指标，旧“本地簿重建成功”类观测项下线。
