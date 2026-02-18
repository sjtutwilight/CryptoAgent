## Why

当前订单簿状态以 `symbol` 作为全局 key 共享，未按 `role/market` 隔离，导致 spot/perp 在同符号场景下发生状态串扰。该问题已在 AAVE 微观结构链路中表现为 `spot.orderbook` 静默停产但 role 仍在线，属于高优先级数据正确性风险，需要立即修复。

## What Changes

- 将订单簿状态存储从“按 symbol 全局共享”调整为“按 role + market(或 exchange+stream) + symbol 隔离”。
- 为 `orderbook_diff`/`integrity` 链路引入稳定的一致性隔离键，确保不同 role 不会读写同一 `BookState`。
- 增加可观测性：当 diff 被丢弃（如 stale/sequence 不匹配）时输出可聚合指标与结构化日志，避免静默故障。
- 增加回归测试，覆盖 `spot/perp` 同符号并发运行时的隔离行为与 topic 持续产出。

## Capabilities

### New Capabilities
- `worker-orderbook-state-isolation`: 订单簿状态按 role/market 作用域隔离，防止跨链路状态污染并保证各自独立产出。

### Modified Capabilities
- `worker-role-atomic-reconcile`: 在 role 重建与替换过程中保持隔离状态一致性，避免历史全局状态在新 role 启动时被复用。

## Impact

- Affected code:
  - `datainjector/worker/internal/resource/orderbook/store.go`
  - `datainjector/worker/internal/resource/orderbook/engine.go`
  - `datainjector/worker/internal/handler/orderbook_handlers.go`
  - `datainjector/worker/internal/handler/integrity/*`
- Affected runtime behavior:
  - `spot.orderbook` 与 `perp.orderbook` 不再共享订单簿内存状态。
  - backfill/snapshot 只影响当前 role 的本地状态空间。
- Observability:
  - 需要新增“diff dropped/stale/sequence mismatch”计数与 role 维度日志字段。
- Testing:
  - 新增并发 role 隔离回归测试与端到端 topic 产出校验。
