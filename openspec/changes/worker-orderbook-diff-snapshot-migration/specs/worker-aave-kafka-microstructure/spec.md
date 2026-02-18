## MODIFIED Requirements

### Requirement: Worker publishes AAVE microstructure streams to Kafka
worker SHALL 将 AAVE 微观结构流写入 Kafka，覆盖 Binance spot 与 Binance perp；其中 orderbook MUST 以 diff/snapshot 双流输出，aggtrades 维持独立流输出。

#### Scenario: Perp orderbook diff stream is configured for Kafka sink
- **WHEN** 角色 `rec-binance-perp-aave-orderbook-diff` 被加载
- **THEN** 其 sink MUST 写入 `perp.orderbook.diff`

#### Scenario: Spot orderbook snapshot stream is configured for Kafka sink
- **WHEN** 角色 `rec-binance-spot-aave-orderbook-snapshot` 被加载
- **THEN** 其 sink MUST 写入 `spot.orderbook.snapshot`

#### Scenario: Spot aggtrades stream remains configured for Kafka sink
- **WHEN** 角色 `rec-binance-spot-aave-aggtrade-full` 被加载
- **THEN** 其 sink MUST 为 `kafka` 并写入配置的 aggtrades topic

### Requirement: Kafka messages preserve downstream-compatible contracts
worker SHALL 在 AAVE orderbook 链路输出“可重建订单簿”的契约：diff 与 snapshot 字段完整、语义清晰，且可区分周期快照与补数快照来源。

#### Scenario: Orderbook diff payload preserves required fields
- **WHEN** handler 链路发出 orderbook diff 消息
- **THEN** 每条消息 MUST 包含 `symbol`、`exchange`、`first_update_id`、`final_update_id`、`prev_final_update_id`、`exchange_ts`、`ingest_ts`

#### Scenario: Orderbook snapshot payload preserves required fields
- **WHEN** handler 链路发出 orderbook snapshot 消息
- **THEN** 每条消息 MUST 包含 `symbol`、`exchange`、`lastUpdateId`、`snapshot`、`snapshot_source`、`exchange_ts`、`ingest_ts`

### Requirement: Kafka partitioning keys are deterministic for AAVE streams
worker SHALL 支持 AAVE 微观结构消息的确定性 key 生成，以保持同标的消息顺序稳定。

#### Scenario: Key is derived from symbol and exchange metadata
- **WHEN** Kafka sink 写入 AAVE orderbook diff、orderbook snapshot 或 aggtrades 消息
- **THEN** 消息 key MUST 由配置 metadata 字段推导，且包含 `symbol` 与 `exchange`
