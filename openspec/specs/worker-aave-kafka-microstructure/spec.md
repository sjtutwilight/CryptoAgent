# worker-aave-kafka-microstructure Specification

## Purpose
TBD - created by archiving change worker-aave-microstructure-kafka. Update Purpose after archive.
## Requirements
### Requirement: Worker publishes AAVE microstructure streams to Kafka
worker SHALL 将 AAVE 的规范化 orderbook 与 aggtrades 流写入 Kafka，覆盖 Binance spot 与 Binance perp 两类角色。

#### Scenario: Perp orderbook stream is configured for Kafka sink
- **WHEN** 角色 `rec-binance-perp-aave-orderbook-full` 被加载
- **THEN** 其 sink type MUST 为 `kafka`，并将 orderbook payload 写入配置的 Kafka topic

#### Scenario: Spot aggtrades stream is configured for Kafka sink
- **WHEN** 角色 `rec-binance-spot-aave-aggtrade-full` 被加载
- **THEN** 其 sink type MUST 为 `kafka`，并将 aggtrades payload 写入配置的 Kafka topic

### Requirement: Worker enforces governed role configuration path
worker SHALL 支持通过静态角色注册文件与控制 API 校验进行治理化发布，不再要求“临时 file 录制流程”作为默认路径。

#### Scenario: Role payload passes validation before apply
- **WHEN** 运维提交 AAVE 角色定义到 `/api/roles/validate`
- **THEN** 仅当 sink 与 handler 必填项完整时，worker MUST 返回 `status=ok`

#### Scenario: Role rollout uses registry-based startup
- **WHEN** worker 以 `--config` 与 `--roles` 参数启动
- **THEN** AAVE 微观结构角色 MUST 从指定角色注册文件加载并启动，且不依赖临时录制转换步骤

### Requirement: Kafka messages preserve downstream-compatible contracts
worker SHALL 在 sink 从 file 迁移到 Kafka 后继续保持 orderbook 与 aggtrades 的下游兼容字段契约。

#### Scenario: Orderbook payload preserves required fields
- **WHEN** handler 链路发出 orderbook diff 消息
- **THEN** 每条消息 MUST 包含 `symbol`、`exchange`、`depth`、`seq`、`snapshot`、`exchange_ts`、`ingest_ts`

#### Scenario: Aggtrades payload preserves required fields
- **WHEN** handler 链路发出 aggtrades 消息
- **THEN** 每条消息 MUST 包含 `symbol`、`exchange`、`price`、`size`、`side`、`buyer_maker`、`exchange_ts`、`ingest_ts`、`trade_id`

### Requirement: Kafka partitioning keys are deterministic for AAVE streams
worker SHALL 支持 AAVE 微观结构消息的确定性 key 生成，以保持同标的消息顺序稳定。

#### Scenario: Key is derived from symbol and exchange metadata
- **WHEN** Kafka sink 写入 AAVE orderbook 或 aggtrades 消息
- **THEN** 消息 key MUST 由配置的 metadata 字段推导，且包含 `symbol` 与 `exchange`

