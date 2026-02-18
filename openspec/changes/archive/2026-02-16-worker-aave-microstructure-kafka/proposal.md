## Why

当前 AAVE 研究链路在 `worker` 侧默认依赖 file sink 录制与离线转换，导致控制面/数据面边界被打破，且无法直接服务实时下游消费。需要将微观结构流在 worker 内直接写入 Kafka，使接入链路符合生产规范并降低链路延迟。

## What Changes

- 将 AAVE 微观结构角色（Binance spot/perp 的 orderbook 与 aggtrades）从 `sink.type=file` 切换为 `sink.type=kafka`。
- 统一该场景下的 worker 配置方式：运行时全局配置使用 `configs/base.yaml`，角色由 `--roles` 注入，不再以临时 file 录制作为默认链路。
- 约定 AAVE orderbook/aggtrades 的 topic 与分区 key 规则，保持与现有实时消费侧契约一致。
- 增加 worker 侧验证与发布流程约束（`validate/resolve/apply`）。
- **BREAKING**: worker 中原有“file sink -> 转换脚本”的 AAVE 默认研究路径不再作为默认接入路径。

## Capabilities

### New Capabilities
- `worker-aave-kafka-microstructure`: worker 可以将 AAVE 微观结构（orderbook/aggtrades）规范化后直接写入 Kafka，并通过治理化角色配置发布。

### Modified Capabilities
- None.

## Impact

- 受影响代码与配置：
  - `datainjector/worker/configs/aave/roles_aave_full_stable.json`
  - `datainjector/worker/configs/config.yaml`（角色注册治理约定）
  - `datainjector/worker/internal/sink/kafka.go`
  - `datainjector/worker/internal/handler/binance_handlers.go`
  - worker 接入文档
- 受影响系统：
  - AAVE 微观结构 Kafka topic 与消费组
  - datainjector worker 控制面发布流程
- 不在范围内：
  - 任何 dbt 变更
  - strategy_engine 特征工程或训练链路变更
