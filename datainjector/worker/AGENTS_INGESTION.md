# Worker Agent Ingestion

## Concepts
- datasource_id: 引用 datasources 注册表，自动注入 protocol/auth/rate_limit。
- template: 引用 role_templates（连接模板，固定 emitter+caller）。
- pipeline: 引用 pipeline_templates（处理链模板，定义 handlers/sink/queue/domain）。
- domain: 领域分类，如 cex.perp.orderbook。

## Config Files
- `configs/base.yaml`: 运行时加载的基础配置（datasources/templates/pipelines/metrics 等）。
- `configs/config.yaml`: 仅作为 role 注册中心（线下管理），不参与运行时加载。
- 运行时如需预加载 role：启动参数增加 `--roles /path/to/config.yaml`。

## domains
- cex.spot.kline
- cex.perp.orderbook
- cex.perp.trades
- cex.perp.mark_index
- cex.perp.funding_rate
- cex.perp.open_interest
- cex.perp.liquidations
- cex.perp.metrics
- chain.dex.transaction
- chain.balance.snapshot
- chain.blocks.integrity
- chain.blocks.raw
- metadata.kafka
- metadata.postgres
- metadata.clickhouse
- batch.file.pull

## datasources (ids)
- dune.sim (http, api_key_env=DUNE_SIM_API_KEY)
- bigquery.api (http, bearer_env=GOOGLE_CLOUD_API_KEY)
- binance.ws (websocket)
- binance.rest (http)
- hyperliquid.ws (websocket)
- hyperliquid.info (http)
- geckoterminal.api (http, api_key_env=GECKOTERMINAL_API_KEY)
- mockprovider.ws (websocket)
- mockprovider.http (http)

## role_templates
- http_paged_batch_to_files: kafka_command + batch_file + direct sink
- ws_stream_native_call: single + native_call (generic)
- binance_spot_ws_stream: binance spot websocket defaults
- binance_perp_ws_stream: binance perp websocket defaults
- hyperliquid_ws_stream: hyperliquid websocket defaults
- mockprovider_ws_stream: mockprovider websocket defaults
- http_poll_native_call: polling + native_call
- sdk_polling: polling + sdk_call
- kafka_command_native_call: kafka_command + native_call
- metadata_polling: polling (queue size default 2000)

## pipeline_templates (examples)
- perp_orderbook_pipeline: cex.perp.orderbook
- batch_file_pipeline: batch.file.pull
- binance_kline_pipeline: cex.spot.kline
- hyperliquid_perp_orderbook_pipeline: cex.perp.orderbook
- metadata_kafka_pipeline: metadata.kafka

## validate endpoint
POST /api/roles/validate
payload: {"roles":[...]}
result: {"status":"ok"} or {"status":"invalid","errors":[{role_id,errors}]}
checks: 必填字段（如 topic/symbol/protocol/endpoint）与结构校验。

## 开发规范
### emitter_config 配置规范
- **禁止硬编码 brokers 地址**：如 `kafka:29092`、`localhost:9092` 等容器或本地地址
- 代码会自动从环境变量获取 Kafka brokers 配置
- emitter_config 中只需配置 `topic` 和 `group_id`
- 示例：
  ```yaml
  emitter_config:
    # ❌ 错误：brokers: ["kafka:29092"]
    # ✅ 正确：不配置 brokers，由代码自动获取
    topic: "batch.tasks"
    group_id: "worker.example.group"
  ```

### sink 配置规范
- 优先使用 `type: "file"` 保存到本地文件，便于调试和数据留存
- 只有明确需要实时流式处理时才使用 `type: "kafka"`
- AAVE 微观结构（spot/perp orderbook + aggtrades）属于 Kafka-first 默认链路，使用 `configs/aave/roles_aave_full_stable.json`
- 启动方式：`--config configs/base.yaml --roles configs/aave/roles_aave_full_stable.json`
- 该场景下 file 录制仅作为调试辅助，不是默认接入路径
- 文件 sink 示例：
  ```yaml
  sink:
    type: "file"
    with:
      output_dir: "runtime/data/{exchange}/{market}/{datatype}/{symbol}"
      output_format: "json"
      filename_prefix: "{datatype}_{interval}"
      max_records_per_file: 10000
  ```
