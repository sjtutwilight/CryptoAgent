# Automation Test Framework

该目录是 Scenario/Stage/Probe 测试框架的实现，设计说明见 `DESIGN.md`。

## Layout

- `scenarios/`: 场景定义（binance_kline、binance_perp、binance_spot_link_kline_batch、hyperliquid_perp、spark_token_holders、geckoterminal_link_liquidity）。
- `shared/core/`: 核心协议与 Stage 抽象（RunContext/ProbeResult/Stage/Scenario）。
- `shared/ingress/`: DataInjector ingress 与 role 构建。
- `shared/process/`: Flink/Spark 处理与验证。
- `shared/infra/`: HTTP/Docker/ClickHouse/JAR 构建等基础能力。
- `shared/*.py`: logging、config_loader、run_id、run_summary、run_artifacts、repo_utils、run_repo。
- `probes/`: infra/kafka/db/http/dq/spark 探针实现。
- `tools/`: `run_scenario.py`（场景运行器，供 `tool/test.sh` 调用）、`probe_cli.py`（探针 CLI）。
- `runs/`: 场景执行产物目录。

## Run Scenarios

```bash
./tool/test.sh list
./tool/test.sh scenario:run binance_kline
./tool/test.sh scenario:run binance_kline --stages=ingress,process
./tool/test.sh scenario:run binance_kline --tags type:e2e --config-json '{"build_jar": true}'
```

## Probe CLI

```bash
python3 automation/test/tools/probe_cli.py infra stack
python3 automation/test/tools/probe_cli.py kafka topic-check --topic binance.kline
```

## Config

- 默认加载 `config/infrastructure/env/docker.env`，可用 `INFRA_ENV_FILE` 覆盖。
- `--config-json` / `--config-file` 的 JSON 会合并到 `RunContext.metadata`。
- `automation/test/shared/core/config.py` 里提供默认键值：
  - ClickHouse：`clickhouse_http` / `clickhouse_user` / `clickhouse_password`
  - Kafka：`kafka_broker` / `kafka_wait_timeout` / `kafka_wait_interval` / `kafka_max_messages`
  - Flink：`flink_rest`
  - DataInjector：`datainjector_api` / `datainjector_container`
  - Build/Cleanup：`build_jar` / `jar_path` / `clean_clickhouse` / `skip_clean_clickhouse` / `cancel_job` / `keep_job`

## Artifacts

- `automation/test/runs/<run_id>/probe_events.jsonl`
- `automation/test/runs/<run_id>/summary.json`
- `automation/test/runs/<run_id>/timeline.json`

## Notes

- `http_probe.py` / `dq_probe.py` 仍是占位实现，按场景补齐真实逻辑。
