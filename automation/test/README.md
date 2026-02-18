# Automation Test Framework

该目录是 Scenario/Stage/Probe 测试框架的实现，设计说明见 `DESIGN.md`。

## Layout

- `scenarios/`: 场景定义（binance_kline、binance_perp、binance_spot_link_kline_batch、hyperliquid_perp、spark_token_holders、geckoterminal_link_liquidity、datainjector_fault_regression）。
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

## DataInjector Fault Regression

场景名：`datainjector_fault_regression`，用于“故障注入 + 日志核验”一键回归，输出按 role 聚合的 PASS/FAIL、缺失事件和证据。

示例（mock 冒烟）：

```bash
./tool/test.sh scenario:run datainjector_fault_regression \
  --config-file automation/test/configs/datainjector_fault_mock_smoke.json
```

示例（real / 指定 role 重启注入）：

```bash
./tool/test.sh scenario:run datainjector_fault_regression \
  --config-file automation/test/configs/datainjector_fault_real_role_restart.json
```

常用参数（可通过 `--config-json` 覆盖）：
- `role_ids`: 目标 role 列表（必填）
- `role_config_yaml`: 角色配置文件（支持 JSON/YAML）
- `fault_mode`: `mock` 或 `real`
- `fault_action`: `role_restart` / `container_pause` / `noop`
- `expect_backfill`: 是否要求 `integrity.backfill.*` 事件
- `apply_roles_before_inject`: 是否在注入前自动 apply role
- `cleanup_stop_roles`: 结束后是否停止本次 apply 的 role
- `require_mock_provider`: mock 模式下是否要求 `mock_provider_base_url` 必须可达（默认 `false`，不可达时仅 `SKIP`）

判定口径（默认）：
- 必需事件：`ws.reconnect.start`、`ws.reconnect.success`、`caller.response`、`pipeline.finish`
- 补数场景额外必需：`integrity.backfill.trigger`、`integrity.backfill.success`
- 失败事件：`handler.error`、`sink.error`、`pipeline.error`、`caller.error`、`integrity.backfill.exhausted`、`integrity.backfill.enqueue.error`
- 若结构化事件缺失，判定器会对 backfill 关键事件走文本兜底，并在结果中记录 fallback 命中

运行产物：
- `automation/test/runs/<run_id>/summary.json`
- `automation/test/runs/<run_id>/summary.txt`
- `automation/test/runs/<run_id>/evidence.jsonl`
- `automation/test/runs/<run_id>/fault_regression_summary.json`

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
