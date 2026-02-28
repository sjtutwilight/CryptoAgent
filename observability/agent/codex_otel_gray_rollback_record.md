# Codex OTel 灰度与回滚演练记录

## 演练时间

- 2026-02-25

## 目标

1. 验证改造后编排配置可解析。
2. 验证闭环链路（分析 -> 审核 -> 回归）可执行。
3. 验证观测链路关闭时，不影响 Codex 主流程分析工具执行。

## 执行记录

### 1) 编排配置解析

命令：

```bash
set -a; source config/infrastructure/env/docker.env; set +a
cd automation/orchestration
docker compose config >/tmp/codex_otel_compose_resolved.yaml
```

结果：成功，生成 3390 行解析结果。

### 2) 闭环演练

命令：

```bash
python3 automation/test/tools/run_codex_otel_drill.py
```

结果：成功，生成路径：

- `runtime/data/codex_otel/drills/20260225-070445/summary.json`
- `runtime/data/codex_otel/snapshots/latest.json`
- `runtime/data/codex_otel/reports/report-20260225-070445.md`

演练输出包含：

- 建议生成（script/doc）
- 审核流转（proposed -> approved -> implemented）
- 回归评估（result=improved）

### 3) 在线联调与回滚验证（已完成）

#### 3.1 在线服务拉起

命令：

```bash
set -a; source config/infrastructure/env/docker.env; set +a
cd automation/orchestration
docker compose up -d loki tempo otel-collector codex-otel-metrics grafana prometheus promtail
```

结果：

- `obs-loki`、`obs-tempo`、`obs-otel-collector`、`obs-codex-otel-metrics`、`obs-grafana`、`obs-prometheus` 均为 `Up`。
- 健康检查通过：
  - `curl http://127.0.0.1:13133/` -> collector 可用
  - `curl http://127.0.0.1:3200/ready` -> tempo ready
  - `curl http://127.0.0.1:9470/metrics` -> codex 指标导出可读

#### 3.2 接入验收

命令：

```bash
./tool/codex_otel_validate.sh
./tool/codex_otel_validate.sh --simulate-exporter-failure
```

结果：

- 两轮均通过，关键事件 OTLP 接收返回 HTTP 200。
- 审计落盘成功：`runtime/data/otel-audit/codex-otel-events.jsonl` 有新增记录。
- 注：短时下游故障场景中 `otelcol_exporter_send_failed_log_records{exporter="otlphttp/loki"}` 未出现增长，说明当前 Collector 行为以重试/恢复后成功发送为主，未进入 failed 计数。

#### 3.3 Prometheus 抓取验证

命令：

```bash
curl -fsS 'http://127.0.0.1:9090/api/v1/targets?state=active'
```

结果（关键 job）：

- `otel-collector`: up
- `tempo`: up
- `codex-otel-metrics`: up

#### 3.4 在线回滚演练

命令：

```bash
cd automation/orchestration
docker compose stop otel-collector codex-otel-metrics tempo
./tool/ops.sh codex:otel analyze --input runtime/data/otel-audit/codex-otel-events.jsonl --window-hours 1
docker compose up -d tempo otel-collector codex-otel-metrics
```

结果：

- 停止观测链路期间，分析 CLI 仍可执行（`analyze_while_down_ok`）。
- 恢复后服务健康重新通过，说明关闭观测不影响 Codex 主流程分析能力。
- 最新联调演练产物：
  - `runtime/data/codex_otel/drills/20260225-072135/summary.json`

## 回滚策略

### 软回滚

- 关闭 Codex OTel 导出（移除或禁用 OTel 配置）。
- 保留 `runtime/data/codex_otel` 历史快照与建议审计。

### 硬回滚

```bash
cd automation/orchestration
docker compose stop otel-collector codex-otel-metrics tempo
```

## 结论

- 在线联调已完成，关键链路（Collector/Tempo/Prometheus/Grafana/Codex metrics）可用。
- 接入验收、故障注入、在线回滚演练均通过。
- 观测链路可独立启停，不阻塞 Codex 主流程分析工具执行。
