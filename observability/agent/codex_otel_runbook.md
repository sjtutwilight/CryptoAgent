# Codex OTel 运行手册（Runbook）

## 1. 目标

本手册用于指导 Codex OTel 链路的启停、巡检、告警分级、止损与回滚。

## 2. 启动步骤

1. 启动观测组件：

```bash
cd automation/orchestration
docker compose up -d loki tempo otel-collector codex-otel-metrics grafana prometheus
```

2. 检查 OTel Collector 健康：

```bash
curl -fsS http://127.0.0.1:${OTEL_COLLECTOR_HEALTH_PORT:-13133}/
```

3. 执行接入验收：

```bash
./tool/codex_otel_validate.sh
```

4. 发送分析任务（示例）：

```bash
python3 automation/ops/codex/otel_analytics.py analyze \
  --input runtime/data/otel-audit/codex-otel-events.jsonl \
  --window-hours 24
```

## 3. 回滚步骤

### 3.1 软回滚（保留基础组件）

- 将 Codex 配置中的 OTel 导出关闭或指向空端点。
- 保留 Grafana/Prometheus/Loki/Tempo，不删除历史数据。

### 3.2 硬回滚（下线链路）

```bash
cd automation/orchestration
docker compose stop otel-collector codex-otel-metrics tempo
```

## 4. 告警分级

- `critical`
  - `CodexOTLPExportFailed`
  - 连续导出失败且影响主链路可观测性
- `warning`
  - `CodexToolErrorBurst`
  - `CodexSessionNoOutputTooLong`

## 5. 止损动作

1. 先保证可接收：确认 Collector 健康和端口可达。
2. 再保证可导出：检查 Loki/Tempo 容器状态与网络。
3. 若导出持续失败：
   - 临时保留本地审计落盘（`runtime/data/otel-audit`）
   - 暂停高频分析任务，避免噪声放大
4. 若会话无产出告警持续：
   - 优先查看 `codex_low_value_read_ratio` 与 `codex_tool_error_total`
   - 落地脚本候选或文档候选后再评估

## 6. 常见问题排查

### 6.1 验收脚本失败

- 检查 `otel-collector` 是否启动。
- 检查端口是否被占用（4318/13133/8888）。

### 6.2 Grafana 看不到 Trace

- 检查 Tempo datasource URL：`http://obs-tempo:3200`
- 检查 Loki derived fields 是否设置 `datasourceUid: tempo`

### 6.3 告警不触发

- 检查 Prometheus 是否抓到 `otel-collector:8888` 与 `codex-otel-metrics:9470`
- 检查 `runtime/data/codex_otel/snapshots/latest.json` 是否持续更新

## 7. 审核与闭环

- 建议审核：

```bash
python3 automation/ops/codex/otel_analytics.py review \
  --id <SUGGESTION_ID> --to approved --actor <your_name> --reason "证据充分"
```

- 实施后回归：

```bash
python3 automation/ops/codex/otel_analytics.py regress \
  --id <SUGGESTION_ID> \
  --before <before_snapshot.json> \
  --after <after_snapshot.json> \
  --actor <your_name> \
  --reason "完成实施后评估"
```
