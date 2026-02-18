# Worker 可观测全量切换发布说明（2026-02-18）

## 1. 目标

本次发布将 Worker 可观测统一迁移到新闭环语义：
- backfill 会话指标：`worker_integrity_backfill_sessions_inflight`
- backfill 结果指标：`worker_integrity_backfill_result_total`
- backfill pending 指标：`worker_integrity_backfill_pending_duration_seconds`
- backfill 去重指标：`worker_integrity_backfill_schedule_dedup_total`
- 漏斗指标：`worker_task_stage_total`
- WS 丢弃指标：`worker_websocket_drops_total`

同时下线旧指标依赖：`worker_integrity_backfills_total`。

## 2. 发布前检查

1. 确认 `config/infrastructure/env/docker.env` 中抓取目标：
- `PROMETHEUS_WORKER_TARGET=worker-app:9100`
- `PROMETHEUS_BACKEND_TARGET=host.docker.internal:8088`（可按环境覆盖）

2. 执行静态门禁：
```bash
datainjector/worker/tools/worker_observability_cutover_gate.sh
```

3. 启动观测栈后执行运行时门禁（压测/回放环境）：
```bash
RUN_RUNTIME_CHECKS=1 \
PROM_URL=http://localhost:9090 \
LOKI_URL=http://localhost:3100 \
datainjector/worker/tools/worker_observability_cutover_gate.sh
```

## 3. 迁移步骤

1. 发布 Worker 新版本（包含新指标接线、旧指标移除）。
2. 部署 Prometheus 规则与抓取配置（worker 默认容器地址）。
3. 部署 Grafana 新 dashboards：
- `observability/provisioning/dashboards/worker-observability-dashboard.json`
- `observability/provisioning/dashboards/logs-dashboard.json`
4. 部署 Promtail 新解析配置：`observability/promtail/config.yml`。
5. 在压测/回放场景验证：
- 能看到 backfill 会话全生命周期（pending/inflight/result/dedup/enqueue/backlog）
- 能看到 caller -> pipeline -> sink 漏斗变化
- 告警中包含 `root_cause_class` 与 `error_class`

## 4. 旧残留清理门禁

执行以下检查，任何命中都不允许放行：
```bash
rg -n "worker_integrity_backfills_total" \
  datainjector/worker/internal \
  observability/prometheus/rules/alerts.yml \
  observability/provisioning/dashboards/worker-observability-dashboard.json \
  observability/agent/README.md
```

## 5. 回滚策略（仅回滚新语义配置版本）

不回滚到旧指标语义，不恢复旧看板/旧规则。

回滚顺序：
1. 回滚 Prometheus 规则与 dashboards 到“上一版新语义配置”。
2. 回滚 Promtail 到上一版新语义解析（保留 event/role_id/error_class 维度）。
3. 回滚 Worker 到上一版仍输出新语义指标的版本。

## 6. 值班排障建议

1. 先看 `Worker 抓取状态` 与 `Backfill Sessions Inflight`。
2. 再看 `Backfill Result 分布` 与 `Caller->Sink 产出率`。
3. 最后用 Logs dashboard 按 `event/role_id/error_class/session_key/cmd_id` 关联会话链路。
