# Worker 可观测全量切换验证报告（2026-02-18）

## 验证环境

- 本地观测栈：Prometheus + Grafana + Loki + Promtail
- Worker 指标与结构化日志回放：`mock-worker`（容器内 `mock_worker_observability_server.py`）
- Prometheus worker 抓取目标验证：`mock-worker:9100`

## 1. 采集拓扑验证（任务 1.3）

- 验证命令：
```bash
curl -fsS --get 'http://localhost:9090/api/v1/query' \
  --data-urlencode 'query=min_over_time(up{job="worker",instance="mock-worker:9100"}[2m])'
```
- 结果：`1`
- 结论：`up{job="worker"}` 在验证窗口内连续稳定为 1。

## 2. 指标与规则验证（任务 2.4 / 3.4）

1. 指标存在性测试
- 命令：`go test ./internal/observability/metrics -count=1`（目录：`datainjector/worker`）
- 结果：PASS

2. Prometheus 规则语法
- 命令：`docker run --rm --entrypoint promtool -v "$PWD/observability/prometheus/rules:/rules:ro" prom/prometheus:latest check rules /rules/alerts.yml`
- 结果：`SUCCESS: 33 rules found`

## 3. Dashboard 验证（任务 4.4）

1. Grafana provisioning 自动加载
- `worker-observability` -> `Worker 可观测闭环`
- `logs-observability` -> `Worker 日志关联检索`

2. Worker dashboard 查询非空
- 对 13 条 PromQL 逐条执行（变量替换为 `.+`）
- 结果：`13/13` 均返回非空数据

## 4. Logs 检索验证（任务 5.3）

- 验证查询：
```bash
sum(count_over_time({service="datainjector-worker",event="integrity.backfill.result",role_id="role-a",error_class=~"none|timeout"}[5m]))
```
- 结果：非空（示例值 `126`）
- 结论：可按 `event/role_id/error_class` 快速过滤。

## 5. E2E 观测验证（任务 6.1）

- 连续 30 秒采样结果：
  - `backfill_result_total`: `4607 -> 5117`
  - `caller_accepted_total`: `8130 -> 9030`
  - `final_succeeded_total`: `4878 -> 5418`
  - `sessions_inflight`: `0 -> 1`
- 结论：可观测 backfill 会话闭环与 `caller -> pipeline -> sink` 漏斗变化。

## 6. 告警载荷验证（任务 6.2）

- Prometheus `/api/v1/alerts` 结果（worker 相关）：
  - `WorkerWSReconnectBurst`（firing）`root_cause_class=link_jitter` `error_class=reconnect_burst`
  - `WorkerWSBufferDropHigh`（firing）`root_cause_class=backpressure` `error_class=ws_drop_burst`
  - `WorkerWSPolicyViolationBurst`（firing）`root_cause_class=rate_limit` `error_class=policy_1008`
- 结论：关键告警可触发，并携带 `root_cause_class` 与 `error_class` 字段。

## 7. 旧残留清理门禁（任务 6.3）

- 执行：`datainjector/worker/tools/worker_observability_cutover_gate.sh`
- 结果：PASS（旧指标残留扫描、规则/看板/配置语法均通过）
