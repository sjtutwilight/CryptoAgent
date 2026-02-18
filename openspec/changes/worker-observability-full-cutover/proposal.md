## Why

当前 Worker 可观测存在“链路有告警但无数据、看板与规则脱节、日志检索粒度不足”的系统性缺口，已经直接影响故障定位与回归验证效率。现在需要一次性完成可观测体系切换，建立以 backfill 会话闭环与阶段漏斗为核心的观测模型，并移除旧指标残留，避免双轨语义长期并存。

## What Changes

- 将 Prometheus 的 Worker 抓取目标改为容器内可达地址，消除 `up{job="worker"}` 失真。
- 以新闭环指标重建告警体系：backfill session、pending 时长、result 分类、dedup 异常、pipeline 产出率、WS drop。
- 重构 Worker 可观测 dashboard：接入 `backfill_sessions_inflight`、`backfill_schedule_dedup_total`、`backfill_result_total`、`backfill_pending_duration_seconds`、`backfill_enqueue_latency_seconds`、`backfill_compensation_backlog`、`worker_task_stage_total`、`worker_websocket_drops_total`。
- 重构 Logs dashboard 与 Promtail 提取规则，支持按 `event/role_id/error_class/session_key/cmd_id` 快速检索。
- **BREAKING**：不保留旧可观测兼容层，迁移后删除对旧指标（如 `worker_integrity_backfills_total`）及旧面板/旧规则的依赖。

## Capabilities

### New Capabilities
- `worker-observability-cutover`: 定义 Worker 可观测全量迁移与旧指标清理的统一行为契约。

### Modified Capabilities
- `worker-reliability-observability`: 可靠性告警与观测指标改为基于 backfill 结果闭环和 pipeline 产出率，不再依赖旧 backfill 计数指标。
- `worker-integrity-structured-events`: 增强结构化事件字段约束与日志可检索性，要求与新告警/看板维度一致。

## Impact

- 受影响代码：`datainjector/worker/internal/observability/metrics`、`datainjector/worker/internal/handler/integrity`、`datainjector/worker/internal/role`、`datainjector/worker/internal/protocol`。
- 受影响观测配置：`observability/prometheus/prometheus.yml`、`observability/prometheus/rules/alerts.yml`、`observability/provisioning/dashboards/worker-observability-dashboard.json`、`observability/provisioning/dashboards/logs-dashboard.json`、`observability/promtail/config.yml`。
- 运行影响：告警表达式与看板查询将切换到新指标；历史基于旧指标的图表和规则将下线。
- 交付影响：需要一次迁移窗口完成规则、看板、日志标签与指标打点的协同发布。
