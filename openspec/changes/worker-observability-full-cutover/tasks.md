## 1. Scrape Topology Cutover

- [x] 1.1 将 `observability/prometheus/prometheus.yml` 中 `job=worker` 抓取目标改为容器网络可达地址（移除默认 `host.docker.internal:9100`）
- [x] 1.2 为 worker/backend 抓取地址增加环境化配置入口，避免手工注释切换
- [x] 1.3 在本地观测栈验证 `up{job="worker"}` 连续稳定为 1

## 2. Worker Metrics Wiring And Cleanup

- [x] 2.1 补齐 `caller -> pipeline -> sink` 漏斗指标接线，确保可按 `role_id` 计算产出率
- [x] 2.2 补齐 backfill 会话闭环指标接线（result/pending/sessions/dedup/enqueue/backlog）
- [x] 2.3 清理旧 backfill 指标残留：删除或停止导出 `worker_integrity_backfills_total` 相关代码路径
- [x] 2.4 增加指标存在性测试：关键指标在 `/metrics` 中可被采集

## 3. Alert Rules Full Migration

- [x] 3.1 重写 `observability/prometheus/rules/alerts.yml` 的 Worker 告警，统一切到新闭环指标
- [x] 3.2 新增告警：session pending 过长、session inflight 卡住、result fail/timeout 激增、dedup 异常、pipeline yield 下降
- [x] 3.3 删除旧指标依赖告警规则（尤其是 `worker_integrity_backfills_total`）
- [x] 3.4 使用 `promtool check rules` 校验规则语法与表达式可解析

## 4. Dashboard Full Migration

- [x] 4.1 重构 `worker-observability-dashboard.json`：接入 backfill 闭环、漏斗产出率、task stage、ws drops
- [x] 4.2 从 dashboard 中移除旧指标查询与旧面板
- [x] 4.3 重构 `logs-dashboard.json`：增加 `event/role_id/error_class/session_key/cmd_id` 维度检索与关联视图
- [x] 4.4 校验 Grafana provisioning 自动加载新 dashboard 且查询无空指标

## 5. Promtail Worker Structured Parsing

- [x] 5.1 在 `observability/promtail/config.yml` 为 `datainjector-worker` 增加 JSON 结构化解析与 Worker 专项标签提取
- [x] 5.2 控制 Loki 标签基数：仅保留低基数字段为标签，`session_key/cmd_id` 保留为日志字段
- [x] 5.3 验证 Worker 日志可按 `event`、`role_id`、`error_class` 快速过滤

## 6. E2E Verification And Legacy Removal Gate

- [x] 6.1 在压测/回放场景执行端到端验证：能观测 backfill 会话全生命周期与漏斗产出变化
- [x] 6.2 验证关键告警可触发并具备 `root_cause_class` 与 `error_class`
- [x] 6.3 执行“旧残留清理门禁”：确认规则、看板、查询、代码均不再引用旧指标
- [x] 6.4 形成迁移发布说明与回滚操作说明（回滚仅限新语义配置版本）
