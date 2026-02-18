## Context

当前 Worker 可观测链路存在四类割裂：

1. 采集面：Prometheus 的 worker target 指向宿主机地址，容器部署下 `up{job="worker"}` 不可靠。
2. 指标面：新 backfill 闭环指标已定义并部分接线，但 dashboard/alerts 仍主要依赖旧指标，出现“规则存在但无数据”的盲区。
3. 日志面：结构化事件字典已完善，但 Loki 查询与 Promtail 标签提取仍偏容器通用，缺少 Worker 专项字段检索。
4. 诊断面：缺少 `caller -> pipeline -> sink` 阶段漏斗指标与告警，难以快速识别“上游有数据但落库稀疏”的退化。

本次变更采用一次性切换策略：统一迁移到新指标语义与新日志检索模型，明确删除旧可观测残留，不保留双轨兼容。

## Goals / Non-Goals

**Goals:**
- 建立 Worker 统一可观测闭环：采集、指标、告警、看板、日志检索一致。
- 全量切换到 backfill session/result/pending/dedup 等新指标，移除旧指标依赖。
- 建立漏斗化诊断能力，支持按 role 快速定位数据产出断层。
- 将结构化日志字段与可视化筛选字段对齐，提升值班排障速度。

**Non-Goals:**
- 不在本变更中调优业务策略阈值（如 backfill timeout/cooldown 参数）。
- 不在本变更中改造非 Worker 服务（Flink、Backend、ClickHouse）指标语义。
- 不提供旧 dashboard/旧告警规则并行保留。

## Decisions

### Decision 1: 采集目标采用容器内可达地址，并以环境化配置管理
- 决策：Prometheus `job=worker` 改为容器网络内地址（如 `worker-app:9100`），不再默认 `host.docker.internal:9100`。
- 原因：当前部署是容器内 worker，宿主机地址导致 target down 与告警失真。
- 备选方案：
  - 继续使用 `host.docker.internal`：仅适用于宿主机进程模式，不满足当前主路径。
  - 使用 file_sd 自动发现：实现复杂度更高，当前单实例 worker 不需要。

### Decision 2: 告警与看板统一迁移到新 backfill 闭环指标
- 决策：将 `worker_integrity_backfills_total` 从规则和看板中移除，统一使用 `worker_integrity_backfill_result_total`、`worker_integrity_backfill_sessions_inflight`、`worker_integrity_backfill_pending_duration_seconds`、`worker_integrity_backfill_schedule_dedup_total`。
- 原因：旧指标存在接线缺失风险，无法支撑可靠告警。
- 备选方案：
  - 保留双轨：增加维护成本，长期语义冲突。
  - 仅修旧指标打点：无法覆盖 session 与 pending 维度。

### Decision 3: 建立阶段漏斗 SLI 并作为核心退化告警
- 决策：新增漏斗观测与告警：`caller_nonzero -> pipeline_finish -> sink_success`，用产出率检测稀疏落库。
- 原因：当前主要问题不在“服务是否活着”，而在“消息是否真正产出”。
- 备选方案：
  - 仅靠 error rate：对静默退化不敏感。
  - 仅靠 gap/backfill 指标：缺少端到端产出视角。

### Decision 4: 日志检索改为 Worker 专项标签与字段双层模型
- 决策：Promtail 对 worker JSON 日志提取 `event/role_id/backfill_type/error_class` 为标签；`session_key/cmd_id/trace_id` 保留字段但不做标签。
- 原因：兼顾高效过滤与标签基数控制。
- 备选方案：
  - 全字段标签化：高基数导致 Loki 成本与查询抖动。
  - 仅 container 标签：无法支撑 role/session 级排障。

### Decision 5: 回滚仅回滚配置版本，不回滚旧指标语义
- 决策：若发布异常，回滚至“上一版新语义配置”，不恢复旧 dashboard/旧告警语义。
- 原因：避免再次引入双轨兼容和语义分裂。
- 备选方案：回滚到旧语义：会重新触发“规则有、数据少”的历史问题。

## Risks / Trade-offs

- [风险] 切换窗口内告警表达式变化可能造成短时噪声上升  
  → Mitigation: 先上线 recording rules，再切换 alert 查询，最后切 dashboard。

- [风险] 历史面板引用旧指标下线后，值班同学短期不适应  
  → Mitigation: 发布前提供“旧面板 -> 新面板”的映射表与 runbook。

- [风险] 新日志标签若提取不当可能提高 Loki 标签基数  
  → Mitigation: 严格限制标签集合，session/cmd 仅作字段检索。

- [风险] 漏斗指标接线不完整导致告警漏报  
  → Mitigation: 在 CI 增加 `/metrics` 指标存在性验证与 promtool 规则检查。

## Migration Plan

1. 更新 Prometheus scrape 配置（worker 目标改为容器地址），验证 `up{job="worker"}=1`。
2. 上线 worker 指标接线补全与 recording rules。
3. 切换 alerts 到新闭环指标，移除旧 backfill 计数规则。
4. 发布新版 worker dashboard 与 logs dashboard。
5. 发布 Promtail worker JSON 提取规则。
6. 执行验收：
   - backfill/result/pending/dedup 面板有数据；
   - pipeline 产出率可按 role 下钻；
   - logs 可按 `event/role_id/error_class` 检索；
   - 旧指标不再被任何规则/看板引用。

## Open Questions

- 是否在本次迁移中同步引入 Alertmanager 抑制规则（减少切换窗口内重复告警）？
- 多实例 worker 部署是否需要把 scrape 改为服务发现而非静态 targets？
- 是否将 worker 观测 runbook 作为发布门禁（未更新不允许合并）？
