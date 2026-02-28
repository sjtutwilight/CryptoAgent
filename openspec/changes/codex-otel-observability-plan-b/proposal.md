## Why

当前 Codex 在仓库内的执行行为缺少统一可观测数据，导致我们无法系统识别“高频可脚本化步骤”“低价值代码读取”“高失败决策路径”等提效机会。现在推进 OTel 接入，是为了把 Codex 行为纳入现有观测体系，并建立可持续的优化闭环，降低后续迭代的人力成本。

## What Changes

- 引入 Codex OTel 采集链路：将 Codex 结构化事件通过 OTLP 发送至 OTel Collector，再分发到 Loki/Tempo/Grafana。
- 建立 Codex 行为观测指标与看板：覆盖 session 生命周期、tool 决策与执行结果、patch 应用、错误分布、阶段耗时。
- 建立“决策链效率”分析模型：识别重复步骤、长链路无产出读取、工具失败热点，并形成可执行优化建议。
- 建立治理闭环：将分析结果沉淀为脚本化候选与文档约束候选，支持人工审核后落地。
- **BREAKING**：采用可观测优先策略，启用 `log_user_prompt=true` 采集原始提示词上下文，并取消关键字段脱敏流程。

## Capabilities

### New Capabilities
- `codex-otel-telemetry-pipeline`: 定义 Codex OTel 数据从采集、处理、存储到检索的端到端行为契约。
- `codex-decision-efficiency-analytics`: 定义 Codex 决策链效率指标、聚合口径与分析输出契约。
- `codex-optimization-feedback-loop`: 定义基于分析结果生成脚本/文档优化建议并闭环验收的流程契约。

### Modified Capabilities
- 无。

## Impact

- 受影响配置：Codex OTel 配置、`automation/orchestration/docker-compose.yml`（新增/接入 collector 与 trace backend）、`observability/provisioning/datasources/datasource.yml`（trace 数据源与跳转规则）。
- 受影响观测模块：`observability/` 下 Prometheus/Grafana/Loki 配置与看板定义。
- 运维影响：新增 OTel 数据处理链路与告警规则，需要定义采样、限流、访问控制、审计和保留策略。
- 组织影响：值班/研发流程将新增“Codex 行为巡检 -> 优化建议评审 -> 落地验证”步骤。
