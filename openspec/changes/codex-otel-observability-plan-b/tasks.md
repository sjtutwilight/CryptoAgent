## 1. OTel 管道与基础设施接入

- [x] 1.1 在 `automation/orchestration/docker-compose.yml` 新增 `otel-collector` 与 `tempo` 服务，并接入现有 `infra` 网络
- [x] 1.2 新建 `observability/otel-collector/config.yaml`，配置 OTLP receiver、批处理 processor、Loki/Tempo exporter
- [x] 1.3 更新 `observability/provisioning/datasources/datasource.yml`，补齐 Tempo 数据源与日志到 trace 的跳转规则
- [x] 1.4 新增/更新 `config/infrastructure/env/docker.env` 所需环境变量（OTLP endpoint、采样、保留策略）

## 2. Codex OTel 配置与可观测优先基线

- [x] 2.1 新增 Codex OTel 配置模板（`[otel]`、`[otel.exporter.otlp-http]`、`[otel.trace_exporter.otlp-http]`）并默认 `log_user_prompt=true`
- [x] 2.2 在 Collector 与存储侧实现访问控制、审计与保留策略，不做关键字段内容级脱敏
- [x] 2.3 编写接入验收脚本（或命令清单），验证关键事件可达、导出失败不阻塞主流程
- [x] 2.4 增加运行指引文档，明确启停、回滚与故障排查步骤

## 3. 决策链效率分析实现

- [x] 3.1 定义 Codex 事件到分析指标的映射口径（重复步骤、低效读取、失败热点、首次产出时延）
- [x] 3.2 实现每日分析任务脚本，按时间窗口读取观测数据并产出指标快照
- [x] 3.3 生成脚本候选与文档候选报告（包含频次、耗时、样本证据）
- [x] 3.4 为分析任务增加失败重试与结果落盘机制（`runtime/data` 下可追溯）

## 4. 优化建议闭环与审核机制

- [x] 4.1 定义建议实体与状态流转（`proposed`/`approved`/`rejected`/`implemented`）
- [x] 4.2 实现建议审核入口（CLI 或文档流程）并记录审计轨迹（操作者、时间、原因）
- [x] 4.3 实现实施后回归评估任务，输出 `improved|neutral|regressed` 结论
- [x] 4.4 对 `regressed` 结果自动生成复盘任务模板

## 5. 看板、告警与值班可用性

- [x] 5.1 新增 Codex 行为观测 Grafana Dashboard（会话、tool 成功率、错误分布、效率趋势）
- [x] 5.2 新增关键告警规则（OTLP 导出异常、tool_error 突增、长时无产出会话）
- [x] 5.3 配置日志/链路联动跳转并验证值班路径（日志 -> trace -> 日志）
- [x] 5.4 编写 Codex 观测 Runbook（巡检、告警分级、止损动作）

## 6. 验证与发布

- [x] 6.1 为分析逻辑与状态流转补齐单元测试
- [x] 6.2 执行一次端到端演练：触发会话、采集事件、生成建议、审核、回归验证
- [x] 6.3 完成灰度发布与回滚演练记录，确认关闭观测不影响 Codex 主流程
- [x] 6.4 更新 `README_中文.md` 或相关模块文档，沉淀接入与治理规范
