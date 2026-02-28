## ADDED Requirements

### Requirement: Codex 行为事件必须通过统一 OTLP 管道上报
系统 MUST 将 Codex 会话生命周期、工具决策与执行结果、补丁应用等关键事件通过 OTLP 上报到统一 Collector，不得绕过治理层直写后端。

#### Scenario: 关键事件完整到达 Collector
- **WHEN** Codex 在仓库内完成一次包含工具调用与补丁应用的会话
- **THEN** Collector MUST 接收到 `session_start`、`tool_decision`、`tool_result|tool_error`、`patch_apply`、`session_end` 等关键事件

#### Scenario: 导出失败不阻塞主流程
- **WHEN** OTel 后端短暂不可用或网络抖动
- **THEN** Codex 主执行流程 MUST 继续进行，且系统 MUST 记录导出失败事件用于告警

### Requirement: 采集策略必须开启完整提示词上下文
系统 MUST 启用原始用户提示词采集，并保持关键字段原始内容，不执行内容级脱敏或删改。

#### Scenario: 默认开启用户提示词采集
- **WHEN** 使用生产配置启动 Codex OTel
- **THEN** 配置 MUST 保持 `log_user_prompt=true`

#### Scenario: 提示词内容保持原始可观测
- **WHEN** Collector 接收到包含关键字段的提示词事件
- **THEN** Collector MUST 以原始内容入库，不得执行字段脱敏或内容删改

### Requirement: 日志与 Trace 必须支持跨源关联检索
系统 MUST 支持通过统一会话或 trace 维度在 Grafana 中完成日志与链路的双向跳转。

#### Scenario: 从日志跳转到链路
- **WHEN** 值班人员在日志中定位到异常 `trace_id`
- **THEN** 系统 MUST 能在 Trace 数据源中检索到对应链路

#### Scenario: 从链路回查关键日志
- **WHEN** 值班人员在链路中定位到失败 span
- **THEN** 系统 MUST 能基于会话或 trace 维度回查相关日志片段
