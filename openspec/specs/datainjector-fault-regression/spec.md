# datainjector-fault-regression Specification

## Purpose
TBD - created by archiving change worker-fault-regression-phase2. Update Purpose after archive.
## Requirements
### Requirement: 故障回归场景必须支持一键执行与模式化注入
系统 MUST 提供可通过统一入口触发的故障回归场景，并支持 `mock` 与 `real` 两种注入模式。场景 MUST 按固定阶段执行：`prepare`、`inject`、`observe`、`assert`、`report`。

#### Scenario: 执行 mock 模式回归
- **WHEN** 用户以 `mock` 模式运行故障回归场景
- **THEN** 系统 MUST 复用 mockDataProvider 配置触发数据丢失或断连故障，并完成完整阶段执行

#### Scenario: 执行 real 模式回归
- **WHEN** 用户以 `real` 模式运行故障回归场景
- **THEN** 系统 MUST 执行可恢复的真实节点故障动作并在结束后恢复节点状态

### Requirement: 回归判定必须按 role 聚合并输出缺失事件
系统 MUST 基于日志事件进行规则判定，并按 `role_id` 输出结果。每个 role 的结果 MUST 至少包含：`status`、`missing_events`、`failed_events`、`evidence_refs`。

#### Scenario: 关键事件缺失
- **WHEN** 某 role 未出现规则要求的关键事件
- **THEN** 该 role MUST 标记为 FAIL，且 `missing_events` MUST 列出缺失事件名

#### Scenario: 命中失败事件
- **WHEN** 某 role 在观察窗口命中失败事件
- **THEN** 该 role MUST 标记为 FAIL，且 `failed_events` MUST 记录事件与时间戳

### Requirement: 断连重连与补数恢复规则必须可配置且默认启用
系统 MUST 内置断连重连和补数恢复的默认判定规则，并允许通过场景参数覆盖观察窗口、必须事件与失败事件集合。

#### Scenario: 使用默认规则
- **WHEN** 用户未显式传入规则配置
- **THEN** 系统 MUST 使用内置默认规则完成判定

#### Scenario: 覆盖规则参数
- **WHEN** 用户传入自定义观察窗口或事件集合
- **THEN** 系统 MUST 以用户参数为准执行判定，并在报告中回显实际生效配置

### Requirement: 回归产物必须标准化并可追溯
系统 MUST 在每次运行后输出标准化产物到运行目录，至少包含 `summary.json`、`summary.txt`、`evidence.jsonl`，且产物 MUST 绑定唯一 `run_id`。

#### Scenario: 回归执行完成
- **WHEN** 故障回归场景正常结束
- **THEN** 系统 MUST 在 `automation/test/runs/<run_id>/` 下写出三类标准产物

#### Scenario: 回归执行失败
- **WHEN** 场景中途失败或超时
- **THEN** 系统 MUST 仍生成包含失败原因和已采集证据的产物文件

