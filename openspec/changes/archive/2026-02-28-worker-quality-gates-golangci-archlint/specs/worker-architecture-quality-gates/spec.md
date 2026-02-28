## ADDED Requirements

### Requirement: Worker 模块必须提供统一的质量门禁执行入口
系统 MUST 为 `datainjector/worker` 提供单一命令入口，按固定顺序执行 `golangci-lint` 与 `go-arch-lint`，并在任一检查失败时返回非零退出码。

#### Scenario: 本地执行统一门禁命令
- **WHEN** 开发者在仓库中执行质量门禁入口命令
- **THEN** 系统 MUST 依次执行代码质量检查和架构依赖检查，并输出每一步结果

#### Scenario: 任一检查失败时阻断
- **WHEN** `golangci-lint` 或 `go-arch-lint` 返回失败
- **THEN** 质量门禁入口 MUST 返回非零退出码并标记失败步骤

### Requirement: 系统必须定义并校验 worker 核心包依赖边界
系统 MUST 通过可执行架构规则约束 `role/emitter/caller/handler/sink/protocol` 的依赖方向，禁止未声明的跨层和反向依赖。

#### Scenario: 出现未声明的跨层依赖
- **WHEN** 新代码引入不在允许列表中的包依赖关系
- **THEN** 架构检查 MUST 报告违规依赖边并返回失败

#### Scenario: 依赖关系符合规则
- **WHEN** 代码依赖关系全部满足已声明架构规则
- **THEN** 架构检查 MUST 返回成功且不报告违规边

### Requirement: 质量门禁结果必须支持机器可读消费
系统 MUST 输出可由 Codex 和 CI 解析的检查结果格式，并至少包含工具名称、违规条目、文件路径与退出状态。

#### Scenario: 生成机器可读报告
- **WHEN** 质量门禁命令执行完成
- **THEN** 系统 MUST 生成至少一种机器可读结果（如 JSON）用于自动化消费

#### Scenario: CI 依据结果执行阻断策略
- **WHEN** CI 读取到新增违规或严重违规
- **THEN** CI MUST 标记流水线失败并给出可定位的违规摘要

### Requirement: 历史质量债务必须采用基线冻结并阻断新增违规
系统 MUST 支持历史问题基线机制，在不放行新增违规的前提下逐步收敛存量问题。

#### Scenario: 存量问题存在但无新增违规
- **WHEN** 当前分支仅命中已登记基线问题
- **THEN** 系统 MUST 按策略允许通过并提示存量债务数量

#### Scenario: 引入新增违规
- **WHEN** 当前分支出现基线外的新违规
- **THEN** 系统 MUST 阻断并明确标识新增违规明细
