# worker-role-atomic-reconcile Specification

## Purpose
TBD - created by archiving change worker-role-hot-reload-p0. Update Purpose after archive.
## Requirements
### Requirement: Apply MUST execute role-level reconcile instead of global replace
系统在处理 roles 热更新时 MUST 基于 `role_id` 计算差异集合（`add`、`update`、`remove`、`unchanged`），并且仅对差异集合执行生命周期操作。系统 MUST NOT 在 apply 流程中对全部运行角色执行统一停止。

#### Scenario: Non-target roles remain running during apply
- **WHEN** 当前运行角色为 `A,B,C`，新配置仅变更 `B`
- **THEN** 系统仅对 `B` 执行更新动作，`A,C` 在整个 apply 过程中保持运行

### Requirement: Apply MUST use two-phase switch for add/update roles
系统 MUST 对 `add/update` 角色先执行 Prepare（构建、校验、就绪检查），仅当全部目标角色 Prepare 成功后，才进入 Commit（替换提交）阶段。若任一目标角色 Prepare 失败，系统 MUST 终止本次提交并保持当前运行态不变。

#### Scenario: Prepare failure blocks commit
- **WHEN** 本次 apply 的目标角色中任一角色在 Prepare 阶段构建或就绪失败
- **THEN** 系统不执行任何角色替换提交，并返回失败结果

### Requirement: Role switch MUST support per-role rollback
在 Commit 阶段，系统 MUST 以单个 `role_id` 为事务边界执行替换。若某角色替换失败，系统 MUST 回滚该角色到旧实例并保留其可用状态；其他已成功提交或未受影响的角色 MUST 保持各自状态。

#### Scenario: One role commit failure does not break others
- **WHEN** 同一批次中角色 `R1` 提交成功而 `R2` 提交失败
- **THEN** 系统保持 `R1` 的新实例运行，并将 `R2` 回滚到旧实例，且不停止其他无关角色

### Requirement: Apply API MUST return per-role execution results
控制面 apply 接口 MUST 返回按角色维度的执行结果，至少包含 `role_id`、`action`、`status`、`error` 字段，用于识别成功、失败与回滚状态。

#### Scenario: Caller can identify failed roles for retry
- **WHEN** 一次 apply 同时包含成功和失败的角色
- **THEN** 响应中包含每个角色的独立结果，调用方可据此仅重试失败角色

### Requirement: Role reconcile MUST clear and rebuild scoped orderbook state on switch
在 role 执行 `update/remove` 的提交阶段，系统 MUST 清理该 `role_id` 对应的订单簿作用域状态；在新实例启动后，系统 MUST 基于新作用域重新建立状态。

#### Scenario: Update role does not inherit stale shared memory
- **WHEN** 角色 `R` 在 apply 中被更新为新实例
- **THEN** 新实例不会继承旧实例的订单簿内存状态，且仅使用自身作用域状态

### Requirement: Scoped state cleanup MUST be limited to target roles
系统在 reconcile 期间进行状态清理时 MUST 仅影响目标 `role_id` 的作用域。系统 MUST NOT 清理非目标 role 的订单簿作用域状态。

#### Scenario: Non-target role keeps producing during update
- **WHEN** apply 仅更新 `R1`，`R2` 为未变更 role
- **THEN** `R2` 的订单簿状态与产出持续不中断

