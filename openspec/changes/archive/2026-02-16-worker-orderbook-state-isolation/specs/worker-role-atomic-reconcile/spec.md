## ADDED Requirements

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
