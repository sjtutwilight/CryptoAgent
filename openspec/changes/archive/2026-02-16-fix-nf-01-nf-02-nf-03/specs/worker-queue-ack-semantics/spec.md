## ADDED Requirements

### Requirement: 队列模式任务终态必须以后处理结果为准
系统在 `pipeline_mode=queue` 时 MUST 以消息经过 handler 与 sink 的最终处理结果作为任务终态依据，不得以“仅入队成功”作为最终成功条件。

#### Scenario: 全部消息成功后上报最终成功
- **WHEN** 一个任务被 caller 产出为多条消息且所有消息都成功写入 sink
- **THEN** 系统 SHALL 上报该任务最终状态为 `SUCCESS`

#### Scenario: 任一消息失败时禁止最终成功
- **WHEN** 任务中任一消息在 handler 或 sink 阶段失败且未在重试窗口内恢复
- **THEN** 系统 SHALL 上报任务最终失败，且 MUST NOT 上报该任务最终 `SUCCESS`

### Requirement: 系统必须提供阶段化状态上报
系统 MUST 支持任务阶段化状态并上报至少以下阶段：`caller_accepted`、`queue_enqueued`、`pipeline_succeeded`、`pipeline_failed`、`final_succeeded`、`final_failed`。

#### Scenario: 队列链路按阶段上报
- **WHEN** 队列模式任务从 caller 执行到 sink 完成全流程
- **THEN** 系统 SHALL 产出可按 `task_id/run_id/stage` 关联的阶段事件序列

#### Scenario: 阶段状态包含错误分类
- **WHEN** 任务在 pipeline 阶段失败
- **THEN** 失败状态事件 MUST 包含 `error_class` 与 `attempt` 字段用于定位

### Requirement: 消费失败必须进入重试与死信流程
系统 MUST 对队列消费失败执行有限重试；重试耗尽后 MUST 将消息写入 DLQ 并将任务标记为最终失败。

#### Scenario: 重试成功则任务可继续完成
- **WHEN** 消息初次处理失败但在配置重试次数内处理成功
- **THEN** 系统 SHALL 继续推进任务完成并保留重试记录

#### Scenario: 重试耗尽后写入 DLQ
- **WHEN** 消息达到最大重试次数后仍失败
- **THEN** 系统 SHALL 将该消息写入 DLQ，并上报对应任务最终失败
