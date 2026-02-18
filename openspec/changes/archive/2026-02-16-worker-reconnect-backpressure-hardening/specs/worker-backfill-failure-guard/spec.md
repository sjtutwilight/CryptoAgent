## ADDED Requirements

### Requirement: Bounded Backfill Retry
Worker MUST 对 backfill 失败执行有限重试，并使用退避策略控制重试频率。

#### Scenario: Backfill failure retries with backoff
- **WHEN** 单次 range 或 snapshot backfill 请求失败
- **THEN** Worker MUST 在重试上限内按退避计划重试，并在每次失败后记录失败原因

#### Scenario: Retry limit exhausted stops immediate retrigger
- **WHEN** 同一 backfill 任务达到重试上限
- **THEN** Worker MUST 停止立即重试并标记为 exhausted

### Requirement: Backfill State Feedback
backfill 执行结果 MUST 回写到 worker 状态机，用于驱动后续补数和恢复策略。

#### Scenario: Backfill exhausted transitions state
- **WHEN** backfill 进入 exhausted
- **THEN** Worker MUST 将该数据流状态切换为 degraded 或 cooldown，并暴露可观测事件

### Requirement: Anti-loop Cooldown for Backfill
Worker MUST 在 backfill exhausted 后进入冷静期，避免抖动式重复触发。

#### Scenario: Trigger is blocked during cooldown
- **WHEN** 数据流处于 backfill 冷静期
- **THEN** Worker MUST 拒绝新的 backfill 触发请求并返回冷静期剩余时间
