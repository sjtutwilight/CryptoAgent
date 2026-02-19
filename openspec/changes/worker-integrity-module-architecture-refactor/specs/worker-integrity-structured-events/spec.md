## MODIFIED Requirements

### Requirement: 完整性与补数关键路径必须输出结构化事件
worker MUST 为完整性检测与 backfill 生命周期输出结构化 JSON 日志事件，覆盖至少：gap 触发、backfill 触发、attempt 失败、backfill 成功、backfill exhausted、snapshot 重锚、timeout 推进、会话状态迁移。

#### Scenario: 触发补数请求
- **WHEN** 完整性引擎检测到需要补数的 gap
- **THEN** worker MUST 输出包含 backfill 类型、范围与触发原因的结构化事件

#### Scenario: 快照重锚发生
- **WHEN** 引擎执行 snapshot anchor（事件或回调触发）
- **THEN** worker MUST 输出包含 `expected_prev`、`expected_new`、`anchor_source` 的结构化事件

#### Scenario: 补数耗尽失败
- **WHEN** 所有补数 attempt 均失败
- **THEN** worker MUST 输出 `backfill exhausted` 结构化失败事件

### Requirement: 结构化事件字段必须满足自动判定最小字段集
完整性与 backfill 事件 MUST 至少包含 `event`、`ts`、`level`、`service`、`role_id`，并按事件类型携带 `stream_key`、`backfill_type`、`attempt`、`start`、`end`、`error_detail`、`session_key`、`session_state`、`expected_seq`、`seen_max` 等字段。

#### Scenario: 记录 backfill 成功事件
- **WHEN** 某次 backfill attempt 成功
- **THEN** 事件 MUST 包含 `role_id`、`backfill_type`、`attempt`、`session_key` 与成功状态字段

#### Scenario: 记录 timeout 推进事件
- **WHEN** 引擎因 `hard_timeout` 执行推进
- **THEN** 事件 MUST 包含推进前后期望序列与触发超时信息

#### Scenario: 记录 backfill 失败事件
- **WHEN** 某次 backfill request 返回错误
- **THEN** 事件 MUST 包含 `role_id`、`session_key` 与错误字段，供回归判定器直接消费
