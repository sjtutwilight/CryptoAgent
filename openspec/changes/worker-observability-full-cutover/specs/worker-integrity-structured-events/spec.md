## MODIFIED Requirements

### Requirement: 完整性与补数关键路径必须输出结构化事件
worker MUST 为完整性检测与 backfill 生命周期输出结构化 JSON 日志事件，覆盖至少：gap 检测、backfill trigger、session pending、dedup、attempt error、success、result、exhausted。

#### Scenario: 触发补数请求
- **WHEN** 完整性引擎检测到需要补数的 gap
- **THEN** worker MUST 输出包含 `backfill_type`、`start`、`end`、`session_key` 的结构化事件

#### Scenario: 会话结果回写
- **WHEN** backfill 会话完成（成功、失败或超时）
- **THEN** worker MUST 输出 `integrity.backfill.result` 且包含 `status`、`error_class`、`pending_ms`、`session_id`、`cmd_id`

### Requirement: 结构化事件字段必须满足自动判定最小字段集
完整性与 backfill 事件 MUST 至少包含 `event`、`ts`、`level`、`service`、`role_id`；当事件属于会话态（session/dedup/result）时 MUST 同时包含 `stream_key`、`backfill_type`、`session_key`、`session_id`、`cmd_id`。

#### Scenario: 记录 backfill 成功事件
- **WHEN** 某次 backfill attempt 成功
- **THEN** 事件 MUST 包含 `role_id`、`stream_key`、`backfill_type`、`attempt`、`session_id`、`cmd_id`

#### Scenario: 记录 backfill 失败事件
- **WHEN** 某次 backfill request 返回错误
- **THEN** 事件 MUST 包含 `role_id`、`error_class` 与错误详情字段，供告警与回归判定直接消费

### Requirement: 自动回归判定必须优先基于结构化事件
回归判定逻辑 MUST 以结构化事件作为唯一核心判定输入，不得把纯文本日志作为主路径。

#### Scenario: 结构化事件可用
- **WHEN** 日志中存在完整结构化事件
- **THEN** 判定器 MUST 仅依赖结构化事件完成核心规则计算

#### Scenario: 结构化事件缺失
- **WHEN** 必需结构化字段缺失
- **THEN** 判定器 MUST 将该次判定标记为数据不合规并返回失败，而不是降级到纯文本主判定
