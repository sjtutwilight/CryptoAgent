## ADDED Requirements

### Requirement: 完整性与补数关键路径必须输出结构化事件
worker MUST 为完整性检测与 backfill 生命周期输出结构化 JSON 日志事件，覆盖至少：gap 触发、backfill 触发、attempt 失败、backfill 成功、backfill exhausted。

#### Scenario: 触发补数请求
- **WHEN** 完整性引擎检测到需要补数的 gap
- **THEN** worker MUST 输出包含 backfill 类型与范围信息的结构化事件

#### Scenario: 补数耗尽失败
- **WHEN** 所有补数 attempt 均失败
- **THEN** worker MUST 输出 `backfill exhausted` 结构化失败事件

### Requirement: 结构化事件字段必须满足自动判定最小字段集
完整性与 backfill 事件 MUST 至少包含 `event`、`ts`、`level`、`service`、`role_id`，并按事件类型携带 `stream_key`、`backfill_type`、`attempt`、`start`、`end`、`error_detail` 等字段。

#### Scenario: 记录 backfill 成功事件
- **WHEN** 某次 backfill attempt 成功
- **THEN** 事件 MUST 包含 `role_id`、`backfill_type`、`attempt` 与成功状态字段

#### Scenario: 记录 backfill 失败事件
- **WHEN** 某次 backfill request 返回错误
- **THEN** 事件 MUST 包含 `role_id` 与错误字段，供回归判定器直接消费

### Requirement: 结构化事件字典必须成为单一事实源
worker MUST 在事件常量与日志规范文档中登记新增完整性/backfill事件，避免运行时事件名与文档定义不一致。

#### Scenario: 新增完整性事件
- **WHEN** 开发者新增完整性或 backfill 事件
- **THEN** 事件常量定义与日志规范文档 MUST 同步更新

#### Scenario: 判定器消费事件
- **WHEN** 自动判定器按事件名拉取日志
- **THEN** 事件名 MUST 与规范一致且可稳定匹配

### Requirement: 自动回归判定必须优先基于结构化事件
回归判定逻辑 MUST 优先消费结构化事件完成核心判定，不得将纯文本日志作为唯一判定依据；纯文本日志仅可作为兼容证据源。

#### Scenario: 结构化事件可用
- **WHEN** 日志中存在完整结构化事件
- **THEN** 判定器 MUST 仅依赖结构化事件完成核心规则计算

#### Scenario: 结构化事件部分缺失
- **WHEN** 少量结构化事件缺失但存在文本线索
- **THEN** 判定器 MAY 使用文本线索补充证据，并在报告中标注“兼容路径命中”
