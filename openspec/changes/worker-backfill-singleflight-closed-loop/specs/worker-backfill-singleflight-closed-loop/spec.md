## ADDED Requirements

### Requirement: Backfill 会话必须满足单飞约束
系统 MUST 对每个 `role_id + stream_key + backfill_type` 维度维持唯一 backfill 会话；同一维度在 `pending` 状态时 SHALL 拒绝创建第二个 in-flight backfill。

#### Scenario: pending 会话期间再次触发缺失检测
- **WHEN** 同一 `role_id + stream_key + backfill_type` 已存在 pending 会话且再次触发 backfill
- **THEN** 系统 SHALL 不再创建新的 in-flight backfill，而是将请求合并到现有会话

#### Scenario: 不同 stream_key 并行触发
- **WHEN** 不同 `stream_key` 各自触发 backfill
- **THEN** 系统 SHALL 允许它们在独立会话中并行执行

### Requirement: 会话状态推进必须由 backfill 结果驱动
系统 MUST 仅在收到 backfill 结果事件（success/fail）后推进会话状态；定时器重试 SHALL 不得直接绕过结果闭环创建新会话。

#### Scenario: backfill 成功回执
- **WHEN** pending 会话收到 success 结果
- **THEN** 系统 SHALL 将会话状态推进为 idle 并允许后续新调度

#### Scenario: backfill 失败回执
- **WHEN** pending 会话收到 fail 结果且达到失败阈值
- **THEN** 系统 SHALL 将会话推进到 cooldown 并在冷却结束前拒绝新调度

### Requirement: 调度层必须支持按 key 去重合并
系统 MUST 以会话 key 执行队列去重；相同 key 的后续请求 SHALL 被合并或覆盖，且不会产生无限增长的重复排队项。

#### Scenario: 高频重复触发同一 key
- **WHEN** 同一 key 在短时间内被重复触发多次
- **THEN** 系统 SHALL 在调度队列中保留单个可执行实体并记录去重计数

#### Scenario: 执行完成后接收新请求
- **WHEN** 同一 key 的会话已完成并回到 idle
- **THEN** 系统 SHALL 为该 key 创建新的可执行调度项

### Requirement: 失败重试必须受统一退避策略约束
系统 MUST 对同一会话的失败执行指数退避和最大尝试限制；达到阈值后 SHALL 进入冷却窗口。

#### Scenario: 临时失败后重试成功
- **WHEN** backfill 首次失败但在后续退避重试中成功
- **THEN** 系统 SHALL 结束会话并清零该会话失败计数

#### Scenario: 连续失败达到阈值
- **WHEN** backfill 连续失败达到 `max_failures`
- **THEN** 系统 SHALL 进入 cooldown 并拒绝该 key 的立即重试

### Requirement: 系统必须暴露会话级可观测性不变量
系统 MUST 提供会话级指标与事件，用于验证“无重复并发、无重试洪泛、可追踪状态迁移”。

#### Scenario: 运行期查询 in-flight 会话
- **WHEN** 监控系统抓取 backfill 指标
- **THEN** 系统 SHALL 提供按 `role_id/stream_key/type` 维度的 in-flight 会话数

#### Scenario: 调度去重生效
- **WHEN** 相同 key 请求被去重合并
- **THEN** 系统 SHALL 记录可检索的去重计数指标与事件
