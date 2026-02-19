## ADDED Requirements

### Requirement: Backfill scheduling MUST enforce keyed singleflight
系统 MUST 以 `session_key(role_id + stream_key + backfill_type)` 为粒度执行单飞调度，同一 key 在任意时刻仅允许一个 in-flight backfill 指令。

#### Scenario: Duplicate trigger merges into pending intent
- **WHEN** 同一 `session_key` 已处于 pending，且再次收到 backfill 触发
- **THEN** 系统 SHALL 合并为 intent 更新而不是再次入队新指令

#### Scenario: Different session keys can progress concurrently
- **WHEN** 不同 `session_key` 同时触发 backfill
- **THEN** 系统 MUST 允许各自独立入队与执行，不得互相阻塞

### Requirement: Pending session MUST be advanced by explicit backfill results
系统 MUST 使用 backfill result 事件驱动 pending 会话迁移，并在结果丢失时提供受控超时兜底。

#### Scenario: Success result releases next merged intent
- **WHEN** pending 会话收到 success 结果且存在已合并 intent
- **THEN** 系统 SHALL 将会话迁移到可调度状态并触发下一次意图执行

#### Scenario: Timeout result enters cooldown path
- **WHEN** pending 会话超过设定超时时间仍未收到结果
- **THEN** 系统 MUST 生成超时结果并进入失败计数/冷却状态机分支
