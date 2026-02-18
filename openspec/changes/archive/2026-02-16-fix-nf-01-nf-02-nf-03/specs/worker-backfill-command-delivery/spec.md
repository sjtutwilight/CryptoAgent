## ADDED Requirements

### Requirement: Backfill 调度接口必须返回可分类错误
系统 MUST 使用可分类错误返回 backfill 投递结果，至少包含 `queue_full`、`enqueue_timeout`、`no_target` 三类失败原因。

#### Scenario: 无可用目标时返回 no_target
- **WHEN** 调度器未注册可处理目标
- **THEN** 系统 SHALL 返回 `no_target` 错误而不是静默失败

#### Scenario: 通道拥塞时返回 queue_full 或 enqueue_timeout
- **WHEN** backfill 目标队列拥塞导致无法按时入队
- **THEN** 系统 SHALL 返回对应分类错误并记录失败指标

### Requirement: Backfill 指令投递必须支持阻塞超时语义
系统 MUST 在投递 backfill 指令时执行“阻塞写 + 超时”策略，超时阈值由配置控制。

#### Scenario: 短时拥塞可在超时前成功入队
- **WHEN** 通道暂时满但在超时窗口内释放容量
- **THEN** 系统 SHALL 完成入队并记录等待耗时

#### Scenario: 超时后触发告警事件
- **WHEN** 在 `enqueue_timeout_ms` 内仍无法入队
- **THEN** 系统 SHALL 产生可检索告警事件并包含流与范围信息

### Requirement: 投递失败必须进入持久化补偿并可重放
系统 MUST 在 backfill 投递超时后将指令写入持久化补偿队列，并支持后台重放直到成功或显式终止。

#### Scenario: 超时后写入补偿队列
- **WHEN** backfill 指令投递超时
- **THEN** 系统 SHALL 将该指令持久化并记录补偿队列偏移或主键

#### Scenario: 后台重放恢复投递
- **WHEN** 后台重放任务运行且目标队列恢复可用
- **THEN** 系统 SHALL 将持久化指令重新投递并标记为已补偿

### Requirement: 不同 backfill 类型必须支持隔离限额
系统 MUST 提供至少 snapshot 与 range 的独立队列或配额，避免单类型拥塞阻塞全部补数。

#### Scenario: range 拥塞不阻塞 snapshot
- **WHEN** range backfill 队列持续拥塞
- **THEN** snapshot backfill 仍 SHALL 在其独立配额内继续投递
