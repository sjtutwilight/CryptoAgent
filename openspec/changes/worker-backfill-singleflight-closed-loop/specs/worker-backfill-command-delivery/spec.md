## MODIFIED Requirements

### Requirement: Backfill 调度接口必须返回可分类错误
系统 MUST 使用可分类错误返回 backfill 投递结果，至少包含 `queue_full`、`enqueue_timeout`、`no_target` 三类失败原因；同时 MUST 在投递响应中包含可追踪的 `cmd_id` 与会话标识，支持后续结果闭环关联。

#### Scenario: 无可用目标时返回 no_target
- **WHEN** 调度器未注册可处理目标
- **THEN** 系统 SHALL 返回 `no_target` 错误而不是静默失败，并附带本次 `cmd_id`

#### Scenario: 通道拥塞时返回 queue_full 或 enqueue_timeout
- **WHEN** backfill 目标队列拥塞导致无法按时入队
- **THEN** 系统 SHALL 返回对应分类错误并记录失败指标，同时保留可用于补偿重放的命令标识

### Requirement: Backfill 指令投递必须支持阻塞超时语义
系统 MUST 在投递 backfill 指令时执行“阻塞写 + 超时”策略，超时阈值由配置控制；对于同一会话 key 的重复触发，系统 SHALL 使用去重合并语义而非重复入队。

#### Scenario: 短时拥塞可在超时前成功入队
- **WHEN** 通道暂时满但在超时窗口内释放容量
- **THEN** 系统 SHALL 完成入队并记录等待耗时

#### Scenario: 超时后触发告警事件
- **WHEN** 在 `enqueue_timeout_ms` 内仍无法入队
- **THEN** 系统 SHALL 产生可检索告警事件并包含流与范围信息

### Requirement: 投递失败必须进入持久化补偿并可重放
系统 MUST 在 backfill 投递超时后将指令写入持久化补偿队列，并支持后台重放直到成功或显式终止；补偿或重放完成后 MUST 产生可关联 `cmd_id` 的结果回执。

#### Scenario: 超时后写入补偿队列
- **WHEN** backfill 指令投递超时
- **THEN** 系统 SHALL 将该指令持久化并记录补偿队列偏移或主键

#### Scenario: 后台重放恢复投递
- **WHEN** 后台重放任务运行且目标队列恢复可用
- **THEN** 系统 SHALL 将持久化指令重新投递并标记为已补偿，同时输出 success 回执
