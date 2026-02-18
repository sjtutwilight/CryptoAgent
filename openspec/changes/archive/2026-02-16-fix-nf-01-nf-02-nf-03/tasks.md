## 1. 配置与通用模型改造

- [x] 1.1 在配置层新增并校验开关与阈值：`strict_task_finalization`、`ws_bounded_buffer`、`backfill_enqueue_timeout_ms`、`backfill_persistent_compensation`
- [x] 1.2 扩展状态事件模型字段：`stage`、`error_class`、`attempt`、`role_id`、`run_id`，保持旧消费者兼容
- [x] 1.3 定义并实现 backfill 分类错误类型（`queue_full`、`enqueue_timeout`、`no_target`）及统一错误编码

## 2. NF-01 队列确认语义与失败治理

- [x] 2.1 在 `internal/role/role.go` 引入 `TaskTracker`，按 `task_id+run_id` 跟踪 expected/enqueued/processed/failed
- [x] 2.2 将队列模式成功上报从 `fireFunc` 移除，改为消费完成后的最终聚合上报
- [x] 2.3 为 handler/sink 失败实现有限重试（指数回退）并记录 attempt/error_class
- [x] 2.4 实现重试耗尽后的 DLQ 写入路径与最终失败状态上报
- [x] 2.5 增加任务状态过期回收与并发上限保护，避免 tracker 泄漏

## 3. NF-02 WebSocket 有界缓冲与背压控制

- [x] 3.1 将 `WebSocketCall.msgBuffer` 改为 ring buffer，支持 `max_messages`+`max_bytes` 双阈值
- [x] 3.2 在 ring buffer 实现 `drop_oldest`、`drop_newest`、`drop_by_stream` 策略并暴露策略配置
- [x] 3.3 统一协议层、shared hub、caller 层溢出指标与结构化日志字段
- [x] 3.4 实现高低水位背压状态机并在消息 metadata 打 `ws_backpressure` 标记
- [x] 3.5 在持续背压场景接入降载动作与 backfill 补偿触发点

## 4. NF-03 Backfill 指令可靠投递

- [x] 4.1 将 `Scheduler/Target` 接口从 `bool` 返回改为 `error` 返回，并完成调用方迁移
- [x] 4.2 把 `ChannelTarget` 改为阻塞写 + 超时语义，输出 `queue_full/enqueue_timeout` 分类错误
- [x] 4.3 将 `SequenceEngine.triggerBackfill` 失败路径改为强告警与指标打点，不再静默返回
- [x] 4.4 引入 backfill 持久化补偿队列（当前实现为文件落盘 + 重放 worker，预留 Kafka/SQLite 扩展）并实现重放 worker
- [x] 4.5 增加 snapshot/range 隔离配额（独立队列或限额），避免相互挤压

## 5. 可观测性与运行保障

- [x] 5.1 新增并接线关键指标：任务阶段计数、WS 各层丢弃计数、backfill 入队延迟与补偿积压
- [x] 5.2 补齐结构化日志字段规范，统一 `role_id/task_id/run_id/stage/backfill_type`
- [x] 5.3 配置默认值与灰度开关文档化，提供保守默认参数与回滚说明

## 6. 测试与验收

- [x] 6.1 增加 NF-01 集成测试：入队成功但 sink 失败时不得上报最终 SUCCESS
- [x] 6.2 增加 NF-02 压测/单测：慢消费下内存封顶且丢弃指标可观测
- [x] 6.3 增加 NF-03 测试：backfill 队列满时产生可检索告警并进入补偿重放
- [x] 6.4 增加兼容性测试：旧状态消费端在新增字段下可正常解析
- [x] 6.5 执行灰度演练脚本（启用/关闭关键开关）并记录回滚验证结果
