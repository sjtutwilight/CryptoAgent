# NF-01/NF-02/NF-03 灰度发布与回滚指引

## 1. 新增配置项（Role 级）

- `strict_task_finalization`：是否启用“最终成功=消费链路最终成功”语义，默认 `true`
- `ws_bounded_buffer`：是否启用 WebSocket caller 有界缓冲，默认 `true`
- `backfill_result_driven_enabled`：是否启用 backfill 结果驱动闭环（single-flight session），默认 `false`
- `backfill_enqueue_timeout_ms`：backfill 指令入队超时，默认 `200`
- `backfill_persistent_compensation`：是否启用 backfill 持久化补偿与重放，默认 `false`
- `backfill_compensation_file`：补偿队列落盘文件，默认 `runtime/data/backfill_compensation.json`
- `backfill_replay_interval_ms`：补偿重放周期，默认 `2000`
- `backfill_compensation_max_pending`：补偿最大积压，默认 `2000`

队列保护参数（`queue`）：
- `task_ttl_seconds`：任务跟踪状态 TTL，默认 `1800`
- `max_tracked_tasks`：任务跟踪最大并发，默认 `10000`
- `backfill_queue_cap`：backfill 通道容量，默认 `256`

## 2. 推荐灰度顺序

1. 先开观测，不改行为：保持 `strict_task_finalization=false`，开启 `ws_bounded_buffer=true`
2. 开启 backfill 超时语义：设置 `backfill_enqueue_timeout_ms=200`
3. 小流量开启结果驱动闭环：`backfill_result_driven_enabled=true`（5% role）
4. 小流量开启严格终态：`strict_task_finalization=true`
5. 小流量开启持久化补偿：`backfill_persistent_compensation=true`
6. 全量放开，并观察 24 小时

## 3. 关键观测项

- 任务阶段：`worker_task_stage_total`
- WebSocket 丢弃：`worker_websocket_drops_total`
- backfill 入队时延：`worker_integrity_backfill_enqueue_latency_seconds`
- backfill 补偿积压：`worker_integrity_backfill_compensation_backlog`
- backfill 单飞会话：`worker_integrity_backfill_sessions_inflight`
- backfill 去重命中：`worker_integrity_backfill_schedule_dedup_total`
- backfill 结果分布：`worker_integrity_backfill_result_total`
- backfill pending 时长：`worker_integrity_backfill_pending_duration_seconds`

## 4. 故障演练

### 4.1 Sink 失败演练
- 注入 sink 持续失败
- 预期：任务出现 `pipeline_failed/final_failed`，不得出现最终 `SUCCESS`

### 4.2 WS 背压演练
- 降低消费速度并提高输入速率
- 预期：进入背压日志，caller 开始偏向 `drop_newest`，内存曲线封顶

### 4.3 Backfill 通道拥塞演练
- 限制 backfill worker 处理速率，制造队列拥塞
- 预期：出现 `enqueue_timeout` 分类错误并进入持久化补偿，重放恢复后积压下降

## 5. 回滚策略

- 行为回滚：
  - `strict_task_finalization=false`
  - `ws_bounded_buffer=false`
  - `backfill_result_driven_enabled=false`
  - `backfill_persistent_compensation=false`
- 参数回滚：放宽 `backfill_enqueue_timeout_ms`，降低背压触发阈值
- 保留新增指标与日志，继续用于定位

## 6. 上线前检查清单（AAVE 四角色）

1. 四个 role 已设置 `backfill_result_driven_enabled=true`
2. 连续观测 30 分钟，`worker_integrity_backfill_sessions_inflight` 不出现同 key > 1
3. 观测窗口内 `integrity.backfill.enqueue.error` 不再洪泛（允许偶发）
4. 主题持续有增量写入，无长时间断流（>= 60s）
5. 发生失败时存在 `integrity.backfill.result` 与 `error_class` 可追踪
