## 1. 协议与数据结构

- [x] 1.1 在 `internal/types/backfill.go` 扩展 `BackfillCmd`（`cmd_id/session_id/key/attempt`）并新增 `BackfillResult` 结构与错误分类映射
- [x] 1.2 为新协议字段补充向后兼容逻辑（旧调用方缺字段时自动填充）
- [x] 1.3 为 `BackfillCmd/BackfillResult` 增加单元测试（序列化、幂等键、错误分类）

## 2. SequenceEngine 单飞状态机

- [x] 2.1 在 `internal/handler/integrity/sequence_engine.go` 引入会话状态（`idle/pending/cooldown`）与会话 key 索引
- [x] 2.2 重构 `triggerBackfill/checkTimeout/retrySnapshotBackfill`：pending 期间不重复调度，改为合并意图
- [x] 2.3 实现 `OnBackfillResult`（或等价入口），用 success/fail 回执驱动状态迁移
- [x] 2.4 增加状态机测试矩阵（并发触发、重复触发、结果乱序、cooldown 恢复）

## 3. 调度与执行闭环

- [x] 3.1 在 `internal/handler/integrity/scheduler.go` 实现 keyed 去重队列（同 key 合并/覆盖）
- [x] 3.2 在 `internal/role/role.go` backfill worker 回写 `BackfillResult`，覆盖 success/fail/timeout 路径
- [x] 3.3 将重试退避集中到执行器路径，移除检测器侧重复重发逻辑
- [x] 3.4 增加调度器与执行闭环集成测试（queue_full、enqueue_timeout、重放成功）

## 4. 可观测性与灰度开关

- [x] 4.1 在 metrics/logging 增加会话不变量指标与事件（inflight、dedup、result、pending_duration）
- [x] 4.2 增加 feature flag（启用结果驱动闭环）并支持运行时配置
- [x] 4.3 更新 `LOGGING_SPEC.md` 与相关文档，补充新事件字典与排障指引

## 5. 回归验证与发布

- [ ] 5.1 用 AAVE 四个 role 执行 30 分钟观测，验证无 backfill 洪泛且 topic 持续写入
- [x] 5.2 补充自动化回归场景（高频缺失 + backfill 慢响应）并纳入 CI
- [x] 5.3 制定灰度与回滚步骤，完成上线前检查清单
