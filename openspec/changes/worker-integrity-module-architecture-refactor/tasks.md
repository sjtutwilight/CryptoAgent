## 1. 观测先行与基线护栏

- [x] 1.1 在 `buffer` 的 add/drain/cleanup/sweep 路径接入 `SetIntegrityBufferSize` 指标更新
- [x] 1.2 在 `dedupe.ShouldDrop` 命中分支接入 `RecordIntegrityDuplicate` 计数
- [x] 1.3 新增完整性状态指标（`expected_seq`、`seen_max`、`head_lag`、`awaiting_snapshot`）及注册代码
- [x] 1.4 新增 gap-window 指标（窗口数量、总缺失长度、最老缺口年龄）及注册代码
- [x] 1.5 补充结构化事件常量：snapshot 重锚、timeout 推进、session 状态迁移
- [x] 1.6 为新增指标与事件补充单元测试（presence/label 最小集）

## 2. 状态机核心抽离

- [x] 2.1 新建 `sequencer core` 状态结构与 `Input/Action` 契约（纯内存、无 IO）
- [x] 2.2 将 `onEqual/onCover/onGap/advance/checkBudget` 迁移为 core 的状态迁移规则
- [x] 2.3 将 `hard_timeout` 与 `max_delay` 判定改为 core 内固定优先级（hard 优先）
- [x] 2.4 在 `SequenceEngine` 中引入执行壳，负责消费 core actions 并调用现有 scheduler/gate/metrics/logging
- [x] 2.5 增加状态机表驱动测试，覆盖初始化、乱序、覆盖、推进、超时分支

## 3. Snapshot 重锚语义收敛

- [x] 3.1 新增统一重锚动作 `ApplyAnchor(lastSeq, source)` 并接入执行壳
- [x] 3.2 将 `OnSnapshotApplied` 路径改为仅触发统一重锚动作，不再维护独立状态分叉
- [x] 3.3 为 `snapshot_sidechannel` 增加“可解析锚点即重锚”的处理逻辑与兼容降级日志
- [x] 3.4 调整 gate 交互时序，确保重锚后 buffer 清理与释放顺序一致
- [x] 3.5 增加 sidechannel/gate 双模式集成测试（含断网大缺口恢复场景）

## 4. Backfill 编排解耦与闭环强化

- [x] 4.1 将 session 状态机（idle/pending/cooldown）抽离到 `BackfillOrchestrator`
- [x] 4.2 迁移 pending intent merge、超时回收、失败冷却逻辑到 orchestrator
- [x] 4.3 保持 `session_key` 单飞不变量并补充跨 key 并发测试
- [x] 4.4 打通 result 驱动闭环（success 释放 intent、timeout/fail 进入受控分支）
- [x] 4.5 校准 compensation replay 与 orchestrator 的交互边界，避免重复调度

## 5. 缺失窗口与缓冲治理

- [x] 5.1 实现缺失窗口数据结构（区间增删并）并接入 gap/anchor/advance 路径
- [x] 5.2 将 window 视图与 buffer 状态联动更新，确保与 `ExpectedNext` 一致
- [x] 5.3 为窗口结构新增边界测试（重复 seq、TTL 清理、maxBuckets 触发）
- [x] 5.4 补充 `buffer.go` 专项测试覆盖 drain/cleanup/sweep 全边界

## 6. 配置兼容、灰度与收尾

- [x] 6.1 增加 feature flag（新 core/orchestrator/anchor 语义）并保证默认兼容旧行为
- [x] 6.2 补充 `config_parser.go` 测试，覆盖 profile 默认值与 `orderbook_mode` 覆盖关系
- [x] 6.3 在 AAVE spot/perp role 做灰度验证，记录关键指标基线与告警阈值建议
- [x] 6.4 清理旧分叉代码路径与失效状态字段，更新开发文档与运维排障说明
