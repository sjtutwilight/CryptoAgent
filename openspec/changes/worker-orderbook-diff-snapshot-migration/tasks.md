## 1. 完整性引擎重构（去本地簿耦合）

- [x] 1.1 在 `integrity` 配置中引入 orderbook 新模式（snapshot backfill 旁路，不依赖 snapshot gate 放行）。
- [x] 1.2 调整 `SequenceEngine` 的 orderbook 路径：gap 触发 snapshot backfill，diff 主流继续按序处理且不等待 `OnSnapshotApplied`。
- [x] 1.3 为 snapshot backfill 输出补充来源与原因元数据（如 `snapshot_source`、`snapshot_reason`）。

## 2. Orderbook handler 与状态模块下线

- [x] 2.1 删除 `orderbook_diff` 与 `orderbook_validator` handler 实现及注册。
- [x] 2.2 删除 `internal/resource/orderbook/*` 本地订单簿状态实现与无用引用。
- [x] 2.3 更新配置校验逻辑，移除对已删除 handler 的相关分支与约束。

## 3. 双 topic 路由能力落地

- [x] 3.1 新增 orderbook topic 路由 handler，按事件类型写入 `ob_topic`（diff/snapshot）。
- [x] 3.2 在 AAVE orderbook 角色 sink 中启用 `topic_field` 路由到 `*.orderbook.diff` 与 `*.orderbook.snapshot`。
- [x] 3.3 确保 backfill snapshot 与周期 snapshot 均走 snapshot topic，实时 diff 仅走 diff topic。

## 4. AAVE 角色配置重写

- [x] 4.1 重写 perp/spot orderbook diff roles，移除本地簿 handler，仅保留 parser + integrity + router。
- [x] 4.2 新增 perp/spot 周期 snapshot polling roles，固定 `polling_interval=10` 并调用对应 depth REST。
- [x] 4.3 保持 perp/spot aggtrade roles 可用并校准 topic/key 配置不回退。

## 5. 可观测性与契约收敛

- [x] 5.1 更新日志与指标定义，覆盖 gap 检测、snapshot backfill 触发/成功/失败、周期快照发射。
- [x] 5.2 明确并落地 diff/snapshot 最小字段契约（含序列字段与时间字段）。
- [x] 5.3 清理旧“本地簿重建成功”类观测项与文档说明。

## 6. 工件与发布准备

- [x] 6.1 更新 worker 文档（README/rollout）到新链路与新 topic 约定。
- [x] 6.2 通过角色校验接口验证新 roles 可加载并满足必填约束。
- [x] 6.3 形成切换清单：下游改读新 topic、旧链路停用步骤与回滚策略（按新架构定义）。
