## 1. 作用域模型与存储改造

- [x] 1.1 设计并实现 `OrderbookScopeKey`（至少含 `role_id`、`symbol`，支持 market/exchange 扩展）
- [x] 1.2 将 `orderbook` store 从 `symbol` 全局键改为 `scope_key` 键，并保留短期兼容入口
- [x] 1.3 为 store 增加按 `role_id` 清理接口，用于 reconcile/update/remove 生命周期

## 2. Handler 与流水线接入

- [x] 2.1 在 `orderbook_diff` 初始化与消息处理路径接入 `scope_key`，确保 snapshot/diff 同作用域应用
- [x] 2.2 调整 `orderbook.Engine` 与调用链，移除对全局 symbol 共享状态的依赖
- [x] 2.3 校验 `orderbook_validator` 与 sink 前消息语义不变（topic 与 payload schema 不变）

## 3. Reconcile 一致性与可观测性

- [x] 3.1 在 role `update/remove` 提交流程中接入作用域清理，确保只影响目标 role
- [x] 3.2 为 `stale/sequence-gap/no-snapshot` 等丢弃路径增加结构化日志字段（role_id/scope_key/symbol/reason）
- [x] 3.3 增加 dropped 计数指标并接入现有监控导出路径

## 4. 测试与验收

- [x] 4.1 新增单元测试：同 symbol 不同 role 的 `BookState` 互不污染
- [x] 4.2 新增集成测试：spot/perp 同时运行时 `spot.orderbook` 与 `perp.orderbook` 持续产出
- [x] 4.3 新增回归用例：role reconcile/update 后不继承旧作用域状态
- [x] 4.4 执行验证与灰度检查，记录回滚步骤与观察项（offset 连续性、dropped 指标）
