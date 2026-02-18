# worker-orderbook-state-isolation 验证与灰度记录

日期：2026-02-15

## 1. 本地验证

已执行：

```bash
go test ./internal/resource/orderbook -count=1
go test ./internal/handler -count=1
go test ./internal/role -count=1
```

结果：全部通过。

覆盖点：
- `role_id + symbol (+market/exchange)` 作用域隔离
- 同 symbol 不同 role 状态互不污染
- role update 仅清理目标 role 作用域
- dropped 指标在 `no_snapshot` 路径可观测
- payload schema 未引入破坏性字段变更（`scope_key` 仅在 metadata）

## 2. 灰度检查步骤

建议按以下顺序执行：

1. 单实例灰度启动新版本 worker（仅包含目标 role，例如 `spot.orderbook`）。
2. 观察 10~15 分钟：
   - `pipeline.finish` 持续出现
   - `worker_orderbook_dropped_total` 按 `role_id/scope_key/reason` 可见
   - 目标 topic offset 连续（无长期停滞）
3. 增加到双 role 并发（`spot + perp` 同 symbol），重复观察。
4. 全量发布前确认：
   - 非目标 role 无中断
   - dropped 指标无持续异常上升

## 3. 观察项

重点关注：
- offset 连续性：`spot.orderbook` 与 `perp.orderbook` 均持续推进
- dropped 指标：按 reason（`no_snapshot/stale/sequence_gap/not_applied`）聚合
- 日志字段完整性：`role_id/scope_key/symbol/reason`

## 4. 回滚步骤

当出现以下任一情况建议回滚：
- 目标 role 持续有输入无产出
- `worker_orderbook_dropped_total` 持续异常攀升
- 非目标 role 受到影响

回滚执行：

1. 回滚本次变更代码（恢复到上一个稳定版本）。
2. 重启 worker 进程，确保内存中的 scope store 被清空。
3. 复核 topic offset 恢复推进后，停止灰度并进入根因分析。
