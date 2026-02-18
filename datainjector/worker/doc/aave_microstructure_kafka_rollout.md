# AAVE 微观结构 Diff/Snapshot 切换说明

## 变更范围

本次发布将 AAVE orderbook 从“本地簿重建单流”切换为“diff 主流 + snapshot 辅流”。

角色清单：

- `rec-binance-perp-aave-orderbook-diff`
- `rec-binance-perp-aave-orderbook-snapshot`（`polling_interval=10`）
- `rec-binance-perp-aave-aggtrade-full`
- `rec-binance-spot-aave-orderbook-diff`
- `rec-binance-spot-aave-orderbook-snapshot`（`polling_interval=10`）
- `rec-binance-spot-aave-aggtrade-full`

## Topic 与契约

Orderbook sink 统一启用：

- `topic_field: ob_topic`
- `topic_map.diff -> *.orderbook.diff`
- `topic_map.snapshot -> *.orderbook.snapshot`

最小字段契约：

- diff: `symbol`、`exchange`、`first_update_id`、`final_update_id`、`prev_final_update_id`、`exchange_ts`、`ingest_ts`
- snapshot: `symbol`、`exchange`、`lastUpdateId`、`snapshot`、`snapshot_source`、`snapshot_reason`、`exchange_ts`、`ingest_ts`

语义约束：

- 实时深度 diff 只进入 `*.orderbook.diff`
- backfill snapshot 与 10s 周期 snapshot 都进入 `*.orderbook.snapshot`

## 发布前校验

```bash
# from DataPlatform root
docker exec datainjector-worker sh -lc '
  curl -sS -X POST http://127.0.0.1:8090/api/roles/validate \
    -H "Content-Type: application/json" \
    --data-binary @/app/configs/aave/roles_aave_full_stable.json
'
```

期望返回：

```json
{"status":"ok"}
```

## 切换清单（6.3）

1. 下游先完成订阅切换：从旧 `*.orderbook` 改为 `*.orderbook.diff + *.orderbook.snapshot`。
2. 下游状态机接入 `snapshot_source/snapshot_reason`，对 gap 窗口执行重锚。
3. 执行角色校验接口，确认 `status=ok` 后再 apply。
4. apply 后观测 30 分钟：
   - `worker_integrity_gaps_total`
   - `worker_integrity_backfill_result_total`
   - `worker_orderbook_snapshot_emitted_total{snapshot_source="periodic"}`
5. 确认无消费者读取旧 `*.orderbook` 单流后，停用旧链路依赖。

## 回滚策略（新架构内）

不回滚到旧本地簿模型。回滚仅在新架构内执行：

1. 使用 `roles_aave_full_stable_file_rollback.json` 重新 apply（保留 diff/snapshot 双流契约）。
2. 若 backfill 压力过高，先降低 diff role 的 backfill 激进参数（`eager_gap`/`max_delay_ms`）并保持 snapshot polling 在线。
3. 下游消费保持 `diff + snapshot` 不变，避免二次契约切换。

