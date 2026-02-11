# AAVE 完整性测试（Spot + Perp）

本目录已支持用 `mockDataProvider` 做 Binance 风格完整性回归测试：

- WebSocket 订阅端点：`ws://localhost:8090/ws/binance`
- 单流端点（兼容旧方式）：`ws://localhost:8090/ws/binance/{stream}`
- 快照端点（Perp）：`http://localhost:8090/fapi/v1/depth`
- 快照端点（Spot）：`http://localhost:8090/api/v3/depth`

## 1. 启动无故障基线

```bash
cd DataPlatform/datainjector/mockDataProvider
go run . configs/integrity_clean.yaml
```

## 2. 启动有缺口场景（每4条丢1条）

```bash
cd DataPlatform/datainjector/mockDataProvider
go run . configs/integrity_gap.yaml
```

`integrity_gap.yaml` 里使用：

- `fault.websocket.data_loss_every_n: 4`

这样可以稳定复现序列缺口，便于验证完整性处理链。

## 3. 让 worker 用 mock 源（基于 /tmp/roles_aave_full_stable.json）

把 4 个 role 的 websocket URL 改为：

- `ws://localhost:8090/ws/binance`

并把 orderbook 的 backfill endpoint 分别保持为：

- Perp: `http://localhost:8090/fapi/v1/depth`
- Spot: `http://localhost:8090/api/v3/depth`

## 4. 建议：给 aggTrade role 也加 integrity handler

当前你的 `aggtrade` role 只有 `binance_aggtrade` handler。若要验证 aggTrade 缺口恢复，建议在其后追加：

```json
{
  "type": "integrity",
  "with": {
    "profile": "binance_trades",
    "sequence_field": "agg_trade_id",
    "stream_key_field": "symbol",
    "eager_gap": 3,
    "max_gap": 20,
    "max_delay_ms": 500,
    "hard_timeout_ms": 2000
  }
}
```

## 5. 自动化自测（mockDataProvider 内置）

```bash
cd DataPlatform/datainjector/mockDataProvider
go test ./... -run TestBinanceSubscribeStream
```

覆盖点：

- 无故障时：depth / aggTrade 序列连续
- 开启 `data_loss_every_n` 时：可稳定检测到 depth 序列缺口
