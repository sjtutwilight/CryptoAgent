# 永续合约指标 API 使用说明

本文档说明 `/v1/perps` 系列接口的用途、请求参数和响应结构，方便前端页面及加密货币分析 / 交易 AI Agent 正确调用。

## 1. 通用信息

- **基础 URL**：`/api/v1/perps`
- **返回格式**：统一返回 `ApiResponse<T>`，字段含义：
  - `code`：状态码，`200` 表示成功。
  - `message`：中文提示信息。
  - `data`：实际数据载荷。
  - `timestamp`：服务器时间戳（毫秒）。
- **时间格式**：`LocalDateTime` 采用 `yyyy-MM-dd'T'HH:mm:ss`，无时区偏移，默认按 UTC 解析。
- **分页 / Limit**：所有 `limit` 参数都有上限，执行面序列最大 `10000` 条，分钟级序列最大 `1440` 条。

调用示例（Curl）：

```bash
curl 'http://localhost:8088/api/v1/perps/markets?page=1&pageSize=50&order=desc'
```

## 2. 市场快照

### GET `/markets`

获取每个 `(symbol, exchange)` 的最新一分钟 `dws_perps_panel_1m` 指标，用于永续合约大盘列表。

| 参数        | 类型           | 说明                                               |
|-------------|----------------|----------------------------------------------------|
| `symbols`   | string / csv   | 过滤符号，支持 `BTCUSDT,ETHUSDT` 形式多选          |
| `exchange`  | string         | 交易所过滤，如 `binance`, `hyperliquid`            |
| `algo`      | string         | 算法版本过滤（对应 `algo_version`）                |
| `page`      | int, 默认 `1`  | 页码（从 1 开始）                                  |
| `pageSize`  | int, 默认 `20` | 每页数量，最大 `500`                               |
| `sortBy`    | string         | 排序字段：`volume`(默认) / `spread` / `basis` 等   |
| `order`     | string         | `asc` 或 `desc`                                    |

返回 `PerpPanelMetric` 列表，字段示例：

```json
{
  "symbol": "BTCUSDT",
  "exchange": "binance",
  "endTime": "2025-11-06T05:29:00",
  "algoVersion": "prod",
  "avgSpreadBps": 1.25,
  "avgDepth50k": 123456.78,
  "fundingRate": 0.00012,
  "liquidityRegime": "THICK",
  "crowdingScore": 0.42,
  "processTime": "2025-11-06T05:29:05"
}
```

**前端推荐用法**：页面初始化时查询 `page=1&pageSize=100&order=desc` 并定时轮询（建议 15 秒）。点击行时将 `exchange`、`algoVersion` 作为查询参数拼接到跳转链接中。

**Agent 推荐用法**：结合 `symbols` 筛选感兴趣交易对，若需要低点差标的，可按 `sortBy=spread&order=asc` 查询。

## 3. 执行面时间序列

### GET `/{symbol}/execution`

对应 `dws_exec_1s`，返回秒级盘口与成交指标。

| 参数        | 类型    | 说明                                                |
|-------------|---------|-----------------------------------------------------|
| `exchange`  | string  | 交易所过滤（可选）                                  |
| `algo`      | string  | 算法版本过滤（可选）                                |
| `startTime` | string  | 起始时间（ISO8601，含秒），可选                     |
| `endTime`   | string  | 结束时间（ISO8601，含秒），可选                     |
| `limit`     | int     | 最大返回行数，默认 `1800`，上限 `10000`             |

返回字段示例：

```json
{
  "symbol": "BTCUSDT",
  "exchange": "binance",
  "endTime": "2025-11-06T05:28:30",
  "midPrice": 35123.45,
  "spreadBps": 1.24,
  "volumeUsd": 52340.12,
  "tradeCount": 84,
  "ofi": -0.32,
  "illiqLambda": 0.0045
}
```

**前端推荐用法**：默认 `limit=1800`（≈30 分钟），可在 “最近 2 小时” 场景下设置 `limit=7200`。如需更长区间，请配合 `startTime/endTime` 滚动请求。

**Agent 推荐用法**：在分析流动性恶化或冲击成本时，取最近 N 秒的 `spread_bps`、`impact_50k_bps` 以及 `depth_*`。对于高频异常，可使用较小 `limit` 反复轮询。

## 4. 语境面时间序列

### GET `/{symbol}/context`

对应 `dws_perps_ctx_1m`，包含资金费率、持仓量、OI 变化等分钟指标。

参数与执行面接口类似，默认 `limit=1440`（24 小时）。

返回字段重点：

- `fundingRate` / `fundingRate8h` / `fundingEma24h`
- `oi`, `oiUsd`, `oiDelta1m`, `oiDeltaPct`
- `isOiCarried`（布尔，标识是否前值填充）

**前端推荐用法**：展示资金费率曲线、OI USD 变化，细粒度时间范围可通过切换 `limit` 控制。

**Agent 推荐用法**：根据 `fundingRate` 过滤极端值（例如 `abs(fundingRate) > 0.001`），结合 `oiDeltaPct` 判断开仓/平仓浪潮。

## 5. 面板时间序列

### GET `/{symbol}/panel`

同 `context` 接口，但使用 `dws_perps_panel_1m` 聚合执行+语境指标，可用于汇合策略和得分跟踪。

关键字段：`avgSpreadBps`, `avgDepth50k`, `avgImpact50kBps`, `avgImbalance`, `sumOfi`, `crowdingScore`, `liquidityRegime` 等。

建议使用同样的 `limit` 方案与 `context` 保持一致，方便前端共用时间选择器。

## 6. 异常信号

### GET `/signals`

检索来自 `perp_signals` 的异常告警，默认按时间倒序。

| 参数        | 类型           | 说明                                        |
|-------------|----------------|---------------------------------------------|
| `symbols`   | string / csv   | 交易对过滤                                  |
| `exchanges` | string / csv   | 交易所过滤                                  |
| `types`     | string / csv   | 信号类型，如 `EXEC_HEALTH`, `CROWDING`      |
| `levels`    | string / csv   | 信号级别：`INFO` / `WARNING` / `CRITICAL`   |
| `algo`      | string         | 算法版本                                    |
| `startTime` | string         | 起始时间                                    |
| `endTime`   | string         | 结束时间                                    |
| `limit`     | int            | 返回数量，默认 `200`, 上限 `1000`           |

响应示例：

```json
{
  "symbol": "ETHUSDT",
  "exchange": "hyperliquid",
  "signalTime": "2025-11-06T05:20:00",
  "signalType": "EXEC_HEALTH",
  "signalLevel": "CRITICAL",
  "metricName": "spread_anomaly",
  "metricValue": 8.4,
  "threshold": 5.0,
  "contextJson": "{\"reason\":\"spread widened\"}"
}
```

**前端推荐用法**：列表展示最近 20 条，并在详情页中附带 `contextJson`，供分析师快速定位。

**Agent 推荐用法**：轮询 `limit=100` 最新信号，按照 `signalLevel` 排序，高优先级可触发自动提示或策略降频处理。

## 7. 常见场景建议

| 场景                             | 调用建议                                                                 | 备注                                                 |
|----------------------------------|--------------------------------------------------------------------------|------------------------------------------------------|
| 大盘列表                         | `GET /markets?page=1&pageSize=100&order=desc`                            | 每 15 秒刷新一次即可                                 |
| 查看某交易对的执行健康           | `GET /{symbol}/execution?exchange=xxx&limit=1800`                        | 观察 `spread_bps`, `impact_50k_bps`, `depth_*`       |
| 分析资金费率与持仓量趋势         | `GET /{symbol}/context?limit=1440`                                       | 绘制 24 小时曲线，关注资金费率极值与 OI 变化        |
| 监控拥挤度 / 流动性 regime 变化  | `GET /{symbol}/panel?limit=1440`                                         | 着重 `crowdingScore`, `liquidityRegime`              |
| 监听所有高危异常信号             | `GET /signals?levels=CRITICAL&limit=100`                                 | Agent 可结合 `contextJson` 给出自然语言告警         |

## 8. 错误与重试

- `code != 200` 时，`message` 包含可读错误信息。前端应提示用户或进行退化处理。
- ClickHouse 查询失败时会记录日志，建议在 Agent 侧做指数退避的重试策略。
- 当输入参数为空时，接口默认返回最近数据；若需严格时间窗，请显式传入 `startTime/endTime`。

## 9. 版本控制

当前接口默认算法版本 `algo_version = 'prod'`。后续若新增实验版本，可通过 `algo` 参数筛选。为了兼容未来扩展，请在调用时保留传参能力，即便当前使用默认值。

---

如需新增指标或返回字段，请同时更新本文件并告知前端 / Agent 维护者。
