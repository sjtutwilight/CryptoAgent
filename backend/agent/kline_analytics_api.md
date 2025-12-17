# K线指标 API 使用说明

本文档描述 `/v1/klines` 下新增的 K 线与技术指标接口，供前端大盘与智能 Agent 调用。接口风格与永续合约指标保持一致，均返回统一的 `ApiResponse<T>`。

## 1. 通用信息

- **基础 URL**：`/api/v1/klines`
- **返回格式**：`ApiResponse<T>`（`code` / `message` / `data` / `timestamp`）
- **时间格式**：`yyyy-MM-dd'T'HH:mm:ss`（UTC，无时区偏移）
- **分页与限制**：
  - 列表接口最大 `pageSize=500`
  - 序列接口默认 `limit=1000`，上限 `5000`
- **数据来源**：
  - `kline_metrics` —— 标准 OHLCV + 衍生均线、信号信息
  - `kline_indicator_metrics` —— 指标（RSI / MACD / BOLL / ATR 等）输出

快速连通性测试：

```bash
curl 'http://localhost:8088/api/v1/klines/markets?interval=1m&page=1&pageSize=20'
```

## 2. 大盘快照

### GET `/markets`

返回每个 `(exchange, symbol, interval)` 组合最近一根已收盘 K 线，用于大盘排行榜。支持符号、交易所、排序字段筛选。

| 参数        | 类型           | 说明                                                         |
|-------------|----------------|--------------------------------------------------------------|
| `symbols`   | string / csv   | 交易对过滤，如 `BTCUSDT,ETHUSDT`                            |
| `exchange`  | string         | 交易所过滤，例如 `binance` / `okx` / `hyperliquid`         |
| `interval`  | string         | K 线周期，默认 `1m`                                         |
| `page`      | int            | 页码（从 1 开始），默认 1                                   |
| `pageSize`  | int            | 每页数量，默认 50，最大 500                                  |
| `sortBy`    | string         | `volume`(默认) / `amplitude` / `change` / `tradeCount` 等   |
| `order`     | string         | `asc` 或 `desc`                                              |

响应示例：

```json
{
  "symbol": "BTCUSDT",
  "exchange": "binance",
  "interval": "1m",
  "startTime": "2025-01-11T08:59:00",
  "closeTime": "2025-01-11T08:59:59",
  "openPrice": 43150.12000000,
  "highPrice": 43180.55000000,
  "lowPrice": 43120.01000000,
  "closePrice": 43160.88000000,
  "quoteVolume": 18572000.12340000,
  "baseVolume": 430.56120000,
  "tradeCount": 842,
  "amplitudePct": 0.0014,
  "changePct": 0.0003,
  "maShortValue": 43155.23120000,
  "maMediumValue": 43140.77450000,
  "emaShortValue": 43158.99230000,
  "signalType": "NONE",
  "signalTimestamp": "2025-01-11T09:00:02"
}
```

**排序字段说明**：

- `volume` / `quoteVolume` —— 以 quote 成交量排序
- `baseVolume` —— 以基础资产成交量排序
- `change` —— 以涨跌幅排序
- `amplitude` —— 以振幅排序
- `tradeCount` —— 以成交笔数排序
- `close` / `open` —— 以价格排序

## 3. K 线时间序列

### GET `/{symbol}/candles`

获取指定交易对在某交易所、周期下的历史 K 线。默认返回最近 1000 根（按时间升序）。

| 参数        | 类型   | 说明                                             |
|-------------|--------|--------------------------------------------------|
| `exchange`  | string | 交易所过滤                                       |
| `interval`  | string | K 线周期，默认 `1m`                              |
| `startTime` | string | 起始时间，ISO8601                                |
| `endTime`   | string | 结束时间，ISO8601                                |
| `limit`     | int    | 返回条数，默认 `1000`，上限 `5000`               |

返回字段与 `kline_metrics` 中一致，包括 `open/high/low/close`、成交量、`MA/EMA`、信号字段等。

Agent 常见用法：筛选 `changePct`、`amplitudePct` 极值，或读取 `signalType`、`signalStrength` 进行策略触发。

## 4. 指标时间序列

### GET `/{symbol}/indicators`

查询 RSI / MACD / BOLL 等指标输出，支持一次性传入多个指标名称。

| 参数         | 类型           | 说明                                                                |
|--------------|----------------|---------------------------------------------------------------------|
| `exchange`   | string         | 交易所过滤                                                          |
| `interval`   | string         | K 线周期，默认 `1m`                                                 |
| `indicators` | string / csv   | 指标名称过滤，如 `RSI,MACD`                                         |
| `startTime`  | string         | 起始时间                                                            |
| `endTime`    | string         | 结束时间                                                            |
| `limit`      | int            | 返回条数，默认 `1000`，上限 `5000`                                  |

返回数据结构：

```json
{
  "symbol": "ETHUSDT",
  "interval": "5m",
  "indicator": "MACD",
  "variant": "fast=12_slow=26_signal=9",
  "startTime": "2025-01-11T08:55:00",
  "value": -12.345,
  "components": [
    { "name": "dif", "value": -8.12 },
    { "name": "dea", "value": -5.67 },
    { "name": "hist", "value": -2.45 }
  ],
  "thresholds": [
    { "name": "zero_line", "value": 0.0 }
  ],
  "signalType": "SELL",
  "signalStrength": 0.72,
  "extraTags": {
    "algo": "trend_v2",
    "source": "panel_joiner"
  },
  "processTime": "2025-01-11T08:55:05"
}
```

若 `extraTags` 来源为 ClickHouse `Map`，将原样返回；若驱动回落为 JSON 字符串，则在结果中以 `raw` 字段标记。

## 5. 常见调用示例

```bash
# 1. 查询 OKX 平台 1 分钟 K 线排行（按涨跌幅排序）
curl 'http://localhost:8088/api/v1/klines/markets?exchange=okx&interval=1m&sortBy=change&order=desc&page=1&pageSize=100'

# 2. 拉取 BTCUSDT 在 Hyperliquid 上最近 200 根 5m K 线
curl 'http://localhost:8088/api/v1/klines/BTCUSDT/candles?exchange=hyperliquid&interval=5m&limit=200'

# 3. 获取 ETHUSDT 1h 周期的 MACD/RSI 指标
curl 'http://localhost:8088/api/v1/klines/ETHUSDT/indicators?exchange=binance&interval=5m&indicators=MACD,RSI&limit=500'
```

## 6. 错误与重试策略

- `code != 200` 时，`message` 为中文错误提示，通常包含 ClickHouse 查询异常描述。
- 若返回空数组，多为筛选条件过滤后无数据，可放宽 `startTime` 或检查 `interval` 是否存在。
- 建议前端采用指数退避重试策略，Agent 则需要对 `signalType` / `signalStrength` 做合理阈值过滤。

---

如需扩展更多字段或新增指标，请同步更新本文档，保持前端与 Agent 的调用兼容性。
