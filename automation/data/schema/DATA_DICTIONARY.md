# 数据字典

本文档记录项目核心表的详细字段说明、上游数据源、下游依赖关系。

---

## ch_account_trade_fact

### 基础信息
- **存储**: ClickHouse
- **引擎**: ReplacingMergeTree(block_id)
- **分区**: toYYYYMM(block_time)
- **排序键**: (token_id, block_time, log_index, account_id)
- **TTL**: 180天
- **业务域**: 链上数据处理

### 上游Topic
| Topic | 消息格式 | 写入Job |
|-------|---------|---------|
| dex_transaction | JSON | TradeFactJob |

### 字段说明
| 字段 | 类型 | 说明 | 来源 |
|-----|------|------|------|
| chain_id | UInt32 | 链ID | kafka.transaction.chainID |
| token_id | UInt64 | Token ID | enrichment(Redis) |
| account_id | UInt64 | 账户ID | enrichment(Redis) |
| account_address | LowCardinality(String) | 账户地址 | kafka.transaction.fromAddress |
| side | LowCardinality(String) | 交易方向(buy/sell) | 业务逻辑计算 |
| pair_id | UInt64 | 交易对ID | enrichment(Redis) |
| pair_address | LowCardinality(String) | 交易对地址 | kafka.event.contractAddress |
| block_time | DateTime | 区块时间 | kafka.transaction.timestamp |
| block_id | UInt64 | 区块号 | kafka.transaction.blockNumber |
| tx_hash | String | 交易哈希 | kafka.transaction.transactionHash |
| log_index | UInt32 | 日志索引 | kafka.event.logIndex |
| qty | Decimal(38,18) | 交易数量 | kafka.event.decodedArgs |
| price_usd | Decimal(38,18) | 价格(USD) | 业务逻辑计算 |
| value_usd | Decimal(38,18) | 交易金额(USD) | qty * price_usd |
| label_mask | UInt16 | 标签位图 | enrichment(Redis) |

### Projection
- **by_token_time**: 按Token查询交易列表（时间倒序）
- **by_account_time**: 按账户查询交易列表（时间倒序）

### 下游依赖
- 视图: `v_token_trades_detail`
- 视图: `v_account_trades_detail`
- 物化视图: `mv_trade_to_minute`
- API: `/api/token/{id}/trades`
- API: `/api/account/{id}/trades`

---

## ch_account_pnl_current_ma

### 基础信息
- **存储**: ClickHouse
- **引擎**: ReplacingMergeTree(version)
- **分区**: toYYYYMM(last_tx_time)
- **排序键**: (account_id, token_id, last_tx_time)
- **TTL**: 90天
- **业务域**: 链上数据处理

### 上游Topic
| Topic | 消息格式 | 写入Job |
|-------|---------|---------|
| dex_transaction | JSON | PnLAggregatorJob |

### 字段说明
| 字段 | 类型 | 说明 | 算法 |
|-----|------|------|------|
| account_id | UInt64 | 账户ID | enrichment |
| account_address | LowCardinality(String) | 账户地址 | enrichment |
| token_id | UInt64 | Token ID | enrichment |
| position | Decimal(38,18) | 剩余仓位 | 买入累加-卖出累减 |
| avg_cost | Decimal(38,18) | 移动加权平均成本 | (position*avg_cost + buy_qty*buy_price)/(position+buy_qty) |
| realized_cost_usd | Decimal(38,18) | 已实现成本累计 | sell_qty * avg_cost |
| realized_proceeds_usd | Decimal(38,18) | 已实现收入累计 | sell_qty * sell_price |
| realized_pnl_usd | Decimal(38,18) | 已实现盈亏 | realized_proceeds - realized_cost |
| last_price_usd | Decimal(38,18) | 最新价格 | broadcast(Redis) |
| unrealized_pnl_usd | Decimal(38,18) | 未实现盈亏 | position * (last_price - avg_cost) |
| total_pnl_usd | Decimal(38,18) | 总盈亏 | realized_pnl + unrealized_pnl |
| roi_pct | Float64 | 投资回报率 | total_pnl / (position * avg_cost) |
| holding_pct | Float64 | 持仓比例 | 可选指标 |
| last_tx_time | DateTime | 最近交易时间 | kafka.timestamp |
| version | UInt64 | 去重版本 | block_id |

### Projection
- **proj_by_account**: 按账户查询PnL
- **proj_by_token**: 按Token查询PnL

### 下游依赖
- 视图: `v_token_macro_latest`
- API: `/api/account/{id}/pnl`
- API: `/api/token/{id}/macro`

---

## token_recent_metric_ch

### 基础信息
- **存储**: ClickHouse
- **引擎**: MergeTree
- **分区**: toYYYYMM(end_time)
- **排序键**: (token_id, time_window, tag, end_time)
- **TTL**: 90天
- **业务域**: 链上数据处理

### 上游Topic
| Topic | 消息格式 | 写入Job |
|-------|---------|---------|
| dex_transaction | JSON | TokenMetricAggregatorJob |

### 字段说明
| 字段 | 类型 | 说明 | 聚合逻辑 |
|-----|------|------|---------|
| token_id | UInt64 | Token ID | keyBy |
| time_window | LowCardinality(String) | 时间窗口(20s/1min/5min/1h) | 层级聚合 |
| end_time | DateTime | 窗口结束时间 | window |
| tag | LowCardinality(String) | 标签(all/cex/smart/whale/fresh) | 分层统计 |
| txcnt | UInt32 | 交易笔数 | count |
| buy_count | UInt32 | 买入笔数 | countIf(side='buy') |
| sell_count | UInt32 | 卖出笔数 | countIf(side='sell') |
| volume_usd | Decimal(24,4) | 总交易量(USD) | sum(value_usd) |
| buy_volume_usd | Decimal(24,4) | 买入量(USD) | sumIf(value_usd, side='buy') |
| sell_volume_usd | Decimal(24,4) | 卖出量(USD) | sumIf(value_usd, side='sell') |
| buy_pressure_usd | Decimal(24,4) | 买压 | buy_volume - sell_volume |
| token_price_usd | Decimal(24,4) | Token价格(USD) | broadcast(Redis) |
| mcap_usd | Decimal(24,4) | 市值(USD) | broadcast(Redis) |
| fdv_usd | Decimal(24,4) | 完全稀释估值(USD) | broadcast(Redis) |
| liquidity_usd | Decimal(24,4) | 流动性(USD) | broadcast(Redis) |

### Projection
- **by_tag**: 按标签查询优化
- **by_time_range**: 按时间范围查询优化

### 下游依赖
- API: `/api/token/{id}/metrics`
- StarRocks表: `token_recent_metric_sr`

---

## ch_account_balance_snapshot

### 基础信息
- **存储**: ClickHouse
- **引擎**: ReplacingMergeTree(block_id)
- **分区**: toYYYYMM(observed_time)
- **排序键**: (snapshot_id, account_id, asset_type, biz_id)
- **TTL**: 30天
- **业务域**: 持仓分析

### 上游Topic
| Topic | 消息格式 | 写入Job |
|-------|---------|---------|
| account_balance_snapshot | JSON | AccountBalanceJob |

### 字段说明
| 字段 | 类型 | 说明 | 来源 |
|-----|------|------|------|
| snapshot_id | UInt64 | 快照ID | Go服务生成 |
| account_id | UInt64 | 账户ID | enrichment |
| account_address | LowCardinality(String) | 账户地址 | kafka.address |
| asset_type | LowCardinality(String) | 资产类型(erc20/native) | kafka.asset_type |
| biz_id | UInt64 | 业务ID(Token ID) | enrichment |
| biz_name | String | 业务名称 | enrichment |
| observed_time | DateTime | 观测时间 | kafka.timestamp |
| end_minute | DateTime | 分钟对齐时间 | toStartOfMinute(observed_time) |
| block_id | UInt64 | 区块号 | kafka.block_number |
| amount | Decimal(38,18) | 余额数量 | kafka.balance |
| price_usd | Decimal(38,18) | 价格(USD) | broadcast(Redis) |
| value_usd | Decimal(38,18) | 价值(USD) | amount * price_usd |
| label_mask | UInt16 | 标签位图 | enrichment |

### Projection
- **proj_by_token**: 按Token查询优化
- **proj_by_time**: 按时间查询优化

### 下游依赖
- 物化视图: `mv_holder_balance_latest`
- 表: `ch_token_holder_balance_latest`

---

## dws_exec_1s

### 基础信息
- **存储**: ClickHouse
- **引擎**: MergeTree
- **分区**: toYYYYMM(end_time)
- **排序键**: (symbol, exchange, end_time)
- **TTL**: 7天
- **业务域**: 永续合约分析

### 上游Topic
| Topic | 消息格式 | 写入Job |
|-------|---------|---------|
| perp.orderbook | JSON | PerpExecJob |
| perp.trades | JSON | PerpExecJob |

### 字段说明
| 字段 | 类型 | 说明 | 计算逻辑 |
|-----|------|------|---------|
| symbol | LowCardinality(String) | 交易对符号 | kafka.symbol |
| exchange | LowCardinality(String) | 交易所 | kafka.exchange |
| end_time | DateTime | 秒级窗口结束时间 | window |
| algo_version | LowCardinality(String) | 算法版本 | 配置 |
| mid_price | Decimal(18,8) | 中间价 | (best_bid + best_ask) / 2 |
| spread_bps | Float64 | 点差(基点) | (best_ask - best_bid) / mid_price * 10000 |
| spread_abs | Decimal(18,8) | 绝对点差 | best_ask - best_bid |
| depth_10k | Decimal(18,2) | ±10k USD深度 | sum(bid/ask qty in ±10k) |
| depth_50k | Decimal(18,2) | ±50k USD深度 | sum(bid/ask qty in ±50k) |
| depth_100k | Decimal(18,2) | ±100k USD深度 | sum(bid/ask qty in ±100k) |
| imbalance_top5 | Float64 | 前5档不平衡 | (bid_qty - ask_qty) / (bid_qty + ask_qty) |
| imbalance_total | Float64 | 总不平衡 | 全部档位不平衡 |
| impact_10k_bps | Float64 | 10k冲击成本(基点) | 买入10k USD的滑点 |
| impact_50k_bps | Float64 | 50k冲击成本(基点) | 买入50k USD的滑点 |
| impact_100k_bps | Float64 | 100k冲击成本(基点) | 买入100k USD的滑点 |
| ofi | Float64 | 订单流不平衡(L1) | Δbid_qty - Δask_qty |
| trade_count | UInt32 | 成交笔数 | count |
| volume_usd | Decimal(18,2) | 成交量(USD) | sum(qty * price) |
| vwap | Decimal(18,8) | 成交均价 | sum(qty * price) / sum(qty) |
| buy_volume_usd | Decimal(18,2) | 主动买入成交量 | sumIf(side='buy') |
| sell_volume_usd | Decimal(18,2) | 主动卖出成交量 | sumIf(side='sell') |
| illiq_lambda | Float64 | Amihud流动性系数 | abs(return) / volume |

### 下游依赖
- 表: `dws_perps_panel_1m` (rollup聚合)
- API: `/api/perp/{symbol}/exec`

---

## dws_perps_ctx_1m

### 基础信息
- **存储**: ClickHouse
- **引擎**: ReplacingMergeTree(process_time)
- **分区**: toYYYYMM(end_time)
- **排序键**: (symbol, exchange, end_time)
- **TTL**: 30天
- **业务域**: 永续合约分析

### 上游Topic
| Topic | 消息格式 | 写入Job |
|-------|---------|---------|
| perp.mark_index | JSON | PerpContextJob |
| perp.funding_rate | JSON | PerpContextJob |
| perp.open_interest | JSON | PerpContextJob |

### 字段说明
| 字段 | 类型 | 说明 | 计算逻辑 |
|-----|------|------|---------|
| symbol | LowCardinality(String) | 交易对符号 | kafka.symbol |
| exchange | LowCardinality(String) | 交易所 | kafka.exchange |
| end_time | DateTime | 分钟级窗口 | window |
| algo_version | LowCardinality(String) | 算法版本 | 配置 |
| mark_price | Decimal(18,8) | 标记价格 | kafka.mark_price |
| index_price | Decimal(18,8) | 指数价格 | kafka.index_price |
| basis_bps | Float64 | 基差(基点) | (mark - index) / index * 10000 |
| funding_rate | Decimal(18,8) | 当前资金费率 | kafka.funding_rate |
| funding_rate_8h | Decimal(18,8) | 8h资金费率 | funding_rate * 3 |
| funding_ema_24h | Decimal(18,8) | 24h资金费率EMA | EMA(funding_rate, 24h) |
| next_funding_time | DateTime | 下次结算时间 | kafka.next_funding_time |
| oi | Decimal(18,2) | 持仓量(张) | kafka.open_interest |
| oi_usd | Decimal(18,2) | 持仓量(USD) | oi * contract_size * mark_price |
| oi_delta_1m | Decimal(18,2) | 1分钟OI变化 | oi - prev_oi |
| oi_delta_pct | Float64 | OI变化百分比 | oi_delta / prev_oi * 100 |
| is_oi_carried | Boolean | OI是否为前值填充 | 数据缺失时填充 |

### 下游依赖
- 表: `dws_perps_panel_1m`
- API: `/api/perp/{symbol}/context`

---

## kline_metrics

### 基础信息
- **存储**: ClickHouse
- **引擎**: ReplacingMergeTree(process_time)
- **分区**: toDate(start_time)
- **排序键**: (symbol, interval, start_time)
- **业务域**: K线分析

### 上游Topic
| Topic | 消息格式 | 写入Job |
|-------|---------|---------|
| binance.kline | JSON | KlineMetricsJob |

### 字段说明
| 字段 | 类型 | 说明 | 计算逻辑 |
|-----|------|------|---------|
| exchange | LowCardinality(String) | 交易所 | kafka.exchange |
| symbol | String | 交易对符号 | kafka.symbol |
| interval | LowCardinality(String) | K线周期(1m/5m/1h) | kafka.interval |
| start_time | DateTime64(3) | K线开始时间 | kafka.start_time |
| close_time | DateTime64(3) | K线结束时间 | kafka.close_time |
| event_time | DateTime64(3) | 事件时间 | kafka.event_time |
| is_closed | UInt8 | 是否已关闭 | kafka.is_closed |
| open_price | Decimal(18,8) | 开盘价 | kafka.open |
| high_price | Decimal(18,8) | 最高价 | kafka.high |
| low_price | Decimal(18,8) | 最低价 | kafka.low |
| close_price | Decimal(18,8) | 收盘价 | kafka.close |
| base_volume | Decimal(18,8) | 基础货币成交量 | kafka.volume |
| quote_volume | Decimal(18,8) | 计价货币成交量 | kafka.quote_volume |
| trade_count | UInt32 | 成交笔数 | kafka.trade_count |
| amplitude_pct | Decimal(18,6) | 振幅(%) | (high - low) / open * 100 |
| change_pct | Decimal(18,6) | 涨跌幅(%) | (close - open) / open * 100 |
| ma_short_value | Decimal(18,8) | 短期MA | avg(close, short_period) |
| ma_medium_value | Decimal(18,8) | 中期MA | avg(close, medium_period) |
| ma_long_value | Decimal(18,8) | 长期MA | avg(close, long_period) |
| ema_short_value | Decimal(18,8) | 短期EMA | EMA(close, short_period) |
| ema_long_value | Decimal(18,8) | 长期EMA | EMA(close, long_period) |
| signal_type | LowCardinality(String) | 信号类型 | 金叉/死叉/超买/超卖 |
| signal_strength | Decimal(10,6) | 信号强度 | 0-1 |

### 下游依赖
- API: `/api/kline/{symbol}/metrics`

---

## tasks

### 基础信息
- **存储**: PostgreSQL
- **表类型**: 普通表
- **索引**: status, data_source_id, scheduled_time, priority
- **业务域**: 控制面服务

### 上游Topic
| Topic | 消息格式 | 写入方式 |
|-------|---------|---------|
| http.tasks | JSON | REST API创建 |

### 字段说明
| 字段 | 类型 | 说明 | 用途 |
|-----|------|------|------|
| id | BIGSERIAL | 主键ID | 自增 |
| task_id | VARCHAR(64) | 任务唯一标识 | UUID |
| data_source_id | VARCHAR(64) | 数据源ID | 限流分组 |
| status | VARCHAR(16) | 任务状态 | PENDING/PROCESSING/SUCCESS/RETRY/FAILED |
| retry_count | INTEGER | 当前重试次数 | 重试控制 |
| max_retry_count | INTEGER | 最大重试次数 | 重试上限 |
| task_type | VARCHAR(64) | 任务类型 | http_jsonrpc等 |
| payload | JSONB | 任务载荷 | 请求参数 |
| metadata | JSONB | 元数据 | 缺失区间、检测时间 |
| scheduled_time | TIMESTAMP | 计划执行时间 | 延时调度 |
| started_at | TIMESTAMP | 实际开始时间 | 性能监控 |
| completed_at | TIMESTAMP | 完成时间 | 性能监控 |
| status_code | INTEGER | HTTP状态码 | 响应码 |
| message | TEXT | 错误消息 | 调试信息 |
| duration_ms | BIGINT | 执行耗时(毫秒) | 性能监控 |
| data_size | INTEGER | 数据大小(字节) | 流量监控 |
| cost | INTEGER | 成本权重 | 限流计算 |
| priority | INTEGER | 优先级 | 调度顺序 |

### 下游依赖
- Kafka Topic: `tasks.status` (状态上报)
- Worker: 任务消费

---

## quality_metrics

### 基础信息
- **存储**: ClickHouse
- **引擎**: MergeTree
- **分区**: toYYYYMMDD(collected_at)
- **排序键**: (domain, stream_key, rule_name, collected_at)
- **TTL**: 30天
- **业务域**: 数据质量引擎

### 上游Topic
| Topic | 消息格式 | 写入Job |
|-------|---------|---------|
| quality.metrics | JSON | QualityMonitorJob |

### 字段说明
| 字段 | 类型 | 说明 | 用途 |
|-----|------|------|------|
| metric_id | String | 指标ID | UUID |
| domain | LowCardinality(String) | 业务域 | onchain/perp/kline |
| stream_key | String | 流标识 | topic/table名称 |
| dimension | LowCardinality(String) | 维度 | completeness/timeliness/accuracy |
| rule_name | LowCardinality(String) | 规则名称 | gap_detection/delay_check |
| value | Float64 | 指标值 | 实际测量值 |
| threshold | Float64 | 阈值 | 告警阈值 |
| passed | UInt8 | 是否通过 | 0/1 |
| window_start | DateTime64(3) | 窗口开始时间 | 检测窗口 |
| window_end | DateTime64(3) | 窗口结束时间 | 检测窗口 |
| message_count | UInt64 | 消息数量 | 窗口内消息数 |
| collected_at | DateTime64(3) | 采集时间 | 指标生成时间 |

### 下游依赖
- 物化视图: `quality_metrics_hourly`
- 视图: `v_stream_health_1h`
- 视图: `v_rule_health_1h`
- API: `/api/quality/metrics`

---

## 更新日志

- 2025-12-17: 初始版本，覆盖10张核心表
