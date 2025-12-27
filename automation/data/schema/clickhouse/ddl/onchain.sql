-- ============================================================
-- 模块: 链上数据处理（Onchain Data Processing）
-- 存储: ClickHouse
-- 维护: Aggregator模块
-- 上游Topic: dex_transaction
-- 关联Job: TradeFactJob, PnLAggregatorJob, TokenMetricAggregatorJob
-- ============================================================

-- ========================================
-- 1. 账户交易事实表
-- ========================================
CREATE TABLE IF NOT EXISTS ch_account_trade_fact
(
  -- 维度
  chain_id     UInt32,
  token_id     UInt64,
  account_id   UInt64,
  account_address LowCardinality(String),
  side         LowCardinality(String),      -- 'buy' | 'sell'
  pair_id      UInt64,                      -- 交易对（可做跳转/联动）
  pair_address LowCardinality(String),
  -- 业务时间
  block_time   DateTime,
  block_id     UInt64,
  -- 唯一定位
  tx_hash      String,
  log_index    UInt32,
  -- 度量
  qty          Decimal(38,18),
  price_usd    Decimal(38,18),
  value_usd    Decimal(38,18),
  -- 标签
  label_mask   UInt16 DEFAULT 0,

  -- 常见过滤/排序加速
  INDEX idx_time (block_time) TYPE minmax GRANULARITY 1,

  -- 投影：Token 页面 -> "某 token 的账户交易列表（按时间倒序展示）"
  PROJECTION by_token_time
  (
    SELECT token_id, block_time, account_id, side, qty, price_usd, value_usd, tx_hash, log_index, label_mask
    ORDER BY (token_id, block_time, log_index)
  ),

  -- 投影：Account 页面 -> "某账户的跨 token 交易列表（按时间倒序展示）"
  PROJECTION by_account_time
  (
    SELECT account_id, block_time, token_id, side, qty, price_usd, value_usd, tx_hash, log_index, label_mask
    ORDER BY (account_id, block_time, log_index)
  )
)
ENGINE = ReplacingMergeTree(block_id)              -- 用区块号做去重版本，防重放/迟到
PARTITION BY toYYYYMM(block_time)
ORDER BY (token_id, block_time, log_index, account_id)  -- Token 优先（Token 页面最常见），辅以时间与事件序
TTL block_time + INTERVAL 180 DAY
SETTINGS index_granularity = 8192,deduplicate_merge_projection_mode = 'rebuild';

-- ========================================
-- 2. 账户交易分钟聚合表
-- ========================================
CREATE TABLE IF NOT EXISTS ch_account_trade_minute
(
  end_time   DateTime,
  account_id UInt64,
  account_address LowCardinality(String),
  token_id   UInt64,
  side         LowCardinality(String),      -- 'buy' | 'sell'
  trade_cnt  UInt32,
  volume_usd Decimal(38,18)
)
ENGINE = SummingMergeTree
PARTITION BY toYYYYMM(end_time)
ORDER BY (account_id, end_time, token_id);

-- 物化视图：交易事实表 -> 分钟聚合
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_trade_to_minute
TO ch_account_trade_minute AS
SELECT
  toStartOfMinute(block_time) AS end_time,
  account_id,
  account_address,
  token_id,
  side,
  count()        AS trade_cnt,
  sum(value_usd) AS volume_usd
FROM ch_account_trade_fact
GROUP BY end_time, account_id, account_address, token_id, side;

-- ========================================
-- 3. Token 时序指标表
-- ========================================
CREATE TABLE IF NOT EXISTS token_recent_metric_ch
(
    token_id UInt64,
    time_window LowCardinality(String),  -- '20s','1min','5min','1h'
    end_time DateTime,
    tag LowCardinality(String),          -- 'all','cex','smart','whale','fresh'
    -- 计数
    txcnt UInt32,
    buy_count UInt32,
    sell_count UInt32,
    -- 金额
    volume_usd Decimal(24,4),
    buy_volume_usd Decimal(24,4),
    sell_volume_usd Decimal(24,4),
    buy_pressure_usd Decimal(24,4),
    -- 价格
    token_price_usd Decimal(24,4),
    mcap_usd Decimal(24,4),
    fdv_usd Decimal(24,4),
    liquidity_usd Decimal(24,4),
    -- 元数据
    process_time DateTime DEFAULT now(),
    create_time  DateTime DEFAULT now(),
    -- Projections
    PROJECTION by_tag
      (SELECT token_id, tag, time_window, end_time, volume_usd, buy_pressure_usd, token_price_usd
       ORDER BY (tag, token_id, end_time)),
    PROJECTION by_time_range
      (SELECT token_id, time_window, end_time, volume_usd, txcnt
       ORDER BY (end_time, token_id))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(end_time)
ORDER BY (token_id, time_window, tag, end_time)
TTL end_time + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;

-- ========================================
-- 4. 账户PnL当前状态表（移动平均成本法）
-- ========================================
CREATE TABLE IF NOT EXISTS ch_account_pnl_current_ma (
    account_id           UInt64,
    account_address      LowCardinality(String),
    token_id             UInt64,
    position             Decimal(38,18),         -- 剩余仓位
    avg_cost             Decimal(38,18),         -- 移动加权成本
    realized_cost_usd    Decimal(38,18),         -- 已实现成本累计
    realized_proceeds_usd Decimal(38,18),        -- 已实现收入累计
    realized_pnl_usd     Decimal(38,18),         -- 已实现盈亏
    last_price_usd       Decimal(38,18),         -- 最新价格
    unrealized_pnl_usd   Decimal(38,18),         -- 未实现盈亏
    total_pnl_usd        Decimal(38,18),         -- 总盈亏
    roi_pct              Float64,                -- 投资回报率（比例）
    holding_pct          Float64,                -- 持仓比例（可选指标）
    last_tx_time         DateTime,               -- 最近交易时间
    version              UInt64,                 -- 去重/排序版本
    -- 索引
    INDEX idx_account_token (account_id, token_id) TYPE bloom_filter() GRANULARITY 1,
    INDEX idx_roi (roi_pct) TYPE minmax GRANULARITY 1,
    INDEX idx_total_pnl (total_pnl_usd) TYPE minmax GRANULARITY 1,
    -- Projections
    PROJECTION proj_by_account
      (SELECT account_id, account_address, token_id, position, total_pnl_usd, roi_pct, last_tx_time
       ORDER BY (account_id, last_tx_time, token_id)),
    PROJECTION proj_by_token
      (SELECT token_id, account_id, account_address, position, total_pnl_usd, roi_pct, last_tx_time
       ORDER BY (token_id, last_tx_time, account_id))
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(last_tx_time)
ORDER BY (account_id, token_id, last_tx_time)
TTL last_tx_time + INTERVAL 90 DAY
SETTINGS index_granularity = 8192,
         deduplicate_merge_projection_mode = 'rebuild';

-- ========================================
-- 5. PnL已实现事件表
-- ========================================
CREATE TABLE IF NOT EXISTS ch_pnl_realized_event (
  token_id UInt64,
  account_id UInt64,
  block_id UInt64,
  block_time DateTime,
  realized_qty Decimal(38,18),
  realized_cost_usd Decimal(38,18),
  realized_proceeds_usd Decimal(38,18),
  realized_pnl_usd Decimal(38,18)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(block_time)
ORDER BY (token_id, block_id, account_id)
TTL block_time + INTERVAL 180 DAY;






