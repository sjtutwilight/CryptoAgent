



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
CREATE TABLE IF NOT EXISTS ch_account_balance_snapshot
(
    `snapshot_id` UInt64,
    `account_id` UInt64,
    `account_address` LowCardinality(String),
    `asset_type` LowCardinality(String),
    `biz_id` UInt64,
    `biz_name` String,
    `observed_time` DateTime,
    `end_minute` DateTime MATERIALIZED toStartOfMinute(observed_time),
    `block_id` UInt64,
    `amount` Decimal(38, 18),
    `price_usd` Decimal(38, 18),
    `value_usd` Decimal(38, 18),
    `label_mask` UInt16 DEFAULT 0,
  INDEX idx_account_time (account_id, observed_time) TYPE bloom_filter() GRANULARITY 1,
    INDEX idx_value_usd value_usd TYPE minmax GRANULARITY 1,
    INDEX idx_label_mask label_mask TYPE set(100) GRANULARITY 1,
  PROJECTION proj_by_token
    (
        SELECT
            snapshot_id,
            biz_id,
            biz_name,
            asset_type,
            account_id,
            account_address,
            observed_time,
            amount,
            value_usd,
            label_mask
        ORDER BY
            snapshot_id,
            biz_id,
            observed_time,
            account_id
    ),
  PROJECTION proj_by_time
    (
        SELECT
            snapshot_id,
            observed_time,
            biz_id,
            biz_name,
            account_id,
            account_address,
            value_usd,
            label_mask
        ORDER BY
            snapshot_id,
            observed_time,
            biz_id
    )
)
ENGINE = ReplacingMergeTree(block_id)
PARTITION BY toYYYYMM(observed_time)
ORDER BY (snapshot_id, account_id, asset_type, biz_id)
TTL observed_time + toIntervalDay(30)
SETTINGS index_granularity = 8192, deduplicate_merge_projection_mode = 'rebuild'



-- 简化的持仓明细表 - 只保留最新两个snapshot用于计算变化率
CREATE TABLE ch_token_holder_balance_latest (
  snapshot_id UInt64,
  token_id    UInt64,
  account_id  UInt64,
  account_address LowCardinality(String),
  amount      Decimal(38,18),
  value_usd   Decimal(38,18),
  label_mask  UInt16,
  version     UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (snapshot_id, token_id, account_id)
SETTINGS deduplicate_merge_projection_mode = 'rebuild'

-- ===========================
-- CREATE MATERIALIZED VIEWS
-- ===========================

-- 物化视图：从快照表提取最新两个snapshot的持仓数据
-- 注意：统一地址为小写，避免同一账户因地址大小写不同产生重复记录
CREATE MATERIALIZED VIEW mv_holder_balance_latest
TO ch_token_holder_balance_latest
AS
WITH latest_snapshots AS (
  SELECT DISTINCT snapshot_id
  FROM ch_account_balance_snapshot
  ORDER BY snapshot_id DESC
  LIMIT 2
)
SELECT
  snapshot_id,
  biz_id AS token_id,
  account_id,
  lower(argMax(account_address, block_id)) AS account_address,
  argMax(amount, block_id) AS amount,
  argMax(value_usd, block_id) AS value_usd,
  argMax(label_mask, block_id) AS label_mask,
  max(block_id) AS version
FROM ch_account_balance_snapshot
WHERE asset_type = 'erc20'
  AND snapshot_id IN (SELECT snapshot_id FROM latest_snapshots)
GROUP BY snapshot_id, biz_id, account_id


-- ===========================
-- CREATE VIEWS (查询视图)
-- ===========================

-- 1) Top持币地址视图（基于最新snapshot）
-- 注意：使用手动去重逻辑，避免ReplacingMergeTree后台merge延迟导致的重复数据
CREATE OR REPLACE VIEW v_token_top_holders_latest AS
WITH latest_snapshot AS (
  SELECT max(snapshot_id) AS max_snapshot_id 
  FROM ch_token_holder_balance_latest
)
SELECT
  h.token_id,
  h.account_id,
  h.account_address,
  h.value_usd,
  round(h.value_usd / nullIf(toFloat64(sum(h.value_usd) OVER (PARTITION BY h.token_id)), 0), 6) AS ownership_pct,
  h.amount,
  h.label_mask
FROM (
  SELECT 
    snapshot_id,
    token_id,
    account_id,
    lower(argMax(account_address, version)) as account_address,
    argMax(value_usd, version) as value_usd,
    argMax(amount, version) as amount,
    argMax(label_mask, version) as label_mask
  FROM ch_token_holder_balance_latest
  WHERE snapshot_id = (SELECT max_snapshot_id FROM latest_snapshot)
  GROUP BY snapshot_id, token_id, account_id
) h
INNER JOIN latest_snapshot l ON h.snapshot_id = l.max_snapshot_id
WHERE h.value_usd > 0
ORDER BY h.token_id, h.value_usd DESC
LIMIT 100 BY h.token_id;
-- 2) Token分布统计视图（基于最新snapshot直接聚合）
-- 注意：使用子查询去重，确保每个(snapshot_id, token_id, account_id)只保留最新version
CREATE OR REPLACE VIEW v_token_distribution_minute AS
WITH latest_snapshot AS (
  SELECT max(snapshot_id) AS max_snapshot_id 
  FROM ch_token_holder_balance_latest
),
deduplicated_data AS (
  SELECT 
    snapshot_id,
    token_id,
    account_id,
    argMax(value_usd, version) as value_usd,
    argMax(label_mask, version) as label_mask
  FROM ch_token_holder_balance_latest
  WHERE snapshot_id = (SELECT max_snapshot_id FROM latest_snapshot)
  GROUP BY snapshot_id, token_id, account_id
)
SELECT
    h.token_id,
    l.max_snapshot_id AS end_time,
    uniqExactIf(h.account_id, h.value_usd > 0) AS holders_count,
    sumIf(h.value_usd, h.value_usd > 0) AS total_value_usd,
    quantileExactIf(0.5)(h.value_usd, h.value_usd > 0) AS median_holder_value_usd,
    avgIf(h.value_usd, h.value_usd > 0) AS avg_holder_value_usd,
    topK(2)(h.value_usd) AS top2_values,
    arraySum(topK(2)(h.value_usd)) AS top2_value_usd,
    if(sumIf(toFloat64(h.value_usd), h.value_usd > 0) > 0,
       arraySum(topK(2)(toFloat64(h.value_usd))) / sumIf(toFloat64(h.value_usd), h.value_usd > 0),
       0) AS top2_share,
    if(sumIf(toFloat64(h.value_usd), h.value_usd > 0) > 0,
       sumIf(toFloat64(h.value_usd), h.value_usd > 0 AND bitAnd(h.label_mask, toUInt16(16)) != 0) / sumIf(toFloat64(h.value_usd), h.value_usd > 0),
       0) AS fresh_holder_value_share
FROM deduplicated_data h
INNER JOIN latest_snapshot l ON h.snapshot_id = l.max_snapshot_id
GROUP BY h.token_id, l.max_snapshot_id
ORDER BY h.token_id ASC;

-- 3) 标签分布视图（基于最新两个snapshot计算变化率）
-- 注意：使用去重逻辑，确保每个key只保留最新version
-- 标签位图定义与后端LabelBitmapUtil保持一致
CREATE OR REPLACE VIEW v_token_holder_tag_minute AS
WITH tags AS (
  SELECT arrayJoin([
    ('exchange',      toUInt16(1)),   -- Bit 0: 交易所
    ('smart_money',   toUInt16(2)),   -- Bit 1: 聪明钱
    ('whale',         toUInt16(4)),   -- Bit 2: 巨鲸
    ('public_figure', toUInt16(8)),   -- Bit 3: 公众人物
    ('fresh_wallet',  toUInt16(16)),  -- Bit 4: 新钱包
    ('top_pnl',       toUInt16(32))   -- Bit 5: Top PnL
  ]) AS t
),
latest_snapshots AS (
  SELECT DISTINCT snapshot_id
  FROM ch_token_holder_balance_latest
  ORDER BY snapshot_id DESC
  LIMIT 1,2
),
deduplicated_current AS (
  SELECT 
    snapshot_id,
    token_id,
    account_id,
    argMax(value_usd, version) as value_usd,
    argMax(label_mask, version) as label_mask
  FROM ch_token_holder_balance_latest
  WHERE snapshot_id = (SELECT max(snapshot_id) FROM latest_snapshots)
  GROUP BY snapshot_id, token_id, account_id
),
deduplicated_previous AS (
  SELECT 
    snapshot_id,
    token_id,
    account_id,
    argMax(value_usd, version) as value_usd,
    argMax(label_mask, version) as label_mask
  FROM ch_token_holder_balance_latest
  WHERE snapshot_id = (SELECT min(snapshot_id) FROM latest_snapshots)
  GROUP BY snapshot_id, token_id, account_id
),
current_data AS (
  SELECT
    h.token_id,
    t.1 AS tag,
    sumIf(h.value_usd, bitAnd(h.label_mask, t.2) != 0 AND h.value_usd > 0) AS current_value_usd,
    uniqExactIf(h.account_id, bitAnd(h.label_mask, t.2) != 0 AND h.value_usd > 0) AS current_holders_count
  FROM deduplicated_current h
  CROSS JOIN tags
  GROUP BY h.token_id, tag
),
previous_data AS (
  SELECT
    h.token_id,
    t.1 AS tag,
    sumIf(h.value_usd, bitAnd(h.label_mask, t.2) != 0 AND h.value_usd > 0) AS prev_value_usd
  FROM deduplicated_previous h
  CROSS JOIN tags
  GROUP BY h.token_id, tag
)
SELECT
  (SELECT max(snapshot_id) FROM latest_snapshots) AS end_time,
  c.token_id,
  c.tag,
  c.current_value_usd AS value_usd,
  c.current_holders_count AS holders_count,
  if(p.prev_value_usd > 0, 
     (c.current_value_usd - p.prev_value_usd) / p.prev_value_usd * 100,
     0) AS pct_change_1min
FROM current_data c
LEFT JOIN previous_data p ON c.token_id = p.token_id AND c.tag = p.tag
ORDER BY c.token_id, c.current_value_usd DESC;

CREATE OR REPLACE VIEW v_token_trades_detail AS
SELECT
  t.token_id,
  t.block_time,
  t.account_id,
  t.account_address,
  t.side,
  t.qty,
  t.price_usd,
  t.value_usd,
  t.tx_hash,
  t.log_index,
  t.pair_id,
  t.label_mask
FROM ch_account_trade_fact AS t;
-- 常见查询：WHERE token_id = ? AND block_time >= now() - INTERVAL 7 DAY
-- ORDER BY block_time DESC LIMIT 100
--example 

CREATE OR REPLACE VIEW v_account_trades_detail AS
SELECT
  t.account_id,
  t.account_address,
  t.block_time,
  t.token_id,
  t.side,
  t.qty,
  t.price_usd,
  t.value_usd,
  t.tx_hash,
  t.log_index,
  t.pair_id,
  t.label_mask
FROM ch_account_trade_fact AS t;
-- 常见查询：WHERE account_id = ? AND block_time >= now() - INTERVAL 7 DAY
-- ORDER BY block_time DESC LIMIT 100
--example

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
GROUP BY end_time, account_id, account_address, token_id,side;
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

-- 正确的宏观指标计算：确保NUPL、MVRV、SOPR等指标计算准确

CREATE OR REPLACE VIEW v_token_macro_latest AS
WITH token_pnl_stats AS (
  SELECT 
    token_id,
    max(last_tx_time) AS latest_time,
    -- 使用toFloat64转换避免Decimal精度问题
    toFloat64(sum(CASE WHEN position > 0 THEN position * avg_cost ELSE 0 END)) AS realized_cap_usd,
    toFloat64(sum(CASE WHEN unrealized_pnl_usd > 0 THEN unrealized_pnl_usd ELSE 0 END)) AS unrealized_profit_usd,
    toFloat64(sum(CASE WHEN unrealized_pnl_usd < 0 THEN abs(unrealized_pnl_usd) ELSE 0 END)) AS unrealized_loss_usd,
    toFloat64(sum(realized_pnl_usd)) AS realized_pnl_usd,
    -- 活跃账户数
    count(distinct account_id) AS active_accounts
  FROM ch_account_pnl_current_ma
  WHERE position > 0 AND avg_cost > 0 AND last_price_usd > 0
    AND last_tx_time >= now() - INTERVAL 1 DAY
  GROUP BY token_id
),
token_mcap AS (
  SELECT 
    token_id,
    toFloat64(argMax(mcap_usd, end_time)) AS current_mcap_usd
  FROM token_recent_metric_ch
  WHERE tag = 'all' AND time_window = '1min' 
    AND end_time >= now() - INTERVAL 1 HOUR
    AND mcap_usd > 0
  GROUP BY token_id
),
token_sopr AS (
  SELECT 
    token_id,
    toFloat64(sum(realized_proceeds_usd)) AS total_proceeds_usd,
    toFloat64(sum(realized_cost_usd)) AS total_cost_usd
  FROM ch_pnl_realized_event
  WHERE block_time >= now() - INTERVAL 1 DAY
    AND realized_qty > 0
  GROUP BY token_id
)
SELECT
  p.token_id AS token_id,
  p.latest_time AS latest_time,
  
  -- 基础指标
  round(p.realized_cap_usd, 2) AS realized_cap_usd,
  round(p.unrealized_profit_usd - p.unrealized_loss_usd, 2) AS net_unrealized_pnl_usd,
  round(p.unrealized_profit_usd, 2) AS unrealized_profit_usd,
  round(p.unrealized_loss_usd, 2) AS unrealized_loss_usd,
  round(p.realized_pnl_usd, 2) AS realized_pnl_usd,
  
  -- Network Value = Realized Cap + Net Unrealized PnL
  round(p.realized_cap_usd + p.unrealized_profit_usd - p.unrealized_loss_usd, 2) AS network_value_usd,
  
  -- NUPL = Net Unrealized PnL / Network Value
  CASE WHEN (p.realized_cap_usd + p.unrealized_profit_usd - p.unrealized_loss_usd) > 0
       THEN round((p.unrealized_profit_usd - p.unrealized_loss_usd) / 
                  (p.realized_cap_usd + p.unrealized_profit_usd - p.unrealized_loss_usd), 6)
       ELSE NULL END AS nupl,
  
  -- MVRV = Market Cap / Realized Cap  
  CASE WHEN p.realized_cap_usd > 0 AND m.current_mcap_usd > 0
       THEN round(m.current_mcap_usd / p.realized_cap_usd, 4)
       ELSE NULL END AS mvrv,
  
  -- SOPR = Realized Proceeds / Realized Cost
  CASE WHEN s.total_cost_usd > 0
       THEN round(s.total_proceeds_usd / s.total_cost_usd, 4)
       ELSE NULL END AS sopr,
  
  -- 市值数据
  round(COALESCE(m.current_mcap_usd, 0), 2) AS current_mcap_usd,
  
  -- 统计信息
  p.active_accounts,
  now() AS last_updated

FROM token_pnl_stats p
LEFT JOIN token_mcap m ON p.token_id = m.token_id
LEFT JOIN token_sopr s ON p.token_id = s.token_id

WHERE p.realized_cap_usd > 0 
   OR p.unrealized_profit_usd > 0 
   OR p.unrealized_loss_usd > 0
   OR m.current_mcap_usd > 0

ORDER BY p.token_id;

-- ========================================
-- 永续合约实时流数据处理表（Perpetual Contract）
-- ========================================

-- 执行面秒级表：订单簿和成交指标
CREATE TABLE IF NOT EXISTS dws_exec_1s
(
    symbol              LowCardinality(String),      -- 交易对符号（BTCUSDT）
    exchange            LowCardinality(String),      -- 交易所（binance）
    end_time            DateTime,                    -- 秒级窗口结束时间
    algo_version        LowCardinality(String),      -- 算法版本（用于A/B测试）

    -- 订单簿指标
    mid_price           Decimal(18, 8),              -- 中间价
    spread_bps          Float64,                     -- 点差（基点）
    spread_abs          Decimal(18, 8),              -- 绝对点差

    -- 深度指标（累计深度，单位：USD）
    depth_10k           Decimal(18, 2),              -- ±10k USD内的深度
    depth_50k           Decimal(18, 2),              -- ±50k USD内的深度
    depth_100k          Decimal(18, 2),              -- ±100k USD内的深度

    -- 订单簿不平衡
    imbalance_top5      Float64,                     -- 前5档不平衡 (bid-ask)/(bid+ask)
    imbalance_total     Float64,                     -- 总不平衡

    -- 冲击成本（买入X USD需要的滑点，基点）
    impact_10k_bps      Float64,                     -- 10k冲击成本
    impact_50k_bps      Float64,                     -- 50k冲击成本
    impact_100k_bps     Float64,                     -- 100k冲击成本

    -- OFI (Order Flow Imbalance) - L1版本
    ofi                 Float64,                     -- 订单流不平衡（L1版）

    -- 成交指标
    trade_count         UInt32,                      -- 成交笔数
    volume_usd          Decimal(18, 2),              -- 成交量（USD）
    vwap                Decimal(18, 8),              -- 成交均价
    buy_volume_usd      Decimal(18, 2),              -- 主动买入成交量
    sell_volume_usd     Decimal(18, 2),              -- 主动卖出成交量

    -- 流动性指标（可选）
    illiq_lambda        Float64,                     -- Amihud流动性系数 λ

    -- 元数据
    process_time        DateTime DEFAULT now(),

    INDEX idx_time (end_time) TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(end_time)
ORDER BY (symbol, exchange, end_time)
TTL end_time + INTERVAL 7 DAY
SETTINGS index_granularity = 8192;

-- 语境面分钟级表：标记价格、资金费率、持仓量
CREATE TABLE IF NOT EXISTS dws_perps_ctx_1m
(
    symbol              LowCardinality(String),      -- 交易对符号
    exchange            LowCardinality(String),      -- 交易所
    end_time            DateTime,                    -- 分钟级窗口
    algo_version        LowCardinality(String),      -- 算法版本

    -- 价格指标
    mark_price          Decimal(18, 8),              -- 标记价格
    index_price         Decimal(18, 8),              -- 指数价格
    basis_bps           Float64,                     -- 基差（基点）(mark-index)/index*10000

    -- 资金费率
    funding_rate        Decimal(18, 8),              -- 当前资金费率
    funding_rate_8h     Decimal(18, 8),              -- 8h资金费率
    funding_ema_24h     Decimal(18, 8),              -- 24h资金费率EMA（在线计算）
    next_funding_time   DateTime,                    -- 下次资金费结算时间

    -- 持仓量
    oi                  Decimal(18, 2),              -- 持仓量（张）
    oi_usd              Decimal(18, 2),              -- 持仓量（USD）
    oi_delta_1m         Decimal(18, 2),              -- 1分钟OI变化（采样差分）
    oi_delta_pct        Float64,                     -- OI变化百分比
    is_oi_carried       Boolean,                     -- OI是否为前值填充

    -- 元数据
    process_time        DateTime DEFAULT now(),

    INDEX idx_time (end_time) TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree(process_time)
PARTITION BY toYYYYMM(end_time)
ORDER BY (symbol, exchange, end_time)
TTL end_time + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;

-- 汇合面板表（Job 3用，当前预留结构）
CREATE TABLE IF NOT EXISTS dws_perps_panel_1m
(
    symbol              LowCardinality(String),
    exchange            LowCardinality(String),
    end_time            DateTime,
    algo_version        LowCardinality(String),

    -- 执行面聚合（从1s rollup）
    avg_spread_bps      Float64,
    max_spread_bps      Float64,
    avg_depth_50k       Decimal(18, 2),
    avg_impact_50k_bps  Float64,
    avg_imbalance       Float64,
    sum_ofi             Float64,
    volume_usd          Decimal(18, 2),
    trade_count         UInt32,

    -- 语境面
    mark_price          Decimal(18, 8),
    basis_bps           Float64,
    funding_rate        Decimal(18, 8),
    funding_ema_24h     Decimal(18, 8),
    oi_usd              Decimal(18, 2),
    oi_delta_1m         Decimal(18, 2),

    -- 衍生指标
    liquidity_regime    LowCardinality(String),     -- THICK/NORMAL/THIN
    crowding_score      Float64,                    -- 拥挤度得分

    -- 元数据
    process_time        DateTime DEFAULT now(),

    INDEX idx_time (end_time) TYPE minmax GRANULARITY 1,

    -- 投影：按时间查询优化
    PROJECTION by_time
    (
        SELECT end_time, symbol, avg_spread_bps, volume_usd, funding_rate, oi_usd
        ORDER BY (end_time, symbol)
    )
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(end_time)
ORDER BY (symbol, exchange, end_time)
TTL end_time + INTERVAL 90 DAY
SETTINGS index_granularity = 8192,
         deduplicate_merge_projection_mode = 'rebuild';

-- 信号表：存储所有检测到的异常信号
CREATE TABLE IF NOT EXISTS perp_signals
(
    symbol              LowCardinality(String),      -- 交易对符号
    exchange            LowCardinality(String),      -- 交易所
    signal_time         DateTime,                    -- 信号产生时间
    signal_type         LowCardinality(String),      -- EXEC_HEALTH/CROWDING/LIQUIDATION_RISK
    signal_level        LowCardinality(String),      -- INFO/WARNING/CRITICAL

    -- 信号内容
    metric_name         String,                      -- spread_anomaly/depth_thin/funding_extreme
    metric_value        Float64,                     -- 指标值
    threshold           Float64,                     -- 阈值

    -- 上下文
    context_json        String,                      -- 完整上下文JSON

    -- 元数据
    algo_version        LowCardinality(String),      -- 算法版本
    process_time        DateTime DEFAULT now(),

    INDEX idx_time (signal_time) TYPE minmax GRANULARITY 1,
    INDEX idx_type (signal_type) TYPE set(10) GRANULARITY 1
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(signal_time)
ORDER BY (symbol, signal_type, signal_time)
TTL signal_time + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;

CREATE TABLE kline_metrics
(
    exchange            LowCardinality(String),
    symbol              String,
    interval            LowCardinality(String),
    start_time          DateTime64(3, 'UTC'),
    close_time          DateTime64(3, 'UTC'),
    event_time          DateTime64(3, 'UTC'),
    is_closed           UInt8,
    ingest_time         DateTime64(3, 'UTC'),
    open_price          Decimal(18, 8),
    high_price          Decimal(18, 8),
    low_price           Decimal(18, 8),
    close_price         Decimal(18, 8),
    base_volume         Decimal(18, 8),
    quote_volume        Decimal(18, 8),
    trade_count         UInt32,
    amplitude_pct       Decimal(18, 6),
    change_pct          Decimal(18, 6),
    ma_short_period     UInt16,
    ma_short_value      Decimal(18, 8),
    ma_medium_period    UInt16,
    ma_medium_value     Decimal(18, 8),
    ma_long_period      UInt16,
    ma_long_value       Decimal(18, 8),
    ema_short_value     Nullable(Decimal(18, 8)),
    ema_long_value      Nullable(Decimal(18, 8)),
    signal_type         LowCardinality(String),
    signal_strength     Decimal(10, 6),
    signal_detail       String,
    signal_timestamp    DateTime64(3, 'UTC'),
    process_time        DateTime64(3, 'UTC'),
    create_time         DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(process_time)
PARTITION BY toDate(start_time)
ORDER BY (symbol, interval, start_time);
CREATE TABLE kline_indicator_metrics
(
    exchange        LowCardinality(String),
    symbol          String,
    interval        LowCardinality(String),
    start_time      DateTime64(3, 'UTC'),
    end_time        DateTime64(3, 'UTC'),
    indicator       LowCardinality(String),       -- RSI / MACD / BOLL / ATR / KDJ / MA 等
    variant         LowCardinality(String),       -- 具体参数，如 "period=14", "fast=12_slow=26"
    value           Float64,                      -- 若只有一个主值，如 RSI
    components      Nested (name String, val Float64),  -- 多输出组件（MACD、BOLL、KDJ）
    thresholds      Nested (name String, val Float64),  -- 例如 RSI 超买/超卖、BOLL stddev
    signal_type     Enum8('NONE'=0,'BUY'=1,'SELL'=2),
    signal_strength Float32,
    signal_detail   String,
    extra_tags      Map(LowCardinality(String), String),
    process_time    DateTime64(3, 'UTC'),
    create_time     DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(create_time)
PARTITION BY toDate(start_time)
ORDER BY (symbol, interval, indicator, start_time);