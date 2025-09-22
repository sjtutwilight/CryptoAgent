



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
CREATE TABLE IF NOT EXISTS ch_account_balance_snapshot (
  account_id       UInt64,
  observed_time    DateTime,
  block_id         UInt64,                   -- 版本/去重
  asset_type       LowCardinality(String),   -- 'erc20'/'lp'
  biz_id           UInt64,                   -- token_id 或 pair_id
  amount           Decimal(38,18),
  price_usd        Decimal(38,18),
  value_usd        Decimal(38,18),
  label_mask       UInt16 DEFAULT 0,

  -- 常用过滤索引
  INDEX idx_account_time (account_id, observed_time) TYPE bloom_filter() GRANULARITY 1,
  INDEX idx_value_usd     (value_usd)                TYPE minmax        GRANULARITY 1,
  INDEX idx_label_mask    (label_mask)               TYPE set(100)      GRANULARITY 1,  -- 低基数用set更省

  -- 方便下游统一对齐：派生“分钟”列
  end_minute DateTime MATERIALIZED toStartOfMinute(observed_time),

  -- Projections（高频查询路径）
  PROJECTION proj_by_token
    (SELECT biz_id, asset_type, account_id, observed_time, amount, value_usd, label_mask
     ORDER BY (biz_id, observed_time, account_id)),
  PROJECTION proj_by_time
    (SELECT observed_time, biz_id, account_id, value_usd, label_mask
     ORDER BY (observed_time, biz_id))
)
ENGINE = ReplacingMergeTree(block_id)
PARTITION BY toYYYYMM(observed_time)
ORDER BY (account_id, observed_time, block_id, biz_id)
TTL observed_time + INTERVAL 30 DAY
SETTINGS index_granularity = 8192,
         deduplicate_merge_projection_mode = 'rebuild';
CREATE TABLE IF NOT EXISTS ch_token_distribution_minute_state (
  token_id  UInt64,
  end_time  DateTime,

  -- 持有人数（value_usd>0 的账户去重）
  holders_uniq_state            AggregateFunction(uniqExact, UInt64),

  -- Fresh 持有人数（位图示例：bit0=Fresh）
  fresh_holders_uniq_state      AggregateFunction(uniqExact, UInt64),

  -- 总价值 / Fresh 价值
  total_value_sum_state         AggregateFunction(sum, Decimal(38,4)),
  fresh_value_sum_state         AggregateFunction(sum, Decimal(38,4)),

  -- 中位数（精确中位数 state）
  median_value_state            AggregateFunction(quantileExact, Decimal(38,4)),

  -- 平均值（可直接 avgMerge）
  avg_value_state               AggregateFunction(avg, Decimal(38,4)),

  -- Top2（用 topKState，然后在视图里 arraySum(topKMerge(2)(…))）
  top2_value_state              AggregateFunction(topK(2), Decimal(38,4))
)
ENGINE = AggregatingMergeTree
ORDER BY (token_id, end_time)
PARTITION BY toYYYYMM(end_time)
TTL end_time + INTERVAL 30 DAY;


CREATE MATERIALIZED VIEW IF NOT EXISTS mv_dist_from_snapshot
TO ch_token_distribution_minute_state
AS
SELECT
  biz_id                    AS token_id,
  end_minute                AS end_time,

  -- holders：只统计 value_usd>0 的账户
  uniqExactStateIf(account_id, value_usd > 0)                                  AS holders_uniq_state,
  uniqExactStateIf(account_id, (bitAnd(label_mask, toUInt16(1)) != 0) AND value_usd > 0)
                                                                              AS fresh_holders_uniq_state,

  -- 总价值 / Fresh 价值
  sumState( toDecimal128(value_usd, 4) )                                       AS total_value_sum_state,
  sumStateIf( toDecimal128(value_usd, 4), (bitAnd(label_mask, toUInt16(1)) != 0) AND value_usd > 0 )
                                                                              AS fresh_value_sum_state,

  -- 中位数（对 value_usd>0 的分位数 state）
  quantileExactState(0.5)( toDecimal128(value_usd, 4) )                        AS median_value_state,

  -- 平均值（也只对正值参与）
  avgStateIf( toDecimal128(value_usd, 4), value_usd > 0 )                      AS avg_value_state,

  -- top2：topKState(2) 对 value_usd>0 参与
  topKState(2)( toDecimal128(value_usd, 4) )                                   AS top2_value_state
FROM ch_account_balance_snapshot
WHERE asset_type = 'erc20'
GROUP BY token_id, end_time;
CREATE OR REPLACE VIEW v_token_distribution_minute AS
SELECT
    token_id,
    end_time,
    uniqExactMerge(holders_uniq_state)          AS holders_count,
    toDecimal64(sumMerge(total_value_sum_state), 4)             AS total_value_usd,
    toDecimal64(quantileExactMerge(0.5)(median_value_state), 4) AS median_holder_value_usd,
    toDecimal64(avgMerge(avg_value_state), 4)                   AS avg_holder_value_usd,
    arraySum(topKMerge(2)(top2_value_state))                    AS top2_value_usd,
    if(sumMerge(total_value_sum_state) > 0,
       arraySum(topKMerge(2)(top2_value_state)) / sumMerge(total_value_sum_state),
       NULL)                                                    AS top2_share,
    if(sumMerge(total_value_sum_state) > 0,
       sumMerge(fresh_value_sum_state) / sumMerge(total_value_sum_state),
       NULL)                                                    AS fresh_holder_value_share,
    if(uniqExactMerge(holders_uniq_state) > 0,
       uniqExactMerge(fresh_holders_uniq_state) / uniqExactMerge(holders_uniq_state),
       NULL)                                                    AS fresh_holder_count_share
FROM ch_token_distribution_minute_state
GROUP BY token_id, end_time
ORDER BY token_id ASC, end_time ASC;


CREATE TABLE IF NOT EXISTS ch_token_holder_balance_minute (
  token_id     UInt64,
  end_time     DateTime,
  account_id   UInt64,
  amount       Decimal(38,18),
  value_usd    Decimal(38,18),
  label_mask   UInt16,
  version      UInt64,  -- 取 block_id 或 observed_time 的最大值

  PROJECTION by_token_time
    (SELECT token_id, end_time, account_id, value_usd, amount, label_mask
     ORDER BY (token_id, end_time, account_id)),

  PROJECTION by_account_time
    (SELECT account_id, token_id, end_time, value_usd, amount, label_mask
     ORDER BY (account_id, end_time, token_id))
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(end_time)
ORDER BY (token_id, end_time, account_id)
TTL end_time + INTERVAL 90 DAY
SETTINGS deduplicate_merge_projection_mode = 'rebuild';

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_holder_balance_minute
TO ch_token_holder_balance_minute
AS
SELECT
  biz_id                                AS token_id,
  end_minute                            AS end_time,
  account_id,
  argMax(amount,     block_id)          AS amount,
  argMax(value_usd,  block_id)          AS value_usd,
  argMax(label_mask, block_id)          AS label_mask,
  max(block_id)                         AS version
FROM ch_account_balance_snapshot
WHERE asset_type = 'erc20'
GROUP BY token_id, end_time, account_id;

CREATE OR REPLACE VIEW v_token_top_holders_latest AS
WITH last_minute AS (
  SELECT token_id, max(end_time) AS end_time
  FROM ch_token_holder_balance_minute
  GROUP BY token_id
)
SELECT
  h.token_id,
  h.end_time,
  h.account_id,
  h.value_usd,
  round(h.value_usd / nullIf(sum(h.value_usd) OVER (PARTITION BY h.token_id, h.end_time),0), 6) AS ownership_pct,
  h.amount,
  h.label_mask
FROM ch_token_holder_balance_minute h
INNER JOIN last_minute lm USING (token_id, end_time)
WHERE h.value_usd > 0
ORDER BY h.token_id, h.value_usd DESC
LIMIT 100 BY h.token_id;


---标签维度
CREATE OR REPLACE VIEW v_token_holder_tag_minute AS
WITH tags AS (
  /* 示例位图映射：bit0=fresh, bit1=whale, bit2=smart, bit3=cex */
  SELECT arrayJoin([
    ('fresh_wallet', toUInt16(1)),
    ('whale',        toUInt16(2)),
    ('smart_money',  toUInt16(4)),
    ('cex',          toUInt16(8))
  ]) AS t
),
base AS (
  SELECT
    h.token_id,
    h.end_time,
    t.1 AS tag,
    sumIf(h.value_usd, bitAnd(h.label_mask, t.2) != 0 AND h.value_usd > 0)        AS value_usd,
    uniqExactIf(h.account_id, bitAnd(h.label_mask, t.2) != 0 AND h.value_usd > 0) AS holders_count
  FROM ch_token_holder_balance_minute h
  CROSS JOIN tags
  GROUP BY h.token_id, h.end_time, tag
)
SELECT
  token_id,
  end_time,
  tag,
  value_usd,
  holders_count,
  (value_usd - lagInFrame(value_usd) OVER (PARTITION BY token_id, tag ORDER BY end_time))
    / nullIf(lagInFrame(value_usd) OVER (PARTITION BY token_id, tag ORDER BY end_time), 0) AS pct_change_1min
FROM base
ORDER BY token_id, tag, end_time;

CREATE OR REPLACE VIEW v_token_trades_detail AS
SELECT
  t.token_id,
  t.block_time,
  t.account_id,
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
  token_id,
side,
  count()        AS trade_cnt,
  sum(value_usd) AS volume_usd
FROM ch_account_trade_fact
GROUP BY end_time, account_id, token_id;
CREATE TABLE IF NOT EXISTS token_recent_metric_ch
(
    token_id UInt64,
    time_window LowCardinality(String),  -- '20s','1min','5min','1h'
    end_time DateTime,
    tag LowCardinality(String),          -- 'all','cex','smart_money','whale','fresh_wallet'
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
      (SELECT account_id, token_id, position, total_pnl_usd, roi_pct, last_tx_time
       ORDER BY (account_id, last_tx_time, token_id)),
    PROJECTION proj_by_token
      (SELECT token_id, account_id, position, total_pnl_usd, roi_pct, last_tx_time
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

CREATE TABLE IF NOT EXISTS ch_token_macro_minute_state (
  token_id UInt64,
  end_time DateTime,
  -- mcap：分钟唯一值
  mcap_max_state          AggregateFunction(max, Decimal(38,4)),
  -- realized cap 近似：Σ(position*avg_cost)
  realized_cap_sum_state  AggregateFunction(sum, Decimal(38,4)),
  -- 未实现盈亏严格拆分
  unreal_profit_sum_state AggregateFunction(sum, Decimal(38,4)),
  unreal_loss_sum_state   AggregateFunction(sum, Decimal(38,4)),
  -- SOPR/Realized PnL
  sopr_proceeds_sum_state AggregateFunction(sum, Decimal(38,4)),
  sopr_cost_sum_state     AggregateFunction(sum, Decimal(38,4)),
  realized_pnl_sum_state  AggregateFunction(sum, Decimal(38,4))
)
ENGINE = AggregatingMergeTree
ORDER BY (token_id, end_time)
TTL end_time + INTERVAL 90 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_macro_from_rt_mcap
TO ch_token_macro_minute_state AS
SELECT
  token_id,
  end_time,
  maxState(toDecimal128(mcap_usd, 4)) AS mcap_max_state,
  -- 其余占位
  sumState(toDecimal128(0, 4)) AS realized_cap_sum_state,
  sumState(toDecimal128(0, 4)) AS unreal_profit_sum_state,
  sumState(toDecimal128(0, 4)) AS unreal_loss_sum_state,
  sumState(toDecimal128(0, 4)) AS sopr_proceeds_sum_state,
  sumState(toDecimal128(0, 4)) AS sopr_cost_sum_state,
  sumState(toDecimal128(0, 4)) AS realized_pnl_sum_state
FROM token_recent_metric_ch
WHERE tag='all' AND time_window='1min' AND mcap_usd IS NOT NULL AND mcap_usd > 0
GROUP BY token_id, end_time;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_macro_from_pnl_snapshot
TO ch_token_macro_minute_state AS
SELECT
  token_id,
  toStartOfMinute(last_tx_time) AS end_time,
  -- realized cap
  sumState(toDecimal128(position * avg_cost, 4)) AS realized_cap_sum_state,
  -- 未实现盈亏严格拆分（仅有效仓/价）
  sumState(toDecimal128(CASE WHEN position > 0 AND last_price_usd > 0 AND avg_cost > 0
                               THEN greatest(position * (last_price_usd - avg_cost), 0)
                               ELSE 0 END, 4)) AS unreal_profit_sum_state,
  sumState(toDecimal128(CASE WHEN position > 0 AND last_price_usd > 0 AND avg_cost > 0
                               THEN greatest(position * (avg_cost - last_price_usd), 0)
                               ELSE 0 END, 4)) AS unreal_loss_sum_state,
  -- 其余占位
  maxState(toDecimal128(0, 4)) AS mcap_max_state,
  sumState(toDecimal128(0, 4)) AS sopr_proceeds_sum_state,
  sumState(toDecimal128(0, 4)) AS sopr_cost_sum_state,
  sumState(toDecimal128(0, 4)) AS realized_pnl_sum_state
FROM ch_account_pnl_current_ma
WHERE position > 0 AND avg_cost > 0
GROUP BY token_id, end_time;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_macro_from_realized_event
TO ch_token_macro_minute_state AS
SELECT
  token_id,
  toStartOfMinute(block_time) AS end_time,
  sumState(toDecimal128(realized_proceeds_usd, 4)) AS sopr_proceeds_sum_state,
  sumState(toDecimal128(realized_cost_usd, 4))     AS sopr_cost_sum_state,
  sumState(toDecimal128(realized_pnl_usd, 4))      AS realized_pnl_sum_state,
  -- 其余占位
  maxState(toDecimal128(0, 4)) AS mcap_max_state,
  sumState(toDecimal128(0, 4)) AS realized_cap_sum_state,
  sumState(toDecimal128(0, 4)) AS unreal_profit_sum_state,
  sumState(toDecimal128(0, 4)) AS unreal_loss_sum_state
FROM ch_pnl_realized_event
WHERE realized_qty > 0
GROUP BY token_id, end_time;

CREATE OR REPLACE VIEW v_token_macro_minute AS
SELECT
  token_id,
  end_time,
  round(maxMerge(mcap_max_state), 2)                                  AS mcap_usd,
  round(sumMerge(realized_cap_sum_state), 2)                           AS realized_cap_usd,
  round(sumMerge(realized_cap_sum_state)
      + sumMerge(unreal_profit_sum_state)
      - sumMerge(unreal_loss_sum_state), 2)                            AS network_value_usd,
  round(sumMerge(unreal_profit_sum_state), 2)                          AS unrealized_profit_usd,
  round(sumMerge(unreal_loss_sum_state), 2)                            AS unrealized_loss_usd,
  /* NUPL（严格拆分分子，分母用 network_value） */
  CASE WHEN (sumMerge(realized_cap_sum_state)
          + sumMerge(unreal_profit_sum_state)
          - sumMerge(unreal_loss_sum_state)) > 0
       THEN round((sumMerge(unreal_profit_sum_state) - sumMerge(unreal_loss_sum_state)) /
                  (sumMerge(realized_cap_sum_state)
                 + sumMerge(unreal_profit_sum_state)
                 - sumMerge(unreal_loss_sum_state)), 6)
       ELSE NULL END                                                   AS nupl,
  /* 其他指标 */
  CASE WHEN sumMerge(realized_cap_sum_state) > 0 AND maxMerge(mcap_max_state) > 0
       THEN round(maxMerge(mcap_max_state) / sumMerge(realized_cap_sum_state), 4)
       ELSE NULL END                                                   AS mvrv,
  CASE WHEN sumMerge(realized_cap_sum_state) > 0
       THEN round((sumMerge(realized_cap_sum_state)
                 + sumMerge(unreal_profit_sum_state)
                 - sumMerge(unreal_loss_sum_state)) /
                  sumMerge(realized_cap_sum_state), 4)
       ELSE NULL END                                                   AS nvt_ratio,
  CASE WHEN sumMerge(sopr_cost_sum_state) > 0
       THEN round(sumMerge(sopr_proceeds_sum_state) / sumMerge(sopr_cost_sum_state), 4)
       ELSE NULL END                                                   AS sopr,
  round(sumMerge(realized_pnl_sum_state), 2)                           AS realized_pnl_usd,
  /* 完整性标记 */
  (maxMerge(mcap_max_state) > 0)                                       AS has_mcap,
  (sumMerge(realized_cap_sum_state) > 0)                               AS has_realized_cap,
  (sumMerge(unreal_profit_sum_state) + sumMerge(unreal_loss_sum_state) > 0) AS has_unrealized_pnl,
  (sumMerge(sopr_proceeds_sum_state) > 0)                              AS has_sopr,
  now()                                                                AS last_updated
FROM ch_token_macro_minute_state
GROUP BY token_id, end_time
HAVING has_mcap OR has_realized_cap OR has_unrealized_pnl OR has_sopr
ORDER BY token_id, end_time;

CREATE OR REPLACE VIEW v_token_macro_latest AS
SELECT
  token_id,
  max(end_time) AS latest_time,
  argMax(mcap_usd, end_time) AS mcap_usd,
  argMax(realized_cap_usd, end_time) AS realized_cap_usd,
  argMax(network_value_usd, end_time) AS network_value_usd,
  argMax(nupl, end_time) AS nupl,
  argMax(mvrv, end_time) AS mvrv,
  argMax(sopr, end_time) AS sopr,
  argMax(realized_pnl_usd, end_time) AS realized_pnl_usd
FROM v_token_macro_minute
WHERE end_time >= now() - INTERVAL 1 DAY
GROUP BY token_id;
