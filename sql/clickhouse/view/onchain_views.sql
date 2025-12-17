-- ============================================================
-- 模块: 链上数据视图（Onchain Data Views）
-- 存储: ClickHouse
-- 维护: Aggregator模块
-- 依赖表: ch_account_trade_fact, ch_account_pnl_current_ma, ch_pnl_realized_event, token_recent_metric_ch
-- ============================================================

-- ========================================
-- 1. Token交易明细视图
-- ========================================
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

-- ========================================
-- 2. 账户交易明细视图
-- ========================================
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

-- ========================================
-- 3. Token宏观指标视图（NUPL/MVRV/SOPR）
-- ========================================
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

