-- ============================================================
-- 模块: 持仓分析视图（Token Holder Views）
-- 存储: ClickHouse
-- 维护: Aggregator模块
-- 依赖表: ch_token_holder_balance_latest
-- ============================================================

-- ========================================
-- 1. Top持币地址视图（基于最新snapshot）
-- ========================================
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

-- ========================================
-- 2. Token分布统计视图（基于最新snapshot直接聚合）
-- ========================================
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

-- ========================================
-- 3. 标签分布视图（基于最新两个snapshot计算变化率）
-- ========================================
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

