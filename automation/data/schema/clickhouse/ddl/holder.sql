-- ============================================================
-- 模块: 持仓分析（Token Holder Analytics）
-- 存储: ClickHouse
-- 维护: Aggregator模块
-- 上游Topic: account_balance_snapshot
-- 关联Job: AccountBalanceJob
-- ============================================================

-- ========================================
-- 1. 账户余额快照表
-- ========================================
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
SETTINGS index_granularity = 8192, deduplicate_merge_projection_mode = 'rebuild';

-- ========================================
-- 2. Token持币地址最新余额表（仅保留最新两个snapshot）
-- ========================================
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
SETTINGS deduplicate_merge_projection_mode = 'rebuild';

-- ========================================
-- 3. 物化视图：从快照表提取最新两个snapshot的持仓数据
-- ========================================
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
GROUP BY snapshot_id, biz_id, account_id;






