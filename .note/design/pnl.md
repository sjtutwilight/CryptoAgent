
## 账户与分布

### 账户余额快照

```sql
CREATE TABLE IF NOT EXISTS ch_account_balance_snapshot (
  account_id       UInt64,
  observed_time    DateTime,
  block_id         UInt64,                   -- 区块号(版本列)
  asset_type       LowCardinality(String),   -- 'ERC20'/'LP'
  biz_id           UInt64,                   -- token_id 或 pair_id
  amount           Decimal(38,18),
  price_usd        Decimal(38,18),
  value_usd        Decimal(38,18),
  label_mask       UInt16 DEFAULT 0,
  -- 索引
  INDEX idx_account_time (account_id, observed_time) TYPE bloom_filter() GRANULARITY 1,
  INDEX idx_value_usd (value_usd) TYPE minmax GRANULARITY 1,
  INDEX idx_label_mask (label_mask) TYPE bloom_filter() GRANULARITY 1,
  -- Projections
  PROJECTION proj_by_token
    (SELECT biz_id, asset_type, account_id, observed_time, amount, value_usd, label_mask
     ORDER BY (biz_id, observed_time, account_id)),
  PROJECTION proj_by_time
    (SELECT observed_time, biz_id, account_id, value_usd, label_mask
     ORDER BY (observed_time, biz_id))
)
ENGINE = ReplacingMergeTree(block_id)
PARTITION BY (asset_type, toYYYYMM(observed_time))
ORDER BY (biz_id, account_id, block_id, observed_time)
TTL observed_time + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;
```

### Token 分布快照

```sql
CREATE TABLE IF NOT EXISTS ch_token_distribution_snapshot (
  token_id                 UInt64,
  snapshot_time            DateTime,
  holders_count            UInt32,
  median_holder_value_usd  Decimal(24,4),
  top2_share               Float64,
  top2_value_usd           Decimal(24,4),
  fresh_holder_share       Float64,
  fresh_value_usd          Decimal(24,4),
  total_value_usd          Decimal(24,4),
  avg_holder_value_usd     Decimal(24,4),
  version                  UInt64,
  PROJECTION proj_by_time
    (SELECT snapshot_time, token_id, holders_count, top2_share, fresh_holder_share, total_value_usd
     ORDER BY (snapshot_time, token_id))
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(snapshot_time)
ORDER BY (token_id, snapshot_time)
TTL snapshot_time + INTERVAL 30 DAY
SETTINGS index_granularity = 4096;
```

### Token 分布（1 分钟）物化视图

```sql
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_token_distribution_1min
TO ch_token_distribution_snapshot
AS
SELECT
    biz_id AS token_id,
    toStartOfMinute(observed_time) AS snapshot_time,
    uniqExactIf(account_id, value_usd > 0)                         AS holders_count,
    quantileExactIf(0.5)(value_usd, value_usd > 0)                 AS median_holder_value_usd,
    sum(value_usd)                                                 AS total_value_usd,
    avgIf(value_usd, value_usd > 0)                                AS avg_holder_value_usd,
    if(uniqExactIf(account_id, value_usd > 0) >= 2,
       toFloat64(arraySum(arraySlice(arrayReverseSort(groupArrayIf(value_usd, value_usd > 0)), 1, 2))) / nullIf(toFloat64(sum(value_usd)), 0),
       if(uniqExactIf(account_id, value_usd > 0) = 1, 1.0, 0))     AS top2_share,
    if(uniqExactIf(account_id, value_usd > 0) >= 2,
       arraySum(arraySlice(arrayReverseSort(groupArrayIf(value_usd, value_usd > 0)), 1, 2)),
       if(uniqExactIf(account_id, value_usd > 0) = 1, maxIf(value_usd, value_usd > 0), 0)) AS top2_value_usd,
    if(sum(value_usd) > 0,
       toFloat64(sumIf(value_usd, bitAnd(label_mask, toUInt16(1)) != 0 AND value_usd > 0)) / toFloat64(sum(value_usd)),
       0)                                                          AS fresh_holder_share,
    sumIf(value_usd, bitAnd(label_mask, toUInt16(1)) != 0 AND value_usd > 0) AS fresh_value_usd,
    toUnixTimestamp(max(observed_time))                            AS version
FROM ch_account_balance_snapshot
WHERE asset_type = 'ERC20' AND value_usd > 0
GROUP BY token_id, snapshot_time;
```

## 滑动窗口指标

### Token 滑动窗口指标

```sql
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
```

---

## 账户 PnL

### 账户 PnL

```sql
CREATE TABLE IF NOT EXISTS ch_account_pnl_current_ma (
    account_id  UInt64,
    token_id    UInt64,
    position    Decimal(38,18),           -- 剩余仓位
    avg_cost    Decimal(38,18),           -- 移动加权成本
    unrealized_pnl Decimal(38,18),        -- 未实现
    realized_pnl   Decimal(38,18),        -- 已实现
    total_pnl      Decimal(38,18),        -- 总盈亏
    pnl_ratio   Float64,                  -- = total_pnl / (realized_cost + position*avg_cost)
    last_price  Decimal(38,18),           -- 最新价格
    update_time DateTime DEFAULT now(),   -- 更新时间
    version     UInt64,                   -- 去重/排序
    -- 索引
    INDEX idx_account_token (account_id, token_id) TYPE bloom_filter() GRANULARITY 1,
    INDEX idx_pnl_ratio (pnl_ratio) TYPE minmax GRANULARITY 1,
    INDEX idx_total_pnl (total_pnl) TYPE minmax GRANULARITY 1,
    -- Projections
    PROJECTION proj_by_account
      (SELECT account_id, token_id, position, total_pnl, pnl_ratio, update_time
       ORDER BY (account_id, update_time DESC, token_id)),
    PROJECTION proj_by_token
      (SELECT token_id, account_id, position, total_pnl, pnl_ratio, update_time
       ORDER BY (token_id, update_time DESC, account_id))
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(update_time)
ORDER BY (account_id, token_id, update_time)
TTL update_time + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;
```

### 已实现盈亏事件

```sql
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
```

### 聚合态

```sql
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
```

### MV：来自实时 mcap（1min）

```sql
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
```

### MV：来自账户快照（RealizedCap + 未实现拆分）

```sql
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
```

### MV：来自已实现事件（SOPR / Realized PnL）

```sql
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
```

### 终端视图：宏观指标（严格拆分 NUPL）

```sql
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
```

### 最新一笔（近 1 天）便捷视图

```sql
CREATE OR REPLACE VIEW v_token_macro_latest AS
SELECT
  token_id,
  max(end_time)                                 AS latest_time,
  argMax(mcap_usd, end_time)                    AS mcap_usd,
  argMax(realized_cap_usd, end_time)            AS realized_cap_usd,
  argMax(network_value_usd, end_time)           AS network_value_usd,
  argMax(nupl, end_time)                        AS nupl,
  argMax(mvrv, end_time)                        AS mvrv,
  argMax(sopr, end_time)                        AS sopr,
  argMax(realized_pnl_usd, end_time)            AS realized_pnl_usd
FROM v_token_macro_minute
WHERE end_time >= now() - INTERVAL 1 DAY
GROUP BY token_id
ORDER BY token_id;
```
