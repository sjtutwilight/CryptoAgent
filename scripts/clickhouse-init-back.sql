-- ClickHouse 初始化脚本
-- 创建链聚合器需要的表结构

-- ============================================================================
-- 1. 账户余额快照表（支持价格增强）
-- ============================================================================

CREATE TABLE IF NOT EXISTS ch_account_balance_snapshot (
  account_id       UInt64,
  observed_time    DateTime,
  block_id         UInt64,                   -- blockchain block number  
  asset_type       LowCardinality(String),   -- 'ERC20'/'LP'
  biz_id           UInt64,                   -- token_id 或 pair_id
  amount           Decimal(38,18),           -- 余额数量
  price_usd        Decimal(38,18),           -- USD价格（来自价格广播流）
  value_usd        Decimal(38,18),           -- USD总价值
  label_mask       UInt16 DEFAULT 0          -- 位图标签
) ENGINE = ReplacingMergeTree(block_id)        -- 使用block_id作为版本字段去重
PARTITION BY (asset_type, toYYYYMM(observed_time))  -- 按资产类型和月份分区
ORDER BY (biz_id, account_id, block_id, observed_time)
SETTINGS index_granularity = 8192;

-- 账户余额表索引优化
ALTER TABLE ch_account_balance_snapshot 
ADD INDEX IF NOT EXISTS idx_account_time (account_id, observed_time) 
TYPE bloom_filter() GRANULARITY 1;

ALTER TABLE ch_account_balance_snapshot 
ADD INDEX IF NOT EXISTS idx_value_usd (value_usd) 
TYPE minmax GRANULARITY 1;

ALTER TABLE ch_account_balance_snapshot 
ADD INDEX IF NOT EXISTS idx_label_mask (label_mask) 
TYPE bloom_filter() GRANULARITY 1;

-- ============================================================================
-- 2. Token分布快照表
-- ============================================================================

CREATE TABLE IF NOT EXISTS ch_token_distribution_snapshot (
  token_id                 UInt64,
  snapshot_time            DateTime,
  holders_count            UInt32,              -- 持有人数量
  median_holder_value_usd  Decimal(24,4),       -- 中位数持有价值
  top2_share               Float64,             -- Top2持有者占比（适配本地5个账户）
  top2_value_usd           Decimal(24,4),       -- Top2总价值
  fresh_holder_share       Float64,             -- Fresh账户占比
  fresh_value_usd          Decimal(24,4),       -- Fresh账户总价值
  total_value_usd          Decimal(24,4),       -- 总价值
  avg_holder_value_usd     Decimal(24,4),       -- 平均持有价值
  version                  UInt64               -- 版本号用于去重
) ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(snapshot_time)
ORDER BY (token_id, snapshot_time)
SETTINGS index_granularity = 4096;

-- ============================================================================
-- 3. Token分布物化视图 - 每分钟聚合
-- ============================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_token_distribution_1min
TO ch_token_distribution_snapshot
AS
SELECT
    biz_id AS token_id,
    toStartOfMinute(observed_time) AS snapshot_time,

    -- 基础统计
    uniqExactIf(account_id, value_usd > 0)        AS holders_count,
    quantileExactIf(0.5)(value_usd, value_usd > 0) AS median_holder_value_usd,
    sum(value_usd)                                AS total_value_usd,
    avgIf(value_usd, value_usd > 0)               AS avg_holder_value_usd,

    -- Top2计算：安全处理只有1个持有者的情况，使用Float64避免Decimal精度问题
    if(
        uniqExactIf(account_id, value_usd > 0) >= 2,
        toFloat64(arraySum(arraySlice(arrayReverseSort(groupArrayIf(value_usd, value_usd > 0)), 1, 2))) / nullIf(toFloat64(sum(value_usd)), 0),
        if(
            uniqExactIf(account_id, value_usd > 0) = 1,
            1.0,  -- 只有1个持有者时，top2占比就是100%
            0
        )
    ) AS top2_share,
    
    if(
        uniqExactIf(account_id, value_usd > 0) >= 2,
        arraySum(arraySlice(arrayReverseSort(groupArrayIf(value_usd, value_usd > 0)), 1, 2)),
        if(
            uniqExactIf(account_id, value_usd > 0) = 1,
            maxIf(value_usd, value_usd > 0),
            0
        )
    ) AS top2_value_usd,

    -- Fresh 占比与金额，使用Float64避免精度问题
    if(
        sum(value_usd) > 0,
        toFloat64(sumIf(value_usd, bitAnd(label_mask, toUInt16(1)) != 0 AND value_usd > 0)) / toFloat64(sum(value_usd)),
        0
    ) AS fresh_holder_share,
    
    sumIf(value_usd, bitAnd(label_mask, toUInt16(1)) != 0 AND value_usd > 0) AS fresh_value_usd,

    -- 版本列：使用时间戳确保唯一性
    toUnixTimestamp(max(observed_time)) AS version

FROM ch_account_balance_snapshot
WHERE asset_type = 'ERC20' AND value_usd > 0
GROUP BY token_id, snapshot_time;

-- ============================================================================
-- 4. Token滑动窗口指标表（替换token_recent_metric）
-- ============================================================================
CREATE TABLE IF NOT EXISTS token_recent_metric_ch
(
    token_id UInt64,
    time_window LowCardinality(String),  -- '20s','1min','5min','1h'
    end_time DateTime,
    tag LowCardinality(String),          -- 'all','cex','smart_money','whale','fresh_wallet'
    
    -- 计数指标
    txcnt UInt32,
    buy_count UInt32,
    sell_count UInt32,
    
    -- 金额指标
    volume_usd Decimal(24,4),
    buy_volume_usd Decimal(24,4),
    sell_volume_usd Decimal(24,4),
    buy_pressure_usd Decimal(24,4),
    
    -- 价格信息
    token_price_usd Decimal(24,4),
          mcap_usd Decimal(24,4),           
    fdv_usd Decimal(24,4),        
    liquidity_usd Decimal(24,4),
    -- 元数据
    process_time DateTime DEFAULT now(),
    create_time DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(end_time)
ORDER BY (token_id, time_window, tag, end_time)
TTL end_time + INTERVAL 90 DAY  -- 数据保留90天
SETTINGS index_granularity = 8192;


-- 4. 创建Projections优化查询性能

-- Token Recent Metrics - 按标签查询优化
ALTER TABLE token_recent_metric_ch 
ADD PROJECTION IF NOT EXISTS by_tag
(
    SELECT token_id, tag, time_window, end_time, volume_usd, buy_pressure_usd, token_price_usd
    ORDER BY (tag, token_id, end_time)
);

-- Token Recent Metrics - 按时间范围查询优化
ALTER TABLE token_recent_metric_ch 
ADD PROJECTION IF NOT EXISTS by_time_range
(
    SELECT token_id, time_window, end_time, volume_usd, txcnt
    ORDER BY (end_time, token_id)
);

-- 物化所有Projections
ALTER TABLE token_recent_metric_ch MATERIALIZE PROJECTION by_tag;
ALTER TABLE token_recent_metric_ch MATERIALIZE PROJECTION by_time_range;

-- ============================================================================
-- 5. 账户余额和Token分布表的投影优化
-- ============================================================================

-- 账户余额表 - 按token查询优化
ALTER TABLE ch_account_balance_snapshot 
ADD PROJECTION IF NOT EXISTS proj_by_token (
    SELECT biz_id, asset_type, account_id, observed_time, amount, value_usd, label_mask
    ORDER BY (biz_id, observed_time, account_id)
);

-- 账户余额表 - 按时间范围查询优化
ALTER TABLE ch_account_balance_snapshot 
ADD PROJECTION IF NOT EXISTS proj_by_time (
    SELECT observed_time, biz_id, account_id, value_usd, label_mask
    ORDER BY (observed_time, biz_id)
);

-- Token分布表 - 按时间查询优化
ALTER TABLE ch_token_distribution_snapshot
ADD PROJECTION IF NOT EXISTS proj_by_time (
    SELECT snapshot_time, token_id, holders_count, top2_share, fresh_holder_share, total_value_usd
    ORDER BY (snapshot_time, token_id)
);

-- 物化账户余额和Token分布相关投影
ALTER TABLE ch_account_balance_snapshot MATERIALIZE PROJECTION proj_by_token;
ALTER TABLE ch_account_balance_snapshot MATERIALIZE PROJECTION proj_by_time;
ALTER TABLE ch_token_distribution_snapshot MATERIALIZE PROJECTION proj_by_time;

-- ============================================================================
-- 6. 账户PnL当前移动平均表（接收Kafka数据）
-- ============================================================================

CREATE TABLE IF NOT EXISTS ch_account_pnl_current_ma (
    account_id       UInt64,
    token_id         UInt64,
    position         Decimal(38,18),           -- 当前剩余仓位数量
    avg_cost         Decimal(38,18),           -- 移动加权平均成本价
    unrealized_pnl   Decimal(38,18),           -- 未实现盈亏
    realized_pnl     Decimal(38,18),           -- 已实现盈亏
    total_pnl        Decimal(38,18),           -- 总盈亏 = unrealized + realized
    pnl_ratio        Float64,                  -- 盈亏比例
    last_price       Decimal(38,18),           -- 最新价格（用于计算未实现盈亏）
    update_time      DateTime DEFAULT now(),   -- 更新时间
    version          UInt64                    -- 版本号用于去重和排序
) ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(update_time)
ORDER BY (account_id, token_id, update_time)
SETTINGS index_granularity = 8192;

-- PnL表索引优化
ALTER TABLE ch_account_pnl_current_ma 
ADD INDEX IF NOT EXISTS idx_account_token (account_id, token_id) 
TYPE bloom_filter() GRANULARITY 1;

ALTER TABLE ch_account_pnl_current_ma 
ADD INDEX IF NOT EXISTS idx_pnl_ratio (pnl_ratio) 
TYPE minmax GRANULARITY 1;

ALTER TABLE ch_account_pnl_current_ma 
ADD INDEX IF NOT EXISTS idx_total_pnl (total_pnl) 
TYPE minmax GRANULARITY 1;

-- PnL投影优化 - 按账户查询
ALTER TABLE ch_account_pnl_current_ma 
ADD PROJECTION IF NOT EXISTS proj_by_account (
    SELECT account_id, token_id, position, total_pnl, pnl_ratio, update_time
    ORDER BY (account_id, update_time DESC, token_id)
);

-- PnL投影优化 - 按Token查询 
ALTER TABLE ch_account_pnl_current_ma 
ADD PROJECTION IF NOT EXISTS proj_by_token (
    SELECT token_id, account_id, position, total_pnl, pnl_ratio, update_time
    ORDER BY (token_id, update_time DESC, account_id)
);

-- 物化PnL投影
ALTER TABLE ch_account_pnl_current_ma MATERIALIZE PROJECTION proj_by_account;
ALTER TABLE ch_account_pnl_current_ma MATERIALIZE PROJECTION proj_by_token;

-- ============================================================================  
-- 7. Kafka引擎表（用于消费PnL数据）
-- ============================================================================

CREATE TABLE IF NOT EXISTS kafka_pnl_account_snapshot (
    account_id            UInt64,
    token_id              UInt64,
    position              Decimal(38,18),
    avg_cost              Decimal(38,18),
    realized_cost_usd     Decimal(38,18),
    realized_proceeds_usd Decimal(38,18),
    realized_pnl_usd      Decimal(38,18),
    last_price_usd        Decimal(38,18),
    unrealized_pnl_usd    Decimal(38,18),
    total_pnl_usd         Decimal(38,18),
    roi_pct               Float64,
    holding_pct           Float64,
    last_tx_time          DateTime,
    version               UInt64
) ENGINE = Kafka()
SETTINGS 
    kafka_broker_list = 'localhost:9092',
    kafka_topic_list = 'pnl.account_snapshot',
    kafka_group_name = 'clickhouse_pnl_consumer',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 2,
    kafka_max_block_size = 1048576;

-- 创建物化视图将Kafka数据写入主表（字段映射）
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_kafka_pnl_to_main
TO ch_account_pnl_current_ma
AS SELECT 
    account_id,
    token_id,
    position,
    avg_cost,
    unrealized_pnl_usd AS unrealized_pnl,
    realized_pnl_usd AS realized_pnl,
    total_pnl_usd AS total_pnl,
    roi_pct AS pnl_ratio,
    last_price_usd AS last_price,
    last_tx_time AS update_time,
    version
FROM kafka_pnl_account_snapshot;

-- ============================================================================
-- 8. 数据清理策略（TTL设置）
-- ============================================================================

-- 账户余额数据保留30天
ALTER TABLE ch_account_balance_snapshot 
MODIFY TTL observed_time + INTERVAL 30 DAY;

-- Token分布数据保留30天
ALTER TABLE ch_token_distribution_snapshot 
MODIFY TTL snapshot_time + INTERVAL 30 DAY;

-- PnL数据保留90天（更长保留期用于分析）
ALTER TABLE ch_account_pnl_current_ma 
MODIFY TTL update_time + INTERVAL 90 DAY;

-- ============================================================================
-- 9. 已实现盈亏事件表
-- ============================================================================

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
ORDER BY (token_id, block_id, account_id);

-- 已实现盈亏事件数据保留180天（交易记录保留更长）
ALTER TABLE ch_pnl_realized_event 
MODIFY TTL block_time + INTERVAL 180 DAY;

-- ============================================================================
-- 10. Token宏观指标系统（NUPL, MVRV, SOPR等）
-- ============================================================================

-- 10.1 宏观指标聚合态表
CREATE TABLE IF NOT EXISTS ch_token_macro_minute_state (
  token_id UInt64,
  end_time DateTime,

  -- 市值（分钟唯一值）
  mcap_max_state                AggregateFunction(max, Decimal(38,4)),

  -- 成本基准（≈ RealizedCap 近似：Σ(position * avg_cost)）
  realized_cap_sum_state        AggregateFunction(sum, Decimal(38,4)),

  -- 严格拆分的未实现盈亏
  unreal_profit_sum_state       AggregateFunction(sum, Decimal(38,4)),
  unreal_loss_sum_state         AggregateFunction(sum, Decimal(38,4)),

  -- SOPR / 已实现盈亏
  sopr_proceeds_sum_state       AggregateFunction(sum, Decimal(38,4)),
  sopr_cost_sum_state           AggregateFunction(sum, Decimal(38,4)),
  realized_pnl_sum_state        AggregateFunction(sum, Decimal(38,4))
)
ENGINE = AggregatingMergeTree
ORDER BY (token_id, end_time)
TTL end_time + INTERVAL 90 DAY;

-- 10.2 市值数据物化视图
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_macro_from_rt_mcap
TO ch_token_macro_minute_state AS
SELECT
  token_id,
  end_time,
  maxState( toDecimal128(mcap_usd, 4) ) AS mcap_max_state,

  -- 其他列占位为 0（保持聚合态列一致）
  sumState( toDecimal128(0, 4) ) AS realized_cap_sum_state,
  sumState( toDecimal128(0, 4) ) AS unreal_profit_sum_state,
  sumState( toDecimal128(0, 4) ) AS unreal_loss_sum_state,
  sumState( toDecimal128(0, 4) ) AS sopr_proceeds_sum_state,
  sumState( toDecimal128(0, 4) ) AS sopr_cost_sum_state,
  sumState( toDecimal128(0, 4) ) AS realized_pnl_sum_state
FROM token_recent_metric_ch
WHERE tag='all' AND time_window='1min' AND mcap_usd IS NOT NULL AND mcap_usd > 0
GROUP BY token_id, end_time;

-- 10.3 账户快照物化视图（RealizedCap + 未实现盈亏拆分）
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_macro_from_pnl_snapshot
TO ch_token_macro_minute_state AS
SELECT
  token_id,
  toStartOfMinute(last_tx_time) AS end_time,

  -- 成本基准：Σ(position * avg_cost) - 只统计有意义的持仓
  sumState( toDecimal128(position * avg_cost, 4) ) AS realized_cap_sum_state,

  -- 严格拆分的未实现盈亏 - 只统计有价格且有持仓的记录
  sumState( toDecimal128( 
    CASE WHEN position > 0 AND last_price_usd > 0 AND avg_cost > 0
         THEN greatest( position * (last_price_usd - avg_cost), 0 )
         ELSE 0 END, 4) ) AS unreal_profit_sum_state,
  sumState( toDecimal128( 
    CASE WHEN position > 0 AND last_price_usd > 0 AND avg_cost > 0
         THEN greatest( position * (avg_cost - last_price_usd), 0 )
         ELSE 0 END, 4) ) AS unreal_loss_sum_state,

  -- 其他列占位
  maxState( toDecimal128(0, 4) ) AS mcap_max_state,
  sumState( toDecimal128(0, 4) ) AS sopr_proceeds_sum_state,
  sumState( toDecimal128(0, 4) ) AS sopr_cost_sum_state,
  sumState( toDecimal128(0, 4) ) AS realized_pnl_sum_state
FROM ch_account_pnl_current_ma
WHERE position > 0 AND avg_cost > 0  -- 过滤掉无效数据
GROUP BY token_id, end_time;

-- 10.4 已实现事件物化视图（SOPR + Realized PnL）
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_macro_from_realized_event
TO ch_token_macro_minute_state AS
SELECT
  token_id,
  toStartOfMinute(block_time) AS end_time,

  -- SOPR 分子/分母
  sumState( toDecimal128(realized_proceeds_usd, 4) ) AS sopr_proceeds_sum_state,
  sumState( toDecimal128(realized_cost_usd, 4) )     AS sopr_cost_sum_state,

  -- Realized PnL
  sumState( toDecimal128(realized_pnl_usd, 4) )      AS realized_pnl_sum_state,

  -- 其他列占位
  maxState( toDecimal128(0, 4) ) AS mcap_max_state,
  sumState( toDecimal128(0, 4) ) AS realized_cap_sum_state,
  sumState( toDecimal128(0, 4) ) AS unreal_profit_sum_state,
  sumState( toDecimal128(0, 4) ) AS unreal_loss_sum_state
FROM ch_pnl_realized_event
WHERE realized_qty > 0
GROUP BY token_id, end_time;

-- 10.5 最终宏观指标查询视图（优化版）
CREATE OR REPLACE VIEW v_token_macro_minute AS
SELECT
  token_id,
  end_time,
  round(maxMerge(mcap_max_state), 2) as mcap_usd,
  round(sumMerge(realized_cap_sum_state), 2) as realized_cap_usd,
  round(sumMerge(realized_cap_sum_state) + sumMerge(unreal_profit_sum_state) - sumMerge(unreal_loss_sum_state), 2) as network_value_usd,
  round(sumMerge(unreal_profit_sum_state), 2) as unrealized_profit_usd,
  round(sumMerge(unreal_loss_sum_state), 2) as unrealized_loss_usd,

  /* 修正版 NUPL：使用网络价值作为分母，添加有效性检查 */
  CASE 
    WHEN (sumMerge(realized_cap_sum_state) + sumMerge(unreal_profit_sum_state) - sumMerge(unreal_loss_sum_state)) > 0
    THEN round((sumMerge(unreal_profit_sum_state) - sumMerge(unreal_loss_sum_state)) / 
              (sumMerge(realized_cap_sum_state) + sumMerge(unreal_profit_sum_state) - sumMerge(unreal_loss_sum_state)), 6)
    ELSE NULL 
  END AS nupl,

  /* 其他宏观指标 - 添加有效性检查 */
  CASE 
    WHEN sumMerge(realized_cap_sum_state) > 0 AND maxMerge(mcap_max_state) > 0
    THEN round(maxMerge(mcap_max_state) / sumMerge(realized_cap_sum_state), 4)
    ELSE NULL 
  END AS mvrv,
  
  CASE 
    WHEN sumMerge(realized_cap_sum_state) > 0
    THEN round((sumMerge(realized_cap_sum_state) + sumMerge(unreal_profit_sum_state) - sumMerge(unreal_loss_sum_state)) / 
              sumMerge(realized_cap_sum_state), 4)
    ELSE NULL 
  END AS nvt_ratio,
  
  CASE 
    WHEN sumMerge(sopr_cost_sum_state) > 0
    THEN round(sumMerge(sopr_proceeds_sum_state) / sumMerge(sopr_cost_sum_state), 4)
    ELSE NULL 
  END AS sopr,
  
  round(sumMerge(realized_pnl_sum_state), 2) as realized_pnl_usd,
  
  /* 数据完整性指标 */
  (maxMerge(mcap_max_state) > 0) AS has_mcap,
  (sumMerge(realized_cap_sum_state) > 0) AS has_realized_cap,
  (sumMerge(unreal_profit_sum_state) + sumMerge(unreal_loss_sum_state) > 0) AS has_unrealized_pnl,
  (sumMerge(sopr_proceeds_sum_state) > 0) AS has_sopr,
  now() AS last_updated
FROM ch_token_macro_minute_state
GROUP BY token_id, end_time
HAVING has_mcap OR has_realized_cap OR has_unrealized_pnl OR has_sopr
ORDER BY token_id, end_time;

-- 10.6 常用查询视图（简化版）
CREATE OR REPLACE VIEW v_token_macro_latest AS
SELECT
  token_id,
  max(end_time) as latest_time,
  argMax(mcap_usd, end_time) as mcap_usd,
  argMax(realized_cap_usd, end_time) as realized_cap_usd,
  argMax(network_value_usd, end_time) as network_value_usd,
  argMax(nupl, end_time) as nupl,
  argMax(mvrv, end_time) as mvrv,
  argMax(sopr, end_time) as sopr,
  argMax(realized_pnl_usd, end_time) as realized_pnl_usd
FROM v_token_macro_minute
WHERE end_time >= now() - INTERVAL 1 DAY
GROUP BY token_id
ORDER BY token_id;
