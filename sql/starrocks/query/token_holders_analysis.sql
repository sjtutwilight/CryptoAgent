-- ============================================================
-- 模块: Token Holders 分析查询
-- 存储: StarRocks
-- 维护: Batch模块
-- 用途: 通过Paimon Catalog查询和分析Token持有者数据
-- 前置条件: 确保已配置Paimon Catalog (参见 ddl/catalog.sql)
-- ============================================================

-- ========================================
-- 1. 基础查询：查看最新快照数据
-- ========================================

-- 查看表结构
DESC paimon_catalog.crypto_analytics.token_holders_snapshot;

-- 查看数据概览
SELECT 
    chain_id,
    token_address,
    snapshot_date,
    COUNT(*) as holder_count,
    SUM(balance_readable) as total_supply,
    MAX(snapshot_timestamp) as latest_snapshot
FROM paimon_catalog.crypto_analytics.token_holders_snapshot
GROUP BY chain_id, token_address, snapshot_date
ORDER BY snapshot_date DESC, chain_id, token_address;

-- ========================================
-- 2. Token 资产集中度分析 (Concentration Metrics)
-- ========================================

-- 2.1 Top N Holders 持仓占比
WITH ranked_holders AS (
    SELECT 
        chain_id,
        token_address,
        snapshot_date,
        wallet_address,
        balance_readable,
        ROW_NUMBER() OVER (
            PARTITION BY chain_id, token_address, snapshot_date 
            ORDER BY balance_readable DESC
        ) as holder_rank
    FROM paimon_catalog.crypto_analytics.token_holders_snapshot
),
total_supply AS (
    SELECT 
        chain_id,
        token_address,
        snapshot_date,
        SUM(balance_readable) as total_balance
    FROM paimon_catalog.crypto_analytics.token_holders_snapshot
    GROUP BY chain_id, token_address, snapshot_date
)
SELECT 
    rh.chain_id,
    rh.token_address,
    rh.snapshot_date,
    -- Top 1 持仓
    SUM(CASE WHEN rh.holder_rank <= 1 THEN rh.balance_readable ELSE 0 END) as top1_balance,
    SUM(CASE WHEN rh.holder_rank <= 1 THEN rh.balance_readable ELSE 0 END) / ts.total_balance * 100 as top1_percentage,
    -- Top 10 持仓
    SUM(CASE WHEN rh.holder_rank <= 10 THEN rh.balance_readable ELSE 0 END) as top10_balance,
    SUM(CASE WHEN rh.holder_rank <= 10 THEN rh.balance_readable ELSE 0 END) / ts.total_balance * 100 as top10_percentage,
    -- Top 50 持仓
    SUM(CASE WHEN rh.holder_rank <= 50 THEN rh.balance_readable ELSE 0 END) as top50_balance,
    SUM(CASE WHEN rh.holder_rank <= 50 THEN rh.balance_readable ELSE 0 END) / ts.total_balance * 100 as top50_percentage,
    -- Top 100 持仓
    SUM(CASE WHEN rh.holder_rank <= 100 THEN rh.balance_readable ELSE 0 END) as top100_balance,
    SUM(CASE WHEN rh.holder_rank <= 100 THEN rh.balance_readable ELSE 0 END) / ts.total_balance * 100 as top100_percentage,
    -- 总供应量
    ts.total_balance as total_supply
FROM ranked_holders rh
JOIN total_supply ts 
    ON rh.chain_id = ts.chain_id 
    AND rh.token_address = ts.token_address 
    AND rh.snapshot_date = ts.snapshot_date
WHERE rh.holder_rank <= 100
GROUP BY 
    rh.chain_id, 
    rh.token_address, 
    rh.snapshot_date,
    ts.total_balance
ORDER BY rh.snapshot_date DESC, rh.chain_id, rh.token_address;

-- 2.2 Gini 系数 (财富分配不平等程度)
-- Gini系数: 0表示完全平等，1表示完全不平等
WITH sorted_balances AS (
    SELECT 
        chain_id,
        token_address,
        snapshot_date,
        balance_readable,
        ROW_NUMBER() OVER (
            PARTITION BY chain_id, token_address, snapshot_date 
            ORDER BY balance_readable ASC
        ) as rank_asc,
        COUNT(*) OVER (PARTITION BY chain_id, token_address, snapshot_date) as total_holders
    FROM paimon_catalog.crypto_analytics.token_holders_snapshot
    WHERE balance_readable > 0
),
cumulative_wealth AS (
    SELECT 
        chain_id,
        token_address,
        snapshot_date,
        rank_asc,
        total_holders,
        balance_readable,
        SUM(balance_readable) OVER (
            PARTITION BY chain_id, token_address, snapshot_date 
            ORDER BY balance_readable ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) as cumulative_balance,
        SUM(balance_readable) OVER (
            PARTITION BY chain_id, token_address, snapshot_date
        ) as total_balance
    FROM sorted_balances
)
SELECT 
    chain_id,
    token_address,
    snapshot_date,
    total_holders,
    total_balance,
    -- Gini 系数计算 (简化版本)
    1 - 2 * SUM(cumulative_balance / total_balance) / total_holders as gini_coefficient
FROM cumulative_wealth
GROUP BY chain_id, token_address, snapshot_date, total_holders, total_balance
ORDER BY snapshot_date DESC, chain_id, token_address;

-- 2.3 HHI 指数 (Herfindahl-Hirschman Index - 市场集中度)
-- HHI < 1500: 低集中度
-- 1500 <= HHI < 2500: 中等集中度
-- HHI >= 2500: 高集中度
WITH holder_shares AS (
    SELECT 
        chain_id,
        token_address,
        snapshot_date,
        wallet_address,
        balance_readable,
        SUM(balance_readable) OVER (
            PARTITION BY chain_id, token_address, snapshot_date
        ) as total_balance,
        balance_readable / SUM(balance_readable) OVER (
            PARTITION BY chain_id, token_address, snapshot_date
        ) * 100 as market_share_pct
    FROM paimon_catalog.crypto_analytics.token_holders_snapshot
    WHERE balance_readable > 0
)
SELECT 
    chain_id,
    token_address,
    snapshot_date,
    COUNT(*) as holder_count,
    total_balance,
    -- HHI 指数 (市场份额百分比的平方和)
    SUM(POWER(market_share_pct, 2)) as hhi_index,
    -- 集中度等级
    CASE 
        WHEN SUM(POWER(market_share_pct, 2)) < 1500 THEN '低集中度'
        WHEN SUM(POWER(market_share_pct, 2)) < 2500 THEN '中等集中度'
        ELSE '高集中度'
    END as concentration_level
FROM holder_shares
GROUP BY chain_id, token_address, snapshot_date, total_balance
ORDER BY snapshot_date DESC, chain_id, token_address;

-- ========================================
-- 3. Top Holders 详细信息
-- ========================================

-- 3.1 Top 100 Holders 列表
SELECT 
    chain_id,
    token_address,
    snapshot_date,
    wallet_address,
    balance_readable,
    balance_readable / SUM(balance_readable) OVER (
        PARTITION BY chain_id, token_address, snapshot_date
    ) * 100 as percentage_of_supply,
    first_acquired,
    has_initiated_transfer,
    DATEDIFF(snapshot_date, DATE(first_acquired)) as holding_days,
    ROW_NUMBER() OVER (
        PARTITION BY chain_id, token_address, snapshot_date 
        ORDER BY balance_readable DESC
    ) as holder_rank
FROM paimon_catalog.crypto_analytics.token_holders_snapshot
WHERE balance_readable > 0
QUALIFY holder_rank <= 100
ORDER BY snapshot_date DESC, chain_id, token_address, holder_rank;

-- 3.2 巨鲸地址 (持仓 > 总供应量的1%)
WITH total_supply AS (
    SELECT 
        chain_id,
        token_address,
        snapshot_date,
        SUM(balance_readable) as total_balance
    FROM paimon_catalog.crypto_analytics.token_holders_snapshot
    GROUP BY chain_id, token_address, snapshot_date
)
SELECT 
    h.chain_id,
    h.token_address,
    h.snapshot_date,
    h.wallet_address,
    h.balance_readable,
    h.balance_readable / ts.total_balance * 100 as percentage_of_supply,
    h.first_acquired,
    h.has_initiated_transfer,
    CASE 
        WHEN h.has_initiated_transfer = false THEN '仅接收'
        ELSE '活跃交易'
    END as wallet_type
FROM paimon_catalog.crypto_analytics.token_holders_snapshot h
JOIN total_supply ts 
    ON h.chain_id = ts.chain_id 
    AND h.token_address = ts.token_address 
    AND h.snapshot_date = ts.snapshot_date
WHERE h.balance_readable / ts.total_balance > 0.01  -- 持仓 > 1%
ORDER BY h.snapshot_date DESC, h.chain_id, h.token_address, h.balance_readable DESC;

-- ========================================
-- 4. 持仓分布分析 (Distribution Analysis)
-- ========================================

-- 4.1 持仓区间分布
WITH balance_brackets AS (
    SELECT 
        chain_id,
        token_address,
        snapshot_date,
        wallet_address,
        balance_readable,
        CASE 
            WHEN balance_readable = 0 THEN '0: 零余额'
            WHEN balance_readable < 1 THEN '1: < 1'
            WHEN balance_readable < 10 THEN '2: 1-10'
            WHEN balance_readable < 100 THEN '3: 10-100'
            WHEN balance_readable < 1000 THEN '4: 100-1K'
            WHEN balance_readable < 10000 THEN '5: 1K-10K'
            WHEN balance_readable < 100000 THEN '6: 10K-100K'
            WHEN balance_readable < 1000000 THEN '7: 100K-1M'
            ELSE '8: > 1M'
        END as balance_bracket
    FROM paimon_catalog.crypto_analytics.token_holders_snapshot
)
SELECT 
    chain_id,
    token_address,
    snapshot_date,
    balance_bracket,
    COUNT(*) as holder_count,
    SUM(balance_readable) as total_balance,
    AVG(balance_readable) as avg_balance,
    MIN(balance_readable) as min_balance,
    MAX(balance_readable) as max_balance,
    -- 占总持有者比例
    COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (
        PARTITION BY chain_id, token_address, snapshot_date
    ) as pct_of_holders,
    -- 占总供应量比例
    SUM(balance_readable) * 100.0 / SUM(SUM(balance_readable)) OVER (
        PARTITION BY chain_id, token_address, snapshot_date
    ) as pct_of_supply
FROM balance_brackets
GROUP BY chain_id, token_address, snapshot_date, balance_bracket
ORDER BY snapshot_date DESC, chain_id, token_address, balance_bracket;

-- 4.2 持仓时长分析
SELECT 
    chain_id,
    token_address,
    snapshot_date,
    CASE 
        WHEN DATEDIFF(snapshot_date, DATE(first_acquired)) < 30 THEN '< 1个月'
        WHEN DATEDIFF(snapshot_date, DATE(first_acquired)) < 90 THEN '1-3个月'
        WHEN DATEDIFF(snapshot_date, DATE(first_acquired)) < 180 THEN '3-6个月'
        WHEN DATEDIFF(snapshot_date, DATE(first_acquired)) < 365 THEN '6-12个月'
        WHEN DATEDIFF(snapshot_date, DATE(first_acquired)) < 730 THEN '1-2年'
        ELSE '> 2年'
    END as holding_period,
    COUNT(*) as holder_count,
    SUM(balance_readable) as total_balance,
    AVG(balance_readable) as avg_balance,
    -- 占总持有者比例
    COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (
        PARTITION BY chain_id, token_address, snapshot_date
    ) as pct_of_holders,
    -- 占总供应量比例
    SUM(balance_readable) * 100.0 / SUM(SUM(balance_readable)) OVER (
        PARTITION BY chain_id, token_address, snapshot_date
    ) as pct_of_supply
FROM paimon_catalog.crypto_analytics.token_holders_snapshot
WHERE first_acquired IS NOT NULL
GROUP BY chain_id, token_address, snapshot_date, holding_period
ORDER BY snapshot_date DESC, chain_id, token_address, 
    CASE holding_period
        WHEN '< 1个月' THEN 1
        WHEN '1-3个月' THEN 2
        WHEN '3-6个月' THEN 3
        WHEN '6-12个月' THEN 4
        WHEN '1-2年' THEN 5
        ELSE 6
    END;

