-- Token分布物化视图测试和验证脚本
-- 用于验证物化视图的正确性和性能

-- ============================================================================
-- 1. 数据验证查询
-- ============================================================================

-- 检查账户余额数据量
SELECT 
    asset_type,
    count() as total_records,
    uniq(account_id) as unique_accounts,
    uniq(biz_id) as unique_tokens,
    min(observed_time) as earliest_time,
    max(observed_time) as latest_time
FROM ch_account_balance_snapshot 
GROUP BY asset_type
ORDER BY asset_type;

-- 检查每个token的最新分布情况
SELECT 
    token_id,
    argMax(snapshot_time, version) as latest_snapshot,
    argMax(holders_count, version) as holders,
    round(argMax(top2_share, version) * 100, 2) as top2_percent,
    round(argMax(fresh_holder_share, version) * 100, 2) as fresh_percent,
    round(argMax(total_value_usd, version), 2) as total_usd
FROM ch_token_distribution_snapshot 
GROUP BY token_id
ORDER BY total_usd DESC;

-- ============================================================================
-- 2. 详细分析查询
-- ============================================================================

-- 查看最近1小时的token分布变化趋势（本地5账户环境）
SELECT 
    token_id,
    snapshot_time,
    holders_count,
    round(top2_share * 100, 2) as top2_percent,
    round(fresh_holder_share * 100, 2) as fresh_percent,
    round(total_value_usd, 2) as total_usd,
    round(avg_holder_value_usd, 2) as avg_holder_usd,
    round(top2_value_usd, 2) as top2_value
FROM ch_token_distribution_snapshot 
WHERE snapshot_time >= now() - INTERVAL 1 HOUR
    AND top2_share >= 0  -- 过滤掉负数异常
ORDER BY snapshot_time DESC, total_value_usd DESC
LIMIT 20;

-- 分析特定token的持有者详情（token_id = 1为例）
SELECT 
    account_id,
    round(amount, 4) as token_amount,
    round(value_usd, 2) as value_usd,
    round(price_usd, 4) as token_price,
    label_mask,
    (label_mask & 1) != 0 as is_fresh,
    observed_time
FROM ch_account_balance_snapshot 
WHERE biz_id = 1 AND asset_type = 'ERC20' 
    AND observed_time >= now() - INTERVAL 10 MINUTE
    AND value_usd > 0
ORDER BY value_usd DESC;

-- ============================================================================
-- 3. 性能测试查询
-- ============================================================================

-- 测试物化视图查询性能
SELECT 
    count() as total_distributions,
    uniq(token_id) as unique_tokens,
    avg(holders_count) as avg_holders,
    avg(top2_share) as avg_top2_share,
    sum(total_value_usd) as total_market_value
FROM ch_token_distribution_snapshot 
WHERE snapshot_time >= now() - INTERVAL 1 HOUR;

-- 测试投影查询性能 - 按token查询
SELECT 
    biz_id as token_id,
    count() as records,
    sum(value_usd) as total_value,
    uniq(account_id) as holders
FROM ch_account_balance_snapshot 
WHERE asset_type = 'ERC20' AND value_usd > 0
    AND observed_time >= now() - INTERVAL 1 HOUR
GROUP BY biz_id
ORDER BY total_value DESC;

-- ============================================================================
-- 4. 数据质量检查
-- ============================================================================

-- 检查是否有负价值或异常数据
SELECT 
    'Negative Values' as check_type,
    count() as issues
FROM ch_account_balance_snapshot 
WHERE value_usd < 0 OR price_usd < 0 OR amount < 0

UNION ALL

SELECT 
    'Zero Price with Non-zero Amount' as check_type,
    count() as issues
FROM ch_account_balance_snapshot 
WHERE price_usd = 0 AND amount > 0

UNION ALL

SELECT 
    'Inconsistent Value Calculation' as check_type,
    count() as issues
FROM ch_account_balance_snapshot 
WHERE abs(value_usd - (amount * price_usd)) > 0.01;

-- 检查物化视图数据完整性
SELECT 
    t1.token_id,
    t1.snapshot_time,
    t1.holders_count as mv_holders,
    t2.actual_holders,
    abs(t1.holders_count - t2.actual_holders) as holder_diff
FROM ch_token_distribution_snapshot t1
JOIN (
    SELECT 
        biz_id as token_id,
        toStartOfMinute(observed_time) as snapshot_time,
        uniqExactIf(account_id, value_usd > 0) as actual_holders
    FROM ch_account_balance_snapshot 
    WHERE asset_type = 'ERC20'
        AND observed_time >= now() - INTERVAL 10 MINUTE
    GROUP BY token_id, snapshot_time
) t2 ON t1.token_id = t2.token_id AND t1.snapshot_time = t2.snapshot_time
WHERE holder_diff > 0
ORDER BY holder_diff DESC
LIMIT 10;

-- ============================================================================
-- 5. 监控查询（可用于定期检查）
-- ============================================================================

-- 最近处理的数据量统计
SELECT 
    toStartOfHour(observed_time) as hour,
    asset_type,
    count() as records,
    uniq(account_id) as accounts,
    uniq(biz_id) as tokens,
    round(sum(value_usd), 2) as total_value
FROM ch_account_balance_snapshot 
WHERE observed_time >= now() - INTERVAL 24 HOUR
GROUP BY hour, asset_type
ORDER BY hour DESC, asset_type;

-- Token分布更新频率检查
SELECT 
    token_id,
    count() as update_count,
    min(snapshot_time) as first_update,
    max(snapshot_time) as last_update,
    round(avg(total_value_usd), 2) as avg_value
FROM ch_token_distribution_snapshot 
WHERE snapshot_time >= now() - INTERVAL 2 HOUR
GROUP BY token_id
ORDER BY update_count DESC;
