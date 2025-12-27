-- ============================================================
-- 模块: 数据质量视图（Data Quality Views）
-- 存储: ClickHouse
-- 维护: OneData模块
-- 依赖表: quality_metrics, quality_alerts_ch
-- ============================================================

-- ========================================
-- 1. 流健康度视图（最近1小时）
-- ========================================
CREATE VIEW IF NOT EXISTS v_stream_health_1h AS
SELECT
    domain,
    stream_key,
    count() AS metric_count,
    sum(passed) AS passed_count,
    sum(1 - passed) AS failed_count,
    round(sum(passed) * 100.0 / count(), 2) AS pass_rate,
    max(collected_at) AS last_check_time,
    dateDiff('second', max(collected_at), now()) AS seconds_since_last_check
FROM quality_metrics
WHERE collected_at >= now() - INTERVAL 1 HOUR
GROUP BY domain, stream_key;

-- ========================================
-- 2. 规则健康度视图（最近1小时）
-- ========================================
CREATE VIEW IF NOT EXISTS v_rule_health_1h AS
SELECT
    rule_name,
    dimension,
    count() AS check_count,
    sum(passed) AS passed_count,
    sum(1 - passed) AS failed_count,
    round(sum(passed) * 100.0 / count(), 2) AS pass_rate,
    round(avg(value), 4) AS avg_metric_value,
    round(max(value), 4) AS max_metric_value
FROM quality_metrics
WHERE collected_at >= now() - INTERVAL 1 HOUR
GROUP BY rule_name, dimension
ORDER BY failed_count DESC;

-- ========================================
-- 3. 告警统计视图（最近24小时）
-- ========================================
CREATE VIEW IF NOT EXISTS v_alert_stats_24h AS
SELECT
    domain,
    level,
    rule_name,
    count() AS alert_count,
    min(alert_time) AS first_alert,
    max(alert_time) AS last_alert
FROM quality_alerts_ch
WHERE alert_time >= now() - INTERVAL 24 HOUR
GROUP BY domain, level, rule_name
ORDER BY alert_count DESC;






