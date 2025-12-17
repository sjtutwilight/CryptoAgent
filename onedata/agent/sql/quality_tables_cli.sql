-- 数据质量引擎 ClickHouse 表结构
-- 用于存储时序质量指标

-- 质量指标表
CREATE TABLE IF NOT EXISTS quality_metrics (
    metric_id String,
    domain LowCardinality(String),
    stream_key String,
    dimension LowCardinality(String),
    rule_name LowCardinality(String),
    
    value Float64,
    threshold Float64,
    passed UInt8,
    
    window_start DateTime64(3),
    window_end DateTime64(3),
    message_count UInt64,
    
    collected_at DateTime64(3),
    
    INDEX idx_domain domain TYPE set(100) GRANULARITY 1,
    INDEX idx_dimension dimension TYPE set(10) GRANULARITY 1,
    INDEX idx_rule rule_name TYPE set(200) GRANULARITY 1
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(collected_at)
ORDER BY (domain, stream_key, rule_name, collected_at)
TTL collected_at + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;

-- 质量指标聚合视图（按小时）
CREATE MATERIALIZED VIEW IF NOT EXISTS quality_metrics_hourly
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(hour)
ORDER BY (domain, stream_key, rule_name, hour)
AS SELECT
    domain,
    stream_key,
    rule_name,
    dimension,
    toStartOfHour(collected_at) AS hour,
    count() AS total_count,
    sum(passed) AS passed_count,
    sum(1 - passed) AS failed_count,
    avg(value) AS avg_value,
    max(value) AS max_value,
    min(value) AS min_value,
    sum(message_count) AS total_messages
FROM quality_metrics
GROUP BY domain, stream_key, rule_name, dimension, hour;

-- 质量告警表（用于快速查询，与PostgreSQL同步）
CREATE TABLE IF NOT EXISTS quality_alerts_ch (
    alert_id String,
    level LowCardinality(String),
    domain LowCardinality(String),
    stream_key String,
    dimension LowCardinality(String),
    rule_name LowCardinality(String),
    
    message String,
    metric_value Float64,
    threshold Float64,
    context_json String,
    
    alert_time DateTime64(3),
    process_time DateTime64(3),
    
    INDEX idx_level level TYPE set(5) GRANULARITY 1,
    INDEX idx_domain domain TYPE set(100) GRANULARITY 1,
    INDEX idx_rule rule_name TYPE set(200) GRANULARITY 1
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(alert_time)
ORDER BY (domain, level, alert_time)
TTL alert_time + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;

-- 流健康度视图（最近1小时）
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

-- 规则健康度视图（最近1小时）
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

-- 告警统计视图（最近24小时）
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

