-- 数据质量引擎 PostgreSQL 表结构
-- 用于存储告警记录和规则配置

-- 告警记录表
CREATE TABLE IF NOT EXISTS quality_alert_records (
    alert_id VARCHAR(36) PRIMARY KEY,
    level VARCHAR(20),
    domain VARCHAR(50),
    stream_key VARCHAR(100),
    dimension VARCHAR(50),
    rule_name VARCHAR(100),
    message VARCHAR(500),
    metric_value DOUBLE PRECISION,
    threshold DOUBLE PRECISION,
    context_json TEXT,
    alert_time TIMESTAMP WITH TIME ZONE,
    process_time TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_alert_domain ON quality_alert_records(domain);
CREATE INDEX IF NOT EXISTS idx_alert_level ON quality_alert_records(level);
CREATE INDEX IF NOT EXISTS idx_alert_time ON quality_alert_records(alert_time);
CREATE INDEX IF NOT EXISTS idx_alert_rule ON quality_alert_records(rule_name);

-- 规则配置表
CREATE TABLE IF NOT EXISTS quality_rule_configs (
    id SERIAL PRIMARY KEY,
    rule_name VARCHAR(100) NOT NULL UNIQUE,
    domain VARCHAR(50),
    dimension VARCHAR(50),
    enabled BOOLEAN DEFAULT TRUE,
    alert_level VARCHAR(20),
    config_json TEXT,
    description VARCHAR(500),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_rule_name ON quality_rule_configs(rule_name);
CREATE INDEX IF NOT EXISTS idx_rule_domain ON quality_rule_configs(domain);
CREATE INDEX IF NOT EXISTS idx_rule_enabled ON quality_rule_configs(enabled);

-- 创建更新时间触发器
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

DROP TRIGGER IF EXISTS update_rule_configs_updated_at ON quality_rule_configs;
CREATE TRIGGER update_rule_configs_updated_at
    BEFORE UPDATE ON quality_rule_configs
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- 告警统计视图
CREATE OR REPLACE VIEW v_alert_stats_hourly AS
SELECT 
    date_trunc('hour', alert_time) as hour,
    domain,
    level,
    rule_name,
    COUNT(*) as alert_count
FROM quality_alert_records
WHERE alert_time >= NOW() - INTERVAL '24 hours'
GROUP BY date_trunc('hour', alert_time), domain, level, rule_name
ORDER BY hour DESC, alert_count DESC;

-- 规则启用状态视图
CREATE OR REPLACE VIEW v_rule_status AS
SELECT 
    rule_name,
    domain,
    dimension,
    enabled,
    alert_level,
    updated_at
FROM quality_rule_configs
ORDER BY domain, rule_name;

COMMENT ON TABLE quality_alert_records IS '数据质量告警记录表';
COMMENT ON TABLE quality_rule_configs IS '数据质量规则配置表';

