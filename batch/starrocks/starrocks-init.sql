-- StarRocks 初始化脚本
-- 创建 analytics 数据库和 token_recent_metric_sr 表

CREATE DATABASE IF NOT EXISTS analytics;

USE analytics;

-- Token 时序指标表 (对齐 ClickHouse token_recent_metric_ch 结构)
-- 注意: StarRocks DEFAULT 语法与 MySQL 略有不同
CREATE TABLE IF NOT EXISTS token_recent_metric_sr (
    token_id BIGINT NOT NULL COMMENT 'Token ID',
    time_window VARCHAR(20) NOT NULL COMMENT '时间窗口类型: 20s/1min/5min/1h',
    end_time DATETIME NOT NULL COMMENT '窗口结束时间',
    tag VARCHAR(50) NOT NULL COMMENT '标签: all/cex/smart/whale/fresh/public',
    txcnt BIGINT COMMENT '交易笔数',
    buy_count BIGINT COMMENT '买入笔数',
    sell_count BIGINT COMMENT '卖出笔数',
    volume_usd DECIMAL(38, 18) COMMENT '总交易量 (USD)',
    buy_volume_usd DECIMAL(38, 18) COMMENT '买入量 (USD)',
    sell_volume_usd DECIMAL(38, 18) COMMENT '卖出量 (USD)',
    buy_pressure_usd DECIMAL(38, 18) COMMENT '买压 = 买入量 - 卖出量',
    token_price_usd DECIMAL(38, 18) COMMENT 'Token 价格 (USD)',
    mcap_usd DECIMAL(38, 18) COMMENT '市值 (USD)',
    fdv_usd DECIMAL(38, 18) COMMENT '完全稀释估值 (USD)',
    liquidity_usd DECIMAL(38, 18) COMMENT '流动性 (USD)',
    process_time DATETIME COMMENT '处理时间',
    create_time DATETIME COMMENT '创建时间'
)
ENGINE = OLAP
DUPLICATE KEY(token_id, time_window, end_time, tag)
DISTRIBUTED BY HASH(token_id) BUCKETS 4
PROPERTIES (
    "replication_num" = "1"
);

-- 验证表创建成功
SHOW TABLES;
DESC token_recent_metric_sr;
