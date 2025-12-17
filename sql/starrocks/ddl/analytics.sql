-- ============================================================
-- 模块: StarRocks 分析数据库
-- 存储: StarRocks
-- 维护: Batch模块
-- 用途: 创建分析数据库和汇总表
-- ============================================================

-- ========================================
-- 1. 创建分析数据库
-- ========================================
CREATE DATABASE IF NOT EXISTS analytics;

USE analytics;

-- ========================================
-- 2. Token 时序指标表 (对齐 ClickHouse token_recent_metric_ch 结构)
-- ========================================
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

-- ========================================
-- 3. Token持有者指标汇总表
-- ========================================
CREATE TABLE IF NOT EXISTS token_holder_metrics (
    chain_id INT NOT NULL COMMENT '链ID',
    token_address VARCHAR(42) NOT NULL COMMENT 'Token地址',
    snapshot_date DATE NOT NULL COMMENT '快照日期',
    
    -- 基础指标
    total_holders BIGINT COMMENT '总持有者数',
    total_supply DECIMAL(38, 18) COMMENT '总供应量',
    
    -- Top Holders 集中度
    top1_balance DECIMAL(38, 18) COMMENT 'Top1持仓',
    top1_pct DECIMAL(10, 4) COMMENT 'Top1占比(%)',
    top10_balance DECIMAL(38, 18) COMMENT 'Top10持仓',
    top10_pct DECIMAL(10, 4) COMMENT 'Top10占比(%)',
    top50_balance DECIMAL(38, 18) COMMENT 'Top50持仓',
    top50_pct DECIMAL(10, 4) COMMENT 'Top50占比(%)',
    top100_balance DECIMAL(38, 18) COMMENT 'Top100持仓',
    top100_pct DECIMAL(10, 4) COMMENT 'Top100占比(%)',
    
    -- 集中度指标
    gini_coefficient DECIMAL(10, 6) COMMENT 'Gini系数',
    hhi_index DECIMAL(15, 4) COMMENT 'HHI指数',
    concentration_level VARCHAR(20) COMMENT '集中度等级',
    
    -- 巨鲸指标
    whale_count INT COMMENT '巨鲸数量(>1%)',
    whale_total_balance DECIMAL(38, 18) COMMENT '巨鲸总持仓',
    whale_pct DECIMAL(10, 4) COMMENT '巨鲸占比(%)',
    
    -- 时间戳
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间'
) ENGINE=OLAP
DUPLICATE KEY(chain_id, token_address, snapshot_date)
COMMENT 'Token持有者指标汇总表'
DISTRIBUTED BY HASH(chain_id, token_address) BUCKETS 10
PROPERTIES (
    "replication_num" = "1",
    "storage_format" = "DEFAULT"
);

-- ========================================
-- 4. 验证表创建成功
-- ========================================
SHOW TABLES;
DESC token_recent_metric_sr;
DESC token_holder_metrics;

