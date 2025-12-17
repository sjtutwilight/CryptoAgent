-- ============================================================================
-- StarRocks Paimon Catalog 配置
-- 用于在 StarRocks 中创建 Paimon 外部 Catalog，实现联邦查询
-- ============================================================================

-- 1. 创建 Paimon Catalog
CREATE EXTERNAL CATALOG IF NOT EXISTS paimon_catalog
PROPERTIES (
    "type" = "paimon",
    "warehouse" = "s3://paimon-warehouse/wh",
    "aws.s3.endpoint" = "http://minio:9000",
    "aws.s3.access_key" = "admin",
    "aws.s3.secret_key" = "password123",
    "aws.s3.use_instance_profile" = "false",
    "aws.s3.enable_ssl" = "false",
    "aws.s3.enable_path_style_access" = "true"
);

-- 2. 查看 Catalog
SHOW CATALOGS;

-- 3. 切换到 Paimon Catalog
SET CATALOG paimon_catalog;

-- 4. 查看数据库
SHOW DATABASES;

-- 5. 查看表
SHOW TABLES FROM crypto_analytics;

-- 6. 查看表结构
DESC crypto_analytics.token_holders_snapshot;

-- 7. 测试查询
SELECT 
    chain_id,
    token_address,
    snapshot_date,
    COUNT(*) as holder_count,
    SUM(balance_readable) as total_supply
FROM crypto_analytics.token_holders_snapshot
GROUP BY chain_id, token_address, snapshot_date;

-- 8. 切换回默认 Catalog
SET CATALOG default_catalog;

-- ============================================================================
-- 在 StarRocks 本地创建分析数据库
-- ============================================================================

-- 创建分析数据库
CREATE DATABASE IF NOT EXISTS analytics;

-- 使用分析数据库
USE analytics;

-- ============================================================================
-- 创建物化视图或汇总表 (可选)
-- 将 Paimon 数据定期同步到 StarRocks 本地以提升查询性能
-- ============================================================================

-- 方式1: 创建物化视图 (StarRocks 3.0+)
-- CREATE MATERIALIZED VIEW token_holder_summary
-- REFRESH ASYNC EVERY(INTERVAL 1 HOUR)
-- AS
-- SELECT 
--     chain_id,
--     token_address,
--     snapshot_date,
--     COUNT(*) as total_holders,
--     SUM(balance_readable) as total_supply,
--     MAX(balance_readable) as max_balance,
--     AVG(balance_readable) as avg_balance
-- FROM paimon_catalog.crypto_analytics.token_holders_snapshot
-- GROUP BY chain_id, token_address, snapshot_date;

-- 方式2: 创建普通表并定期 INSERT INTO
-- (参考 token_holders_analytics.sql 中的 token_holder_metrics 表定义)

-- ============================================================================
-- 验证 Paimon Catalog 连接
-- ============================================================================

-- 查询 Paimon 中的数据
SELECT 
    'Paimon Catalog 连接成功' as status,
    COUNT(*) as record_count
FROM paimon_catalog.crypto_analytics.token_holders_snapshot;

