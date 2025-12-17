-- ============================================================
-- 模块: StarRocks Paimon Catalog配置
-- 存储: StarRocks
-- 维护: Batch模块
-- 用途: 在StarRocks中创建Paimon外部Catalog，实现联邦查询
-- ============================================================

-- ========================================
-- 1. 创建 Paimon Catalog
-- ========================================
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

-- ========================================
-- 2. 查看 Catalog
-- ========================================
SHOW CATALOGS;

-- ========================================
-- 3. 切换到 Paimon Catalog
-- ========================================
SET CATALOG paimon_catalog;

-- ========================================
-- 4. 查看数据库
-- ========================================
SHOW DATABASES;

-- ========================================
-- 5. 查看表
-- ========================================
SHOW TABLES FROM crypto_analytics;

-- ========================================
-- 6. 查看表结构
-- ========================================
DESC crypto_analytics.token_holders_snapshot;

-- ========================================
-- 7. 测试查询
-- ========================================
SELECT 
    chain_id,
    token_address,
    snapshot_date,
    COUNT(*) as holder_count,
    SUM(balance_readable) as total_supply
FROM crypto_analytics.token_holders_snapshot
GROUP BY chain_id, token_address, snapshot_date;

-- ========================================
-- 8. 切换回默认 Catalog
-- ========================================
SET CATALOG default_catalog;

-- ========================================
-- 9. 验证 Paimon Catalog 连接
-- ========================================
SELECT 
    'Paimon Catalog 连接成功' as status,
    COUNT(*) as record_count
FROM paimon_catalog.crypto_analytics.token_holders_snapshot;

