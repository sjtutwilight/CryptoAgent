-- 在 StarRocks 上创建 Paimon External Catalog 并查询验证
-- 若你已有同名 Catalog，可调整名字或先 DROP

-- 1) 创建 External Catalog（Paimon + MinIO/S3）
CREATE EXTERNAL CATALOG paimon_fs
PROPERTIES (
  "type" = "paimon",
  "paimon.catalog.type" = "filesystem",
  "paimon.catalog.warehouse" = "s3://paimon-warehouse/wh",

  -- MinIO 访问配置（与你的环境保持一致）
  "aws.s3.endpoint" = "http://minio:9000",
  "aws.s3.access_key" = "admin",
  "aws.s3.secret_key" = "password123",
  "aws.s3.enable_path_style_access" = "true"
);

-- 2) 基础检查
SHOW DATABASES FROM paimon_fs;
USE paimon_fs.demo_db;
SHOW TABLES;

-- 3) 查询 append-only 表
SELECT chain_id,
       COUNT(*) AS cnt,
       MIN(block_number) AS bmin,
       MAX(block_number) AS bmax
FROM paimon_fs.lake_bronze.tx_logs
WHERE dt='2023-12-01'
GROUP BY chain_id
ORDER BY cnt DESC
LIMIT 10;

-- 4) 查询主键表（最新态）
SELECT chain_id, token, COUNT(*) AS holders
FROM account_balance
WHERE dt='2023-12-01'
GROUP BY chain_id, token
ORDER BY holders DESC
LIMIT 20;

-- 5) 简单抽样
SELECT chain_id, address, token, balance, block_number
FROM paimon_fs.demo_db.account_balance
WHERE dt='2023-12-01'
ORDER BY RAND()
LIMIT 20;

-- 6) CDC 表行数（用于对比不同 changelog 策略时的产出量）
SELECT dt, COUNT(*) AS cdc_rows
FROM account_balance_cdc
GROUP BY dt
ORDER BY dt;