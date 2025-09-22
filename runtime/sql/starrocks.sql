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


SELECT * FROM paimon_fs.lake_bronze.tx_transaction ORDER BY block_timestamp DESC LIMIT 100;
SELECT * FROM paimon_fs.lake_bronze.tx_events ORDER BY block_timestamp DESC LIMIT 100;
