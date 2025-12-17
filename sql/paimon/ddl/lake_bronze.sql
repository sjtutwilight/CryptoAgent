-- ============================================================
-- 模块: Paimon 数据湖 Bronze层
-- 存储: Paimon (MinIO S3)
-- 维护: Batch模块
-- 用途: 定义数据湖Bronze层表结构
-- ============================================================

SET 'table.dynamic-table-options.enabled' = 'true';
SET 'execution.checkpointing.interval' = '10 s';

-- ========================================
-- 1. 创建 Paimon Catalog
-- ========================================
DROP CATALOG IF EXISTS paimon;

CREATE CATALOG paimon WITH (
  'type' = 'paimon',
  'warehouse' = 's3://paimon-warehouse/wh',
  's3.endpoint' = 'http://minio:9000',
  's3.access-key' = 'admin',
  's3.secret-key' = 'password123',
  's3.path.style.access' = 'true'
);

USE CATALOG paimon;
CREATE DATABASE IF NOT EXISTS lake_bronze;

-- ========================================
-- 2. DEX交易事实表（Bronze层）
-- ========================================
CREATE TABLE IF NOT EXISTS lake_bronze.tx_transaction (
  chain_id STRING,
  block_number BIGINT,
  block_timestamp TIMESTAMP_LTZ(3),
  transaction_hash STRING,
  gas_used BIGINT,
  gas_price STRING,
  nonce BIGINT,
  from_address STRING,
  to_address STRING,
  transaction_value STRING,
  tx_status STRING,
  input_data STRING,
  source STRING,
  ingest_time TIMESTAMP_LTZ(3),
  pt STRING
) PARTITIONED BY (pt)
WITH (
  'write-mode' = 'append-only',
  'file.format' = 'parquet'
);

-- ========================================
-- 3. DEX事件表（Bronze层）
-- ========================================
CREATE TABLE IF NOT EXISTS lake_bronze.tx_events (
  chain_id STRING,
  block_number BIGINT,
  block_timestamp TIMESTAMP_LTZ(3),
  transaction_hash STRING,
  event_name STRING,
  contract_address STRING,
  log_index INT,
  topics_json STRING,
  event_data STRING,
  decoded_args_json STRING,
  source STRING,
  ingest_time TIMESTAMP_LTZ(3),
  pt STRING
) PARTITIONED BY (pt)
WITH (
  'write-mode' = 'append-only',
  'file.format' = 'parquet'
);

