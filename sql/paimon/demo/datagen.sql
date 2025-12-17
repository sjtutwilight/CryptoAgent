-- ============================================================
-- 模块: Paimon Demo数据生成
-- 存储: Paimon (MinIO S3)
-- 维护: Batch模块
-- 用途: 使用Datagen生成测试数据，验证Paimon功能
-- ============================================================

SET 'table.dynamic-table-options.enabled' = 'true';
SET 'sql-client.execution.result-mode' = 'TABLEAU';

-- ========================================
-- 1. 创建Datagen源表：DEX事件
-- ========================================
USE CATALOG default_catalog;
CREATE DATABASE IF NOT EXISTS demo_db;
USE demo_db;

CREATE TABLE IF NOT EXISTS dex_event_src (
  tx_id              STRING,
  block_number       BIGINT,
  event_time         BIGINT,
  token_in           STRING,
  token_out          STRING,
  amount_in          DECIMAL(30,10),
  amount_out         DECIMAL(30,10)
) WITH (
  'connector' = 'datagen',
  'rows-per-second' = '200',
  'fields.tx_id.kind' = 'random',
  'fields.event_time.min' = '1670000000000',
  'fields.event_time.max' = '1700000000000',
  'fields.token_in.length' = '3',
  'fields.token_out.length' = '3'
);

-- ========================================
-- 2. 创建Datagen源表：CEX K线
-- ========================================
CREATE TABLE IF NOT EXISTS cex_kline_src (
  symbol             STRING,
  window_start       BIGINT,
  window_end         BIGINT,
  open_price         DECIMAL(30,10),
  high_price         DECIMAL(30,10),
  low_price          DECIMAL(30,10),
  close_price        DECIMAL(30,10),
  volume             DECIMAL(30,10)
) WITH (
  'connector' = 'datagen',
  'rows-per-second' = '100',
  'fields.symbol.length' = '3',
  'fields.window_start.min' = '1670000000000',
  'fields.window_start.max' = '1700000000000',
  'fields.window_end.min' = '1670000060000',
  'fields.window_end.max' = '1700000060000'
);

-- ========================================
-- 3. 创建Paimon Catalog
-- ========================================
DROP CATALOG IF EXISTS paimon_catalog1;
CREATE CATALOG paimon_catalog1 WITH (
  'type' = 'paimon',
  'warehouse' = 's3a://paimon-warehouse/wh',
  'fs.s3a.endpoint' = 'http://minio:9000',
  'fs.s3a.access.key' = 'admin',
  'fs.s3a.secret.key' = 'password123',
  'fs.s3a.path.style.access' = 'true',
  'fs.s3a.connection.ssl.enabled' = 'false'
);
USE CATALOG paimon_catalog1;

CREATE DATABASE IF NOT EXISTS demo_db;
USE demo_db;

-- ========================================
-- 4. 创建Paimon Sink表：DEX事件
-- ========================================
CREATE TABLE IF NOT EXISTS dex_event (
  tx_id        STRING,
  block_number BIGINT,
  event_time   BIGINT,
  token_in     STRING,
  token_out    STRING,
  amount_in    DECIMAL(30,10),
  amount_out   DECIMAL(30,10),
  dt           STRING,
  PRIMARY KEY (tx_id) NOT ENFORCED
)
PARTITIONED BY (dt)
WITH (
  'connector' = 'paimon',
  'sink.parallelism' = '4'
);

-- ========================================
-- 5. 创建Paimon Sink表：CEX K线
-- ========================================
CREATE TABLE IF NOT EXISTS cex_kline (
  symbol        STRING,
  window_start  BIGINT,
  window_end    BIGINT,
  open_price    DECIMAL(30,10),
  high_price    DECIMAL(30,10),
  low_price     DECIMAL(30,10),
  close_price   DECIMAL(30,10),
  volume        DECIMAL(30,10),
  dt            STRING,
  PRIMARY KEY (symbol, window_start) NOT ENFORCED
)
PARTITIONED BY (dt)
WITH (
  'connector'    = 'paimon',
  'sink.parallelism' = '4'
);

-- ========================================
-- 6. 插入DEX事件数据
-- ========================================
INSERT INTO paimon_catalog1.demo_db.dex_event
SELECT
  tx_id,
  block_number,
  event_time,
  token_in,
  token_out,
  amount_in,
  amount_out,
  '2023-12-01' as dt
FROM default_catalog.demo_db.dex_event_src;

-- ========================================
-- 7. 插入CEX K线数据
-- ========================================
INSERT INTO paimon_catalog1.demo_db.cex_kline
SELECT
  symbol,
  window_start,
  window_end,
  open_price,
  high_price,
  low_price,
  close_price,
  volume,
  '2023-12-01' as dt
FROM default_catalog.demo_db.cex_kline_src;

