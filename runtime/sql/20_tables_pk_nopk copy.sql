
-- 一
-- 建表：无主键事实表 + 主键表（含变更流产出）
-- 假定：你已创建好 paimon_catalog1 指向 MinIO 的 warehouse
--      例如：'warehouse'='s3://paimon-warehouse/wh' 且 s3.* 已配置

SET 'table.dynamic-table-options.enabled' = 'true';
DROP CATALOG IF EXISTS paimon_catalog1;
CREATE CATALOG paimon_catalog1 WITH (
  'type' = 'paimon',
  'warehouse' = 's3://paimon-warehouse/wh',
  's3.endpoint' = 'http://172.18.0.100:9000',
  's3.access-key' = 'admin',
  's3.secret-key' = 'password123',
  's3.path.style.access' = 'true'
);
USE CATALOG paimon_catalog1;
CREATE DATABASE IF NOT EXISTS demo_db;
USE demo_db;

-- A) Append-only 事实表：链上日志/交易明细
CREATE TABLE IF NOT EXISTS tx_logs (
  chain_id      STRING,
  block_number  BIGINT,
  tx_hash       STRING,
  log_index     INT,
  event_time_ms BIGINT,    -- 业务时间（毫秒）
  contract      STRING,
  topic0        STRING,
  data_hex      STRING,
  dt            STRING
)
PARTITIONED BY (dt)
WITH (
  'connector' = 'paimon',
  -- 文件布局/小文件治理 & 压缩可观测
  'bucket' = '8',
  'file.format' = 'parquet',
  'target-file-size' = '128mb',
  'compaction.early-max-file-count' = '20',
  'compaction.target-file-size' = '128mb',
  -- 可选：写入排序，提升压缩与谓词下推效果
  'write.sort-columns' = 'chain_id, dt, block_number',
  'bucket-key' = 'tx_hash'
);

-- B) 主键表：账户/资产“最新状态”（CHANGE_LOG）
CREATE TABLE IF NOT EXISTS account_balance (
  chain_id      STRING,
  address       STRING,
  token         STRING,
  balance       DECIMAL(38, 18),
  block_number  BIGINT,
  block_time_ms BIGINT,
  dt            STRING,
  PRIMARY KEY (dt,chain_id, address, token) NOT ENFORCED
)
PARTITIONED BY (dt)
WITH (
  'connector' = 'paimon',
  'table.type' = 'CHANGE_LOG',
  -- 乱序/幂等裁决关键：谁“更新”取决于 sequence.field
  'merge-engine' = 'deduplicate',
  'sequence.field' = 'block_number',
  -- 写入缓冲与并行布局
  'write-buffer-size' = '256mb',
  'bucket' = '-1',
  'target-file-size' = '128mb',
  'compaction.early-max-file-count' = '20'
);

-- C) 主键表 + 变更流产出（用于下游消费对比）
CREATE TABLE IF NOT EXISTS account_balance_cdc (
  chain_id      STRING,
  address       STRING,
  token         STRING,
  balance       DECIMAL(38, 18),
  block_number  BIGINT,
  block_time_ms BIGINT,
  dt            STRING,
  PRIMARY KEY (dt,chain_id, address, token) NOT ENFORCED
)
PARTITIONED BY (dt)
WITH (
  'connector' = 'paimon',
  'table.type' = 'CHANGE_LOG',
  'merge-engine' = 'deduplicate',
  'sequence.field' = 'block_number',
  -- 产出变更流模式：input / full-compaction / lookup / none
  'changelog-producer' = 'input',
  'bucket' = '-1',
  'target-file-size' = '128mb'
);

-- 二
-- 有界数据写入（datagen 的 number-of-rows），便于一次执行就结束
-- 覆盖：append-only 吞吐、主键表乱序/重复写入、CDC 表对比

-- 1) 定义 datagen 源（在 default_catalog 下）
USE CATALOG default_catalog;
CREATE DATABASE IF NOT EXISTS demo_db;
USE demo_db;

-- 1.A) tx_logs 的 datagen（有界）
DROP TABLE IF EXISTS tx_logs_src;
CREATE TABLE tx_logs_src (
  chain_id      STRING,
  block_number  BIGINT,
  tx_hash       STRING,
  log_index     INT,
  event_time_ms BIGINT,
  contract      STRING,
  topic0        STRING,
  data_hex      STRING,
  dt            STRING
) WITH (
  'connector' = 'datagen',
  'number-of-rows' = '50000',            -- 有界；可调大压测吞吐/小文件
  'rows-per-second' = '200',           -- 吞吐
  'fields.chain_id.length' = '3',
  'fields.block_number.min' = '10000000',
  'fields.block_number.max' = '10050000',
  'fields.tx_hash.length' = '64',
  'fields.log_index.min' = '0',
  'fields.log_index.max' = '15',
  'fields.event_time_ms.min' = '1701388800000', -- 2023-12-01 00:00:00
  'fields.event_time_ms.max' = '1701475199000',
  'fields.contract.length' = '40',
  'fields.topic0.length' = '64',
  'fields.data_hex.length' = '120',
  'fields.dt.length' = '10'
);

-- 1.B) 账户余额（较新版本，高序号）
DROP TABLE IF EXISTS balance_src_newer;
CREATE TABLE balance_src_newer (
  chain_id      STRING,
  address       STRING,
  token         STRING,
  balance       DECIMAL(38,18),
  block_number  BIGINT,
  block_time_ms BIGINT,
  dt            STRING
) WITH (
  'connector' = 'datagen',
  'number-of-rows' = '20000',
  'rows-per-second' = '100',
  'fields.chain_id.length' = '3',
  'fields.address.length' = '40',
  'fields.token.length' = '6',
  'fields.balance.min' = '0',
  'fields.balance.max' = '1000000',
  'fields.block_number.min' = '2000',
  'fields.block_number.max' = '3000',
  'fields.block_time_ms.min' = '1701388800000',
  'fields.block_time_ms.max' = '1701475199000',
  'fields.dt.length' = '10'
);

-- 1.C) 账户余额（较旧版本，低序号）——用于构造“乱序回补”
DROP TABLE IF EXISTS balance_src_older;
CREATE TABLE balance_src_older (
  chain_id      STRING,
  address       STRING,
  token         STRING,
  balance       DECIMAL(38,18),
  block_number  BIGINT,
  block_time_ms BIGINT,
  dt            STRING
) WITH (
  'connector' = 'datagen',
  'number-of-rows' = '20000',
  'rows-per-second' = '100',
  'fields.chain_id.length' = '3',
  'fields.address.length' = '40',
  'fields.token.length' = '6',
  'fields.balance.min' = '0',
  'fields.balance.max' = '1000000',
  'fields.block_number.min' = '1500',
  'fields.block_number.max' = '1800',  -- 比 newer 的 block_number 小
  'fields.block_time_ms.min' = '1701302400000',
  'fields.block_time_ms.max' = '1701388799000',
  'fields.dt.length' = '10'
);

-- 2) 写入 Paimon（在 paimon_catalog1 下）
USE CATALOG paimon_catalog1;
CREATE DATABASE IF NOT EXISTS demo_db;
USE demo_db;

-- 2.A) 写入 append-only 表：tx_logs
INSERT INTO tx_logs
SELECT
  chain_id,
  block_number,
  tx_hash,
  log_index,
  event_time_ms,
  contract,
  topic0,
  data_hex,
  '2023-12-01' AS dt
FROM default_catalog.demo_db.tx_logs_src;

-- 2.B) 主键表：先写“较新版本”（高 block_number）
INSERT INTO account_balance
SELECT
  chain_id, address, token, balance, block_number, block_time_ms,
  '2023-12-01' AS dt
FROM default_catalog.demo_db.balance_src_newer;

-- 2.C) 主键表：再写“较旧版本”（低 block_number）——模拟乱序回补
-- 由于 sequence.field=block_number，较旧记录不会覆盖较新记录
INSERT INTO account_balance
SELECT
  chain_id, address, token, balance, block_number, block_time_ms,
  '2023-12-01' AS dt
FROM default_catalog.demo_db.balance_src_older;

-- 2.D) CDC 表：同样写入（可对比 changelog 行数/时效）
INSERT INTO account_balance_cdc
SELECT
  chain_id, address, token, balance, block_number, block_time_ms,
  '2023-12-01' AS dt
FROM default_catalog.demo_db.balance_src_newer;

INSERT INTO account_balance_cdc
SELECT
  chain_id, address, token, balance, block_number, block_time_ms,
  '2023-12-01' AS dt
FROM default_catalog.demo_db.balance_src_older;

-- 2.E) 重复写入“较新版本”以验证幂等（可选）
INSERT INTO account_balance
SELECT
  chain_id, address, token, balance, block_number, block_time_ms,
  '2023-12-01' AS dt
FROM default_catalog.demo_db.balance_src_newer;