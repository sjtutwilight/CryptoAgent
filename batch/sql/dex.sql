SET 'table.dynamic-table-options.enabled' = 'true';
DROP CATALOG IF EXISTS paimon;

-- 建立 Paimon Catalog（MinIO 后端）
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

-- 可选：开启 checkpoint 让作业更稳
-- Kafka 源表
SET 'execution.checkpointing.interval' = '10 s';

-- === Source: Kafka topic with nested JSON ===
CREATE TEMPORARY TABLE src_dex_transaction (
  `transaction` ROW<
      blockNumber BIGINT,
      blockHash STRING,
      `timestamp` BIGINT,
      transactionHash STRING,
      transactionIndex INT,
      transactionStatus STRING,
      gasUsed BIGINT,
      gasPrice STRING,
      nonce BIGINT,
      fromAddress STRING,
      toAddress STRING,
      transactionValue STRING,
      inputData STRING,
      chainID STRING>,
  `events` ARRAY<ROW<
      eventName STRING,
      contractAddress STRING,
      logIndex INT,
      blockNumber BIGINT,
      topics ARRAY<STRING>,
      eventData STRING,
      decodedArgs MAP<STRING, STRING>>>,
  event_ts AS TO_TIMESTAMP_LTZ(`transaction`.`timestamp`, 3),
  WATERMARK FOR event_ts AS event_ts - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'dex_transaction',
  'properties.bootstrap.servers' = 'kafka:29092',
  'properties.group.id' = 'flink-std',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

-- === Sinks: flat tables for StarRocks-friendly schemas ===
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

-- === Ingest from Kafka into flat tables ===
INSERT INTO lake_bronze.tx_transaction
SELECT
  `transaction`.chainID,
  `transaction`.blockNumber,
  event_ts,
  `transaction`.transactionHash,
  `transaction`.gasUsed,
  `transaction`.gasPrice,
  `transaction`.nonce,
  `transaction`.fromAddress,
  `transaction`.toAddress,
  `transaction`.transactionValue,
  `transaction`.transactionStatus,
  `transaction`.inputData,
  CAST('hardhat' AS STRING),
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP_LTZ(3)),
  DATE_FORMAT(event_ts, 'yyyy-MM-dd')
FROM src_dex_transaction;

SET 'json.ignore-parse-errors' = 'false';


INSERT INTO lake_bronze.tx_events
SELECT
  `transaction`.chainID,
  `transaction`.blockNumber,
  event_ts,
  `transaction`.transactionHash,
  e.eventName,
  e.contractAddress,
  e.logIndex,
  -- 最终优化版本：转换为标准JSON格式，便于数据管道处理
  CAST(e.topics AS STRING) AS topics_json,
  e.eventData,
  CAST(e.decodedArgs AS STRING) AS decoded_args_json,
  CAST('hardhat' AS STRING),
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP_LTZ(3)),
  DATE_FORMAT(event_ts, 'yyyy-MM-dd')
FROM src_dex_transaction
CROSS JOIN UNNEST(`events`) AS e;