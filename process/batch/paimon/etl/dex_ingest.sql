-- ============================================================
-- 模块: DEX数据入湖ETL
-- 存储: Paimon (MinIO S3)
-- 维护: Batch模块
-- 上游Topic: dex_transaction
-- 用途: 从Kafka消费DEX交易数据并写入Paimon数据湖
-- ============================================================

SET 'table.dynamic-table-options.enabled' = 'true';
SET 'sql-client.execution.result-mode' = 'TABLEAU';
SET 'execution.checkpointing.interval' = '10 s';
SET 'json.ignore-parse-errors' = 'true';

-- ========================================
-- 1. 创建Kafka源表
-- ========================================
USE CATALOG default_catalog;
CREATE DATABASE IF NOT EXISTS demo_db;
USE demo_db;

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

-- ========================================
-- 2. 插入交易数据到Paimon
-- ========================================
INSERT INTO paimon.lake_bronze.tx_transaction
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

-- ========================================
-- 3. 插入事件数据到Paimon
-- ========================================
INSERT INTO paimon.lake_bronze.tx_events
SELECT
  `transaction`.chainID,
  `transaction`.blockNumber,
  event_ts,
  `transaction`.transactionHash,
  e.eventName,
  e.contractAddress,
  e.logIndex,
  -- 转换为标准JSON格式，便于数据管道处理
  CAST(e.topics AS STRING) AS topics_json,
  e.eventData,
  CAST(e.decodedArgs AS STRING) AS decoded_args_json,
  CAST('hardhat' AS STRING),
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP_LTZ(3)),
  DATE_FORMAT(event_ts, 'yyyy-MM-dd')
FROM src_dex_transaction
CROSS JOIN UNNEST(`events`) AS e;






