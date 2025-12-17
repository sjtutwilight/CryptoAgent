package com.twilight.aggregator;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Flink SQL Job - 将Kafka数据导入到Paimon数据湖
 * 基于runtime/batch/sql/dex.sql的逻辑实现
 * 
 * 新增功能：交易事实表(account_trade_fact)数据处理
 * - 从Kafka dex_transaction主题消费Swap事件
 * - 解析交易双方的买卖行为
 * - 生成PnL计算所需的事实数据
 * 
 * MVP版本特性：
 * 1. 地址映射：使用哈希函数生成临时ID，避免复杂的数据库查询
 * 2. 价格计算：基于Swap事件中的amount比率计算相对价格
 * 3. 过滤机制：排除异常数据（qty<=0或price<=0）
 * 
 * 后续优化方向：
 * 1. 集成Redis/DB查询真实的account_id, token_id, pair_id
 * 2. 接入外部价格源计算准确的USD价格
 * 3. 实现更精确的手续费计算逻辑
 * 4. 添加账户标签(label_mask)的实时查询
 * 
 * 数据流：Kafka -> Paimon -> 后续ClickHouse PnL计算
 */
public class FlinkSqlJob {
    private static final Logger log = LoggerFactory.getLogger(FlinkSqlJob.class);

    public static void main(String[] args) throws Exception {
        log.info("🚀 Starting Flink SQL Job for Kafka to Paimon ingestion");

        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 设置执行配置
        setupExecution(tEnv);
        
        // 创建Paimon Catalog
        createPaimonCatalog(tEnv);
        
        // 创建Kafka源表
        createKafkaSourceTable(tEnv);
        
        // 创建Paimon目标表
        createPaimonTables(tEnv);
        
        // 执行数据导入
        executeIngestion(tEnv);

        log.info("🎯 Flink SQL Job started successfully");
    }

    private static void setupExecution(StreamTableEnvironment tEnv) {
        log.info("⚙️ Setting up execution configuration");
        
        // 使用getConfig()来设置配置，而不是executeSql
        tEnv.getConfig().getConfiguration().setString("table.dynamic-table-options.enabled", "true");
        tEnv.getConfig().getConfiguration().setString("execution.checkpointing.interval", "10s");
        
        log.debug("✅ Execution configuration completed");
    }

    private static void createPaimonCatalog(StreamTableEnvironment tEnv) {
        log.info("📁 Creating Paimon Catalog");
        
        // 删除已存在的catalog
        tEnv.executeSql("DROP CATALOG IF EXISTS paimon");
        
        // 创建Paimon Catalog（MinIO后端）
        String createCatalogSql = 
            "CREATE CATALOG paimon WITH (" +
            "  'type' = 'paimon'," +
            "  'warehouse' = 's3://paimon-warehouse/wh'," +
            "  's3.endpoint' = 'http://localhost:9000'," +
            "  's3.access-key' = 'admin'," +
            "  's3.secret-key' = 'password123'," +
            "  's3.path.style.access' = 'true'" +
            ")";
        
        tEnv.executeSql(createCatalogSql);
        tEnv.executeSql("USE CATALOG paimon");
        tEnv.executeSql("CREATE DATABASE IF NOT EXISTS lake_bronze");
        
        log.debug("✅ Paimon Catalog created and database initialized");
    }

    private static void createKafkaSourceTable(StreamTableEnvironment tEnv) {
        log.info("📊 Creating Kafka source table");
        
        String createSourceSql = 
            "CREATE TEMPORARY TABLE src_dex_transaction (" +
            "  `transaction` ROW<" +
            "      blockNumber BIGINT," +
            "      blockHash STRING," +
            "      `timestamp` BIGINT," +
            "      transactionHash STRING," +
            "      transactionIndex INT," +
            "      transactionStatus STRING," +
            "      gasUsed BIGINT," +
            "      gasPrice STRING," +
            "      nonce BIGINT," +
            "      fromAddress STRING," +
            "      toAddress STRING," +
            "      transactionValue STRING," +
            "      inputData STRING," +
            "      chainID STRING>," +
            "  `events` ARRAY<ROW<" +
            "      eventName STRING," +
            "      contractAddress STRING," +
            "      logIndex INT," +
            "      blockNumber BIGINT," +
            "      topics ARRAY<STRING>," +
            "      eventData STRING," +
            "      decodedArgs MAP<STRING, STRING>>>," +
            "  event_ts AS TO_TIMESTAMP_LTZ(`transaction`.`timestamp`, 3)," +
            "  WATERMARK FOR event_ts AS event_ts - INTERVAL '5' SECOND" +
            ") WITH (" +
            "  'connector' = 'kafka'," +
            "  'topic' = 'dex_transaction'," +
            "  'properties.bootstrap.servers' = 'localhost:9092'," +
            "  'properties.group.id' = 'flink-sql-job'," +
            "  'scan.startup.mode' = 'latest-offset'," +
            "  'format' = 'json'," +
            "  'json.ignore-parse-errors' = 'true'" +
            ")";
        
        tEnv.executeSql(createSourceSql);
        log.debug("✅ Kafka source table created");
    }

    private static void createPaimonTables(StreamTableEnvironment tEnv) {
        log.info("🏗️ Creating Paimon target tables");
        
        // 创建交易表
        String createTxTableSql = 
            "CREATE TABLE IF NOT EXISTS lake_bronze.tx_transaction (" +
            "  chain_id STRING," +
            "  block_number BIGINT," +
            "  block_timestamp TIMESTAMP_LTZ(3)," +
            "  transaction_hash STRING," +
            "  gas_used BIGINT," +
            "  gas_price STRING," +
            "  nonce BIGINT," +
            "  from_address STRING," +
            "  to_address STRING," +
            "  transaction_value STRING," +
            "  tx_status STRING," +
            "  input_data STRING," +
            "  source STRING," +
            "  ingest_time TIMESTAMP_LTZ(3)," +
            "  pt STRING" +
            ") PARTITIONED BY (pt)" +
            "WITH (" +
            "  'write-mode' = 'append-only'," +
            "  'file.format' = 'parquet'" +
            ")";
        
        tEnv.executeSql(createTxTableSql);
        
        // 创建事件表
        String createEventsTableSql = 
            "CREATE TABLE IF NOT EXISTS lake_bronze.tx_events (" +
            "  chain_id STRING," +
            "  block_number BIGINT," +
            "  block_timestamp TIMESTAMP_LTZ(3)," +
            "  transaction_hash STRING," +
            "  event_name STRING," +
            "  contract_address STRING," +
            "  log_index INT," +
            "  topics_json STRING," +
            "  event_data STRING," +
            "  decoded_args_json STRING," +
            "  source STRING," +
            "  ingest_time TIMESTAMP_LTZ(3)," +
            "  pt STRING" +
            ") PARTITIONED BY (pt)" +
            "WITH (" +
            "  'write-mode' = 'append-only'," +
            "  'file.format' = 'parquet'" +
            ")";
        
        tEnv.executeSql(createEventsTableSql);
        
        // 创建交易事实表（PnL计算基础表）
        String createTradeFactSql = 
            "CREATE TABLE IF NOT EXISTS lake_bronze.account_trade_fact (" +
            "  chain_id INT," +
            "  token_id BIGINT," +
            "  account_id BIGINT," +
            "  side STRING," +           // 'buy'/'sell'
            "  qty DECIMAL(38,18)," +
            "  price_usd DECIMAL(38,18)," +
            "  value_usd DECIMAL(38,18)," +
            "  fee_usd DECIMAL(38,18)," +
            "  pair_id BIGINT," +
            "  router STRING," +
            "  block_id BIGINT," +
            "  block_time TIMESTAMP_LTZ(3)," +
            "  tx_hash STRING," +
            "  log_index INT," +
            "  label_mask INT," +
            "  source STRING," +
            "  ingest_time TIMESTAMP_LTZ(3)," +
            "  pt STRING" +              // 分区字段：yyyy-MM-dd
            ") PARTITIONED BY (pt)" +
            "WITH (" +
            "  'write-mode' = 'append-only'," +
            "  'file.format' = 'parquet'" +
            ")";
        
        tEnv.executeSql(createTradeFactSql);
        log.debug("✅ Paimon target tables created (including trade fact table)");
    }

    private static void executeIngestion(StreamTableEnvironment tEnv) {
        log.info("🔄 Starting data ingestion");
        
        // 交易数据导入
        String insertTxSql = 
            "INSERT INTO lake_bronze.tx_transaction " +
            "SELECT " +
            "  `transaction`.chainID," +
            "  `transaction`.blockNumber," +
            "  event_ts," +
            "  `transaction`.transactionHash," +
            "  `transaction`.gasUsed," +
            "  `transaction`.gasPrice," +
            "  `transaction`.nonce," +
            "  `transaction`.fromAddress," +
            "  `transaction`.toAddress," +
            "  `transaction`.transactionValue," +
            "  `transaction`.transactionStatus," +
            "  `transaction`.inputData," +
            "  CAST('hardhat' AS STRING)," +
            "  CAST(CURRENT_TIMESTAMP AS TIMESTAMP_LTZ(3))," +
            "  DATE_FORMAT(event_ts, 'yyyy-MM-dd') " +
            "FROM src_dex_transaction";
        
        // 事件数据导入
        String insertEventsSql = 
            "INSERT INTO lake_bronze.tx_events " +
            "SELECT " +
            "  `transaction`.chainID," +
            "  `transaction`.blockNumber," +
            "  event_ts," +
            "  `transaction`.transactionHash," +
            "  e.eventName," +
            "  e.contractAddress," +
            "  e.logIndex," +
            "  CAST(e.topics AS STRING) AS topics_json," +
            "  e.eventData," +
            "  CAST(e.decodedArgs AS STRING) AS decoded_args_json," +
            "  CAST('hardhat' AS STRING)," +
            "  CAST(CURRENT_TIMESTAMP AS TIMESTAMP_LTZ(3))," +
            "  DATE_FORMAT(event_ts, 'yyyy-MM-dd') " +
            "FROM src_dex_transaction " +
            "CROSS JOIN UNNEST(`events`) AS e";
        
        // 交易事实数据导入 - 从Swap事件提取买卖双方交易记录
        String insertTradeFactSql =
            "INSERT INTO lake_bronze.account_trade_fact " +
            "WITH swap_events AS (\n" +
            "  SELECT\n" +
            "    `transaction`.chainID AS chain_id,\n" +
            "    `transaction`.blockNumber AS block_number,\n" +
            "    event_ts AS block_time,\n" +
            "    `transaction`.transactionHash AS tx_hash,\n" +
            "    e.logIndex,\n" +
            "    e.contractAddress AS pair_address,\n" +
            "    `transaction`.fromAddress AS sender,\n" +
            "    `transaction`.toAddress AS recipient,\n" +
            "    e.decodedArgs['amount0In'] AS amount0_in,\n" +
            "    e.decodedArgs['amount1In'] AS amount1_in,\n" +
            "    e.decodedArgs['amount0Out'] AS amount0_out,\n" +
            "    e.decodedArgs['amount1Out'] AS amount1_out\n" +
            "  FROM src_dex_transaction\n" +
            "  CROSS JOIN UNNEST(`events`) AS e\n" +
            "  WHERE e.eventName = 'Swap'\n" +
            "    AND (e.decodedArgs['amount0In'] IS NOT NULL OR e.decodedArgs['amount1In'] IS NOT NULL)\n" +
            "    AND (e.decodedArgs['amount0Out'] IS NOT NULL OR e.decodedArgs['amount1Out'] IS NOT NULL)\n" +
            "),\n" +
            "-- 为每个Swap事件生成买卖双方记录\n" +
            "trade_records AS (\n" +
            "  -- 买方记录：用token1买token0 (amount1In > 0, amount0Out > 0)\n" +
            "  SELECT\n" +
            "    CAST(chain_id AS INT) AS chain_id,\n" +
            "    -- 使用哈希生成唯一ID，避免数据覆盖\n" +
            "    ABS(HASH_CODE(CONCAT(pair_address, '_token0'))) % 1000000 AS token_id,\n" +
            "    ABS(HASH_CODE(sender)) % 1000000 AS account_id,\n" +
            "    ABS(HASH_CODE(pair_address)) % 100000 AS pair_id,\n" +
            "    CAST('buy' AS STRING) AS side,\n" +
            "    CAST(amount0_out AS DECIMAL(38,18)) / POWER(10, 18) AS qty,\n" +
            "    CASE\n" +
            "      WHEN CAST(amount0_out AS DECIMAL(38,18)) > 0\n" +
            "        THEN (CAST(amount1_in AS DECIMAL(38,18)) / POWER(10, 18)) / (CAST(amount0_out AS DECIMAL(38,18)) / POWER(10, 18))\n" +
            "      ELSE 1.0\n" +
            "    END AS price_usd,\n" +
            "    CAST(amount1_in AS DECIMAL(38,18)) / POWER(10, 18) AS value_usd,\n" +
            "    (CAST(amount1_in AS DECIMAL(38,18)) / POWER(10, 18)) * 0.003 AS fee_usd,\n" +
            "    CAST('TWSwapRouter' AS STRING) AS router,\n" +
            "    block_number AS block_id,\n" +
            "    block_time,\n" +
            "    tx_hash,\n" +
            "    logIndex AS log_index,\n" +
            "    CAST(0 AS INT) AS label_mask\n" +
            "  FROM swap_events\n" +
            "  WHERE amount1_in IS NOT NULL AND amount1_in <> '0' \n" +
            "    AND amount0_out IS NOT NULL AND amount0_out <> '0'\n" +
            "    AND CAST(amount1_in AS DECIMAL(38,18)) > 0\n" +
            "    AND CAST(amount0_out AS DECIMAL(38,18)) > 0\n" +
            "\n" +
            "  UNION ALL\n" +
            "\n" +
            "  -- 卖方记录：卖token0得token1 (amount0In > 0, amount1Out > 0)\n" +
            "  SELECT\n" +
            "    CAST(chain_id AS INT) AS chain_id,\n" +
            "    ABS(HASH_CODE(CONCAT(pair_address, '_token0'))) % 1000000 AS token_id,\n" +
            "    ABS(HASH_CODE(sender)) % 1000000 AS account_id,\n" +
            "    ABS(HASH_CODE(pair_address)) % 100000 AS pair_id,\n" +
            "    CAST('sell' AS STRING) AS side,\n" +
            "    CAST(amount0_in AS DECIMAL(38,18)) / POWER(10, 18) AS qty,\n" +
            "    CASE\n" +
            "      WHEN CAST(amount0_in AS DECIMAL(38,18)) > 0\n" +
            "        THEN (CAST(amount1_out AS DECIMAL(38,18)) / POWER(10, 18)) / (CAST(amount0_in AS DECIMAL(38,18)) / POWER(10, 18))\n" +
            "      ELSE 1.0\n" +
            "    END AS price_usd,\n" +
            "    CAST(amount1_out AS DECIMAL(38,18)) / POWER(10, 18) AS value_usd,\n" +
            "    (CAST(amount1_out AS DECIMAL(38,18)) / POWER(10, 18)) * 0.003 AS fee_usd,\n" +
            "    CAST('TWSwapRouter' AS STRING) AS router,\n" +
            "    block_number AS block_id,\n" +
            "    block_time,\n" +
            "    tx_hash,\n" +
            "    logIndex AS log_index,\n" +
            "    CAST(0 AS INT) AS label_mask\n" +
            "  FROM swap_events\n" +
            "  WHERE amount0_in IS NOT NULL AND amount0_in <> '0'\n" +
            "    AND amount1_out IS NOT NULL AND amount1_out <> '0'\n" +
            "    AND CAST(amount0_in AS DECIMAL(38,18)) > 0\n" +
            "    AND CAST(amount1_out AS DECIMAL(38,18)) > 0\n" +
            ")\n" +
            "SELECT\n" +
            "  chain_id, token_id, account_id, side, qty, price_usd, value_usd, fee_usd,\n" +
            "  pair_id, router, block_id, block_time, tx_hash, log_index, label_mask,\n" +
            "  CAST('hardhat' AS STRING) AS source,\n" +
            "  CAST(CURRENT_TIMESTAMP AS TIMESTAMP_LTZ(3)) AS ingest_time,\n" +
            "  DATE_FORMAT(block_time, 'yyyy-MM-dd') AS pt\n" +
            "FROM trade_records";
        
        // 将多个 INSERT 合并为同一个 StatementSet，避免 Kafka 同一 group.id 被多个独立作业消费导致“分摊消息”
        log.info("🧾 Building single StatementSet for all sinks");
        var statementSet = tEnv.createStatementSet();
        statementSet.addInsertSql(insertTxSql);
        statementSet.addInsertSql(insertEventsSql);
        statementSet.addInsertSql(insertTradeFactSql);

        log.info("🚚 Executing StatementSet (single streaming job for all sinks)");
        statementSet.execute();
        log.info("✅ StatementSet submitted successfully");
        log.info("🔍 Note: Trade fact logic updated to handle Kafka message format:");
        log.info("   - Fixed decodedArgs MAP access syntax");
        log.info("   - Corrected buy/sell logic based on amount1In vs amount0Out");
        log.info("   - Added decimal scaling (/10^18) for token amounts");
        log.info("   - Relaxed filtering to allow more records through");
    }
    
}
