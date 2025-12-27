package com.twilight.aggregator.utils;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twilight.aggregator.config.FlinkConfig;

/**
 * SequenceExtractJob - 多源序列提取作业（扩展版本）
 * 
 * 功能：从多个 Kafka topic 中提取序列号，供控制面进行数据缺失检测
 * 
 * ============================================================================
 * 扩展性设计：
 * ============================================================================
 * 1. 支持多个数据源 topic 接入（通过配置文件指定）
 * 2. 每个数据源可配置不同的序列号字段路径
 * 3. 统一输出到 data.sequence topic
 * 4. 使用 type 字段区分不同业务的序列数据
 * 5. 使用 StatementSet 合并多个数据源，单个作业处理所有流
 * 
 * ============================================================================
 * 当前支持的数据源：
 * ============================================================================
 * - dex_transaction: DEX交易数据（序列号：transaction.blockNumber）
 * - ethereum_blocks: 以太坊区块数据（序列号：params.result.number，十六进制）
 * 
 * ============================================================================
 * 配置示例（application-dev.properties）：
 * ============================================================================
 * # 数据源列表（逗号分隔）
 * sequence.sources=dex_transaction,ethereum_blocks
 * 
 * # dex_transaction 配置
 * sequence.dex_transaction.topic=dex_transaction
 * sequence.dex_transaction.type=dex_transaction
 * sequence.dex_transaction.sequence_field=transaction.blockNumber
 * sequence.dex_transaction.timestamp_field=transaction.timestamp
 * sequence.dex_transaction.hash_field=transaction.blockHash
 * sequence.dex_transaction.chain_field=transaction.chainID
 * 
 * # ethereum_blocks 配置
 * sequence.ethereum_blocks.topic=chain.ethereum.blocks
 * sequence.ethereum_blocks.type=ethereum_blocks
 * sequence.ethereum_blocks.sequence_field=params.result.number
 * sequence.ethereum_blocks.timestamp_field=params.result.timestamp
 * sequence.ethereum_blocks.hash_field=params.result.hash
 * sequence.ethereum_blocks.chain_field=ethereum
 * 
 * # 输出配置
 * sequence.output.topic=data.sequence
 * sequence.output.bootstrap.servers=localhost:9092
 * 
 * ============================================================================
 * 统一输出格式：
 * ============================================================================
 * {
 *   "type": "dex_transaction",          // 业务类型标识
 *   "chainId": "31337",                 // 链ID
 *   "sequenceNumber": "1000005",        // 序列号（十六进制会转为十进制）
 *   "sequenceHash": "0x...",            // 序列哈希
 *   "sequenceTimestamp": 1697654400,    // 序列时间戳（秒）
 *   "processTime": 1697654410000        // 处理时间戳（毫秒）
 * }
 * 
 * ============================================================================
 * 运行方式：
 * ============================================================================
 * ./scripts/dev.sh aggregator:run sequence local
 * 
 * ============================================================================
 * 控制面使用场景：
 * ============================================================================
 * - 监控序列连续性，检测缺失的数据
 * - 触发数据回填机制
 * - 数据质量监控和告警
 * - 多链、多业务的统一监控
 * 
 * ============================================================================
 * 新增数据源步骤：
 * ============================================================================
 * 1. 在 application-dev.properties 中添加数据源配置
 * 2. 将数据源名称添加到 sequence.sources 列表
 * 3. 如果数据格式特殊，在 createSourceTableForConfig() 中添加表结构定义
 * 4. 在 buildExtractionSql() 中添加对应的提取逻辑
 */
public class SequenceExtractJob {
    private static final Logger log = LoggerFactory.getLogger(SequenceExtractJob.class);
    private static final FlinkConfig config = FlinkConfig.getInstance();
    
    /**
     * 数据源配置类
     */
    private static class SourceConfig {
        String sourceName;      // 数据源名称
        String topic;           // Kafka topic
        String type;            // 业务类型
        String sequenceField;   // 序列号字段路径
        String timestampField;  // 时间戳字段路径
        String hashField;       // 哈希字段路径
        String chainField;      // 链ID字段路径（可能是固定值）
        
        SourceConfig(String sourceName) {
            this.sourceName = sourceName;
            this.topic = config.getConfigProperty("sequence." + sourceName + ".topic");
            this.type = config.getConfigProperty("sequence." + sourceName + ".type");
            this.sequenceField = config.getConfigProperty("sequence." + sourceName + ".sequence_field");
            this.timestampField = config.getConfigProperty("sequence." + sourceName + ".timestamp_field");
            this.hashField = config.getConfigProperty("sequence." + sourceName + ".hash_field");
            this.chainField = config.getConfigProperty("sequence." + sourceName + ".chain_field");
        }
        
        boolean isValid() {
            return topic != null && type != null && sequenceField != null;
        }
    }
    
    public static void main(String[] args) throws Exception {
        log.info("🚀 Starting Multi-Source Sequence Extract Job (扩展版本)");
        
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        
        // 设置执行配置
        setupExecution(tEnv);

        // 注册辅助函数
        registerFunctions(tEnv);
        
        // 读取配置的数据源列表
        String sourcesStr = config.getConfigProperty("sequence.sources", "dex_transaction");
        String[] sourceNames = sourcesStr.split(",");
        
        List<SourceConfig> sourceConfigs = new ArrayList<>();
        for (String sourceName : sourceNames) {
            sourceName = sourceName.trim();
            SourceConfig sourceConfig = new SourceConfig(sourceName);
            if (sourceConfig.isValid()) {
                sourceConfigs.add(sourceConfig);
                log.info("✅ Loaded source config: {} (type={}, topic={})", 
                    sourceName, sourceConfig.type, sourceConfig.topic);
            } else {
                log.warn("⚠️ Invalid source config for: {}", sourceName);
            }
        }
        
        if (sourceConfigs.isEmpty()) {
            log.error("❌ No valid source configurations found!");
            throw new IllegalStateException("No valid source configurations");
        }
        
        // 创建统一的输出表
        createUnifiedSinkTable(tEnv);
        
        // 为每个数据源创建输入表
        for (SourceConfig sourceConfig : sourceConfigs) {
            createSourceTableForConfig(tEnv, sourceConfig);
        }
        
        // 使用 StatementSet 合并多个 INSERT 语句
        log.info("🔧 Building StatementSet for all data sources");
        var statementSet = tEnv.createStatementSet();
        
        for (SourceConfig sourceConfig : sourceConfigs) {
            String insertSql = buildExtractionSql(sourceConfig);
            if (insertSql != null) {
                statementSet.addInsertSql(insertSql);
                log.info("✅ Added extraction for: {} -> data.sequence", sourceConfig.sourceName);
            }
        }
        
        // 执行所有提取任务
        log.info("🚚 Executing StatementSet (single streaming job for all sources)");
        statementSet.execute();
        
        log.info("✅ Multi-Source Sequence Extract Job started successfully");
        log.info("📊 Processing {} data sources:", sourceConfigs.size());
        for (SourceConfig sc : sourceConfigs) {
            log.info("   - {} (type: {}, topic: {})", sc.sourceName, sc.type, sc.topic);
        }
    }
    
    /**
     * 配置执行环境
     */
    private static void setupExecution(StreamTableEnvironment tEnv) {
        log.info("⚙️ Setting up execution configuration");
        
        // 启用动态表选项
        tEnv.getConfig().getConfiguration().setString("table.dynamic-table-options.enabled", "true");
        
        // 设置 Checkpoint 间隔（10秒）
        tEnv.getConfig().getConfiguration().setString("execution.checkpointing.interval", "10s");
        
        // 设置空闲超时，避免等待空分区
        tEnv.getConfig().getConfiguration().setString("table.exec.source.idle-timeout", "5s");
        
        log.debug("✅ Execution configuration completed");
    }

    /**
     * 注册作业运行所需的自定义函数
     */
    private static void registerFunctions(StreamTableEnvironment tEnv) {
        tEnv.createTemporarySystemFunction("HEX_TO_DECIMAL", HexToDecimalFunction.class);
    }
    
    /**
     * 创建统一的 Kafka 输出表：data.sequence
     */
    private static void createUnifiedSinkTable(StreamTableEnvironment tEnv) {
        log.info("📤 Creating unified Kafka sink table: data.sequence");
        
        String outputTopic = config.getConfigProperty("sequence.output.topic", "data.sequence");
        String bootstrapServers = config.getConfigProperty("sequence.output.bootstrap.servers", "localhost:9092");
        
        String createSinkSql = 
            "CREATE TEMPORARY TABLE sink_unified_sequence (" +
            "  type STRING," +                      // 业务类型标识
            "  chainId STRING," +                   // 链ID
            "  sequenceNumber STRING," +            // 序列号（统一为字符串，支持十六进制）
            "  sequenceHash STRING," +              // 序列哈希
            "  sequenceTimestamp BIGINT," +         // 序列时间戳（秒）
            "  processTime BIGINT," +               // 处理时间戳（毫秒）
            "  PRIMARY KEY (type, chainId, sequenceNumber) NOT ENFORCED" +  // 复合主键去重
            ") WITH (" +
            "  'connector' = 'upsert-kafka'," +
            "  'topic' = '" + outputTopic + "'," +
            "  'properties.bootstrap.servers' = '" + bootstrapServers + "'," +
            "  'key.format' = 'json'," +
            "  'value.format' = 'json'" +
            ")";
        
        tEnv.executeSql(createSinkSql);
        log.debug("✅ Unified sink table created: {}", outputTopic);
    }
    
    /**
     * 为特定数据源创建 Kafka 源表
     */
    private static void createSourceTableForConfig(StreamTableEnvironment tEnv, SourceConfig config) {
        log.info("📊 Creating Kafka source table for: {} (topic: {})", config.sourceName, config.topic);
        
        String tableName = "src_" + config.sourceName.replace(".", "_");
        String createSourceSql;
        
        // 根据数据源类型创建不同的表结构
        if ("dex_transaction".equals(config.type)) {
            // DEX交易数据结构
            createSourceSql = 
                "CREATE TEMPORARY TABLE " + tableName + " (" +
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
                "      decodedArgs MAP<STRING, STRING>>>" +
                ") WITH (" +
                "  'connector' = 'kafka'," +
                "  'topic' = '" + config.topic + "'," +
                "  'properties.bootstrap.servers' = 'localhost:9092'," +
                "  'properties.group.id' = 'sequence-extractor-" + config.sourceName + "'," +
                "  'scan.startup.mode' = 'latest-offset'," +
                "  'format' = 'json'," +
                "  'json.ignore-parse-errors' = 'true'" +
                ")";
        } else if ("ethereum_blocks".equals(config.type)) {
            // 以太坊区块数据结构 - 注意：很多字段名是SQL保留关键字，需要用反引号包裹
            createSourceSql = 
                "CREATE TEMPORARY TABLE " + tableName + " (" +
                "  jsonrpc STRING," +
                "  `method` STRING," +
                "  params ROW<" +
                "      subscription STRING," +
                "      `result` ROW<" +
                "          `number` STRING," +
                "          `hash` STRING," +
                "          parentHash STRING," +
                "          `timestamp` STRING," +
                "          miner STRING," +
                "          difficulty STRING," +
                "          totalDifficulty STRING," +
                "          `size` STRING," +
                "          gasLimit STRING," +
                "          gasUsed STRING," +
                "          transactionsRoot STRING," +
                "          stateRoot STRING," +
                "          receiptsRoot STRING>>" +
                ") WITH (" +
                "  'connector' = 'kafka'," +
                "  'topic' = '" + config.topic + "'," +
                "  'properties.bootstrap.servers' = 'localhost:9092'," +
                "  'properties.group.id' = 'sequence-extractor-" + config.sourceName + "'," +
                "  'scan.startup.mode' = 'latest-offset'," +
                "  'format' = 'json'," +
                "  'json.ignore-parse-errors' = 'true'" +
                ")";
        } else {
            log.warn("⚠️ Unknown source type: {}, using generic JSON structure", config.type);
            // 通用 JSON 结构（使用 RAW 类型）
            createSourceSql = 
                "CREATE TEMPORARY TABLE " + tableName + " (" +
                "  data STRING" +  // 将整个JSON作为字符串处理
                ") WITH (" +
                "  'connector' = 'kafka'," +
                "  'topic' = '" + config.topic + "'," +
                "  'properties.bootstrap.servers' = 'localhost:9092'," +
                "  'properties.group.id' = 'sequence-extractor-" + config.sourceName + "'," +
                "  'scan.startup.mode' = 'latest-offset'," +
                "  'format' = 'raw'" +
                ")";
        }
        
        tEnv.executeSql(createSourceSql);
        log.debug("✅ Source table created: {}", tableName);
    }
    
    /**
     * 构建特定数据源的提取 SQL
     */
    private static String buildExtractionSql(SourceConfig config) {
        log.info("🔄 Building extraction SQL for: {} (type: {})", config.sourceName, config.type);
        
        String tableName = "src_" + config.sourceName.replace(".", "_");
        String extractSql;
        
        if ("dex_transaction".equals(config.type)) {
            // DEX交易序列提取
            extractSql = 
                "INSERT INTO sink_unified_sequence " +
                "SELECT " +
                "  CAST('" + config.type + "' AS STRING) AS type," +
                "  `transaction`.chainID AS chainId," +
                "  CAST(`transaction`.blockNumber AS STRING) AS sequenceNumber," +
                "  `transaction`.blockHash AS sequenceHash," +
                "  `transaction`.`timestamp` / 1000 AS sequenceTimestamp," +  // 转为秒
                "  UNIX_TIMESTAMP() * 1000 AS processTime " +
                "FROM " + tableName + " " +
                "WHERE `transaction`.blockNumber IS NOT NULL " +
                "  AND `transaction`.chainID IS NOT NULL";
        } else if ("ethereum_blocks".equals(config.type)) {
            // 以太坊区块序列提取（需要十六进制转十进制）
            // 注意：Flink SQL 使用 CHAR_LENGTH 而不是 LENGTH
            extractSql = 
                "INSERT INTO sink_unified_sequence " +
                "SELECT " +
                "  CAST('" + config.type + "' AS STRING) AS type," +
                "  CAST('" + config.chainField + "' AS STRING) AS chainId," +  // 固定链ID
                "  COALESCE(CAST(HEX_TO_DECIMAL(params.`result`.`number`) AS STRING), '0') AS sequenceNumber," +
                "  params.`result`.`hash` AS sequenceHash," +
                "  COALESCE(CAST(HEX_TO_DECIMAL(params.`result`.`timestamp`) AS BIGINT), 0) AS sequenceTimestamp," +
                "  UNIX_TIMESTAMP() * 1000 AS processTime " +
                "FROM " + tableName + " " +
                "WHERE params.`result`.`number` IS NOT NULL " +
                "  AND params.`result`.`hash` IS NOT NULL " +
                "  AND params.`result`.`timestamp` IS NOT NULL";
        } else {
            log.warn("⚠️ Unknown source type: {}, skipping", config.type);
            return null;
        }
        
        log.debug("📝 Built extraction SQL for: {}", config.sourceName);
        return extractSql;
    }

    /**
     * 十六进制字符串转十进制字符串的自定义函数
     */
    public static class HexToDecimalFunction extends ScalarFunction {
        public String eval(String value) {
            if (value == null) {
                return null;
            }

            String trimmed = value.trim();
            if (trimmed.isEmpty()) {
                return null;
            }

            if (trimmed.startsWith("0x") || trimmed.startsWith("0X")) {
                trimmed = trimmed.substring(2);
            }

            if (trimmed.isEmpty()) {
                return null;
            }

            try {
                return new BigInteger(trimmed, 16).toString(10);
            } catch (NumberFormatException ex) {
                log.warn("Invalid hex string for HEX_TO_DECIMAL: {}", value);
                return null;
            }
        }
    }
}
