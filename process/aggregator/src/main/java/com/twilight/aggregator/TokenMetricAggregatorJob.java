package com.twilight.aggregator;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twilight.aggregator.config.FlinkConfig;
import com.twilight.aggregator.model.ProcessEvent;
import com.twilight.aggregator.model.Token;
import com.twilight.aggregator.model.TokenMetrics;
import com.twilight.aggregator.model.TokenRecentMetric;
import com.twilight.aggregator.model.dexTransaction.KafkaMessage;
import com.twilight.aggregator.process.common.EventEnrichmentMap;
import com.twilight.aggregator.process.common.RedisTokenMetricsBroadcaster;
import com.twilight.aggregator.process.common.UnifiedFilterOperator;
import com.twilight.aggregator.process.token.TokenEventExtractor;
import com.twilight.aggregator.process.token.TokenWindowManager;
import com.twilight.aggregator.serialization.KafkaMessageDeserializer;
import com.twilight.aggregator.sink.ClickHouseSink;
import com.twilight.aggregator.source.RedisTokenMetricsSource;

/**
 * 精简版聚合器作业 - 展示标准化算子流
 * 
 * 标准化流程：
 * 1. KafkaMessage -> UnifiedFilterOperator (事件提取+过滤) -> ProcessEvent
 * 2. ProcessEvent -> AsyncEventEnrichmentProcessor (异步查询元数据) -> ProcessEvent
 * 3. ProcessEvent -> RedisTokenMetricsBroadcaster (广播Token价格指标) -> ProcessEvent
 * 4. ProcessEvent -> TokenEventExtractor -> Token流
 * 5. Token -> TokenWindowManager (层级化窗口聚合) -> TokenRecentMetric
 * 6. TokenRecentMetric -> ClickHouseSink
 * 
 * 标准化优势：
 * - 算子职责清晰：过滤、元数据查询、价格广播、转换、处理分离
 * - 元数据异步查询：pairMetadata、accountMetadata、tokenMetadata
 * - 价格指标广播：price、mcap、fdv、liquidity等通过broadcast
 * - 流程标准化：所有job遵循相同的处理模式
 */
public class TokenMetricAggregatorJob {
    private static final Logger log = LoggerFactory.getLogger(TokenMetricAggregatorJob.class);
    private static final FlinkConfig config = FlinkConfig.getInstance();
    
    // 统一的水印策略
    private static final WatermarkStrategy<ProcessEvent> PROCESS_EVENT_WATERMARK_STRATEGY = 
        WatermarkStrategy.<ProcessEvent>forBoundedOutOfOrderness(Duration.ofMillis(500))
            .withTimestampAssigner((event, timestamp) -> {
                Long eventTimestamp = event.getTimestamp();
                return eventTimestamp != null ? eventTimestamp : System.currentTimeMillis();
            })
            .withIdleness(Duration.ofSeconds(5));
    
    private static final WatermarkStrategy<Token> TOKEN_WATERMARK_STRATEGY = 
        WatermarkStrategy.<Token>forBoundedOutOfOrderness(Duration.ofMillis(500))
            .withTimestampAssigner((token, timestamp) -> {
                Long tokenTimestamp = token.getTimestamp();
                return tokenTimestamp != null ? tokenTimestamp : System.currentTimeMillis();
            })
            .withIdleness(Duration.ofSeconds(5));
    
    public static void main(String[] args) throws Exception {
        log.info("🚀 Starting Streamlined DeFi Aggregator with Standardized Pipeline");
        
        // 设置执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // 设置并行度
        int parallelism = config.getParallelism();
        env.setParallelism(parallelism);
        log.info("🔧 Set parallelism to {}", parallelism);
        
        // ===== Step 1: Kafka源配置 =====
        log.info("📨 Setting up Kafka source");
        KafkaSource<KafkaMessage> kafkaSource = KafkaSource.<KafkaMessage>builder()
            .setBootstrapServers(config.getKafkaBootstrapServers())
            .setTopics(config.getKafkaTopic())
            .setGroupId(config.getKafkaGroupId() + "-streamlined")
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(new KafkaMessageDeserializer())
            .build();
        
        // Kafka消息水印策略
        WatermarkStrategy<KafkaMessage> kafkaWatermarkStrategy = WatermarkStrategy
            .<KafkaMessage>forBoundedOutOfOrderness(Duration.ofMillis(500))
            .withTimestampAssigner((message, timestamp) -> {
                Long msgTimestamp = message.getTransaction().getTimestamp();
                return msgTimestamp != null ? msgTimestamp : System.currentTimeMillis();
            })
            .withIdleness(Duration.ofSeconds(5));
        
        // ===== Step 2: 统一事件过滤 (提取+过滤) =====
        log.info("🔧 Setting up unified event filtering");
        DataStream<ProcessEvent> filteredEventStream = env
            .fromSource(kafkaSource, kafkaWatermarkStrategy, "Kafka Source")
            .flatMap(UnifiedFilterOperator.Factory.forTokenAnalysis())
            .name("Unified Filter Operator")
            .assignTimestampsAndWatermarks(PROCESS_EVENT_WATERMARK_STRATEGY);
        
        // ===== Step 3: 异步元数据增强 (pairMetadata, accountMetadata, tokenMetadata) =====
        log.info("🔧 Setting up async metadata enrichment");
        DataStream<ProcessEvent> metadataEnrichedStream =filteredEventStream.map(new EventEnrichmentMap()).name("Event Enrichment Map");
        
        // ===== Step 4: Token指标广播流设置 =====
        log.info("🔧 Setting up Redis token metrics broadcast stream");
        RedisTokenMetricsSource metricsSource = new RedisTokenMetricsSource(config.getPriceRefreshInterval());
        BroadcastStream<Map<String, TokenMetrics>> metricsBroadcastStream = env
            .addSource(metricsSource)
            .setParallelism(1)
            .name("Redis Token Metrics Source")
            .broadcast(RedisTokenMetricsBroadcaster.TOKEN_METRICS_STATE_DESCRIPTOR);
        
        // ===== Step 5: Token指标增强 (price, mcap, fdv, liquidity) =====
        log.info("🔧 Setting up token metrics enrichment with broadcast");
        DataStream<ProcessEvent> enrichedEventStream = metadataEnrichedStream
            .connect(metricsBroadcastStream)
            .process(new RedisTokenMetricsBroadcaster())
            .name("Redis Token Metrics Broadcaster");
        
        // ===== Step 6: Token事件提取 =====
        log.info("🔧 Setting up token event extraction");
        DataStream<Token> tokenStream = enrichedEventStream
            .flatMap(new TokenEventExtractor())
            .name("Token Event Extractor")
            .assignTimestampsAndWatermarks(TOKEN_WATERMARK_STRATEGY);
        
        // ===== Step 7: Token流处理和层级化窗口聚合 =====
        log.info("🔧 Setting up token stream processing with hierarchical windowing");
        KeyedStream<Token, String> keyedTokenStream = tokenStream
            .keyBy(Token::getTokenAddress);
        
        // 使用TokenWindowManager进行层级化窗口聚合 (20s -> 1min -> 5min -> 1h)
        DataStream<TokenRecentMetric> tokenRecentMetrics = TokenWindowManager.createSlidingHierarchicalWindows(keyedTokenStream);
        
        // ===== Step 8: 输出到ClickHouse =====
        log.info("🔧 Setting up ClickHouse sink for token metrics");
        tokenRecentMetrics
            .addSink(ClickHouseSink.createTokenRecentMetricSink())
            .name("Token Recent Metrics ClickHouse Sink");
        
        // ===== 执行任务 =====
        log.info("✅ Streamlined pipeline setup completed");
        log.info("📊 TokenMetricAggregatorJob pipeline summary:");
        log.info("  ├─ Kafka Source (dex_transaction)");
        log.info("  ├─ Unified Filter Operator (extract + filter events)");
        log.info("  ├─ Async Metadata Enrichment (pairMetadata, accountMetadata, tokenMetadata)");
        log.info("  ├─ Redis Token Metrics Source (broadcast price + metrics)");
        log.info("  ├─ Redis Token Metrics Broadcaster (add price, mcap, fdv, liquidity)");
        log.info("  ├─ Token Event Extractor (ProcessEvent -> Token)");
        log.info("  ├─ TokenWindowManager (hierarchical windowing: 20s -> 1min -> 5min -> 1h)");
        log.info("  └─ ClickHouse Sink (token_recent_metric_ch)");
        log.info("");
        log.info("🎯 Hierarchical Window Architecture:");
        log.info("  📊 Base Window: 20s sliding windows");
        log.info("  📊 1-minute: aggregated from base windows");
        log.info("  📊 5-minute: aggregated from 1-minute windows");
        log.info("  📊 1-hour: aggregated from 5-minute windows");
        log.info("");
        log.info("🎯 Standardized Architecture Benefits:");
        log.info("  🔧 Clear separation: metadata async query vs metrics broadcast");
        log.info("  📊 Async metadata: pairMetadata, accountMetadata, tokenMetadata");
        log.info("  📡 Broadcast metrics: price, mcap, fdv, liquidity");
        log.info("  ⚡ Multi-granularity analysis: real-time to hourly aggregations");
        log.info("🚀 Starting job execution: Streamlined DeFi Token Aggregator");
        
        env.execute("Streamlined DeFi Token Aggregator");
    }
}
