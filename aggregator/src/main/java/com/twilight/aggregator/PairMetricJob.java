// package com.twilight.aggregator;

// import java.time.Duration;
// import java.util.Map;

// import org.apache.flink.api.common.eventtime.WatermarkStrategy;
// import org.apache.flink.connector.kafka.source.KafkaSource;
// import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
// import org.apache.flink.streaming.api.datastream.BroadcastStream;
// import org.apache.flink.streaming.api.datastream.DataStream;
// import org.apache.flink.streaming.api.datastream.KeyedStream;
// import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;

// import com.twilight.aggregator.config.FlinkConfig;
// import com.twilight.aggregator.model.Pair;
// import com.twilight.aggregator.model.ProcessEvent;
// import com.twilight.aggregator.model.TokenMetrics;
// import com.twilight.aggregator.model.PairMetric;
// import com.twilight.aggregator.model.dexTransaction.KafkaMessage;
// import com.twilight.aggregator.process.common.AsyncEventEnrichmentProcessor;
// import com.twilight.aggregator.process.common.RedisTokenMetricsBroadcaster;
// import com.twilight.aggregator.process.common.UnifiedFilterOperator;
// import com.twilight.aggregator.process.pair.PairEventExtractor;
// import com.twilight.aggregator.process.pair.PairWindowManager;
// import org.apache.flink.streaming.api.datastream.AsyncDataStream;
// import java.util.concurrent.TimeUnit;
// import com.twilight.aggregator.serialization.KafkaMessageDeserializer;
// import com.twilight.aggregator.sink.ClickHouseSink;
// import com.twilight.aggregator.source.RedisTokenMetricsSource;

// /**
//  * 精简版Pair分析作业 - 使用标准化算子流
//  * 处理DEX交易对的流动性、交易量等指标分析
//  * 
//  * 标准化流程：
//  * 1. KafkaMessage -> UnifiedFilterOperator -> ProcessEvent (所有DEX事件)
//  * 2. ProcessEvent -> AsyncEventEnrichmentProcessor -> ProcessEvent (异步查询元数据)
//  * 3. ProcessEvent -> RedisTokenMetricsBroadcaster -> ProcessEvent (广播Token价格指标)
//  * 4. ProcessEvent -> PairEventExtractor -> Pair
//  * 5. Pair -> PairWindowManager (层级化窗口聚合) -> PairMetric
//  * 6. 输出到ClickHouse
//  */
// public class PairMetricJob {
//     private static final Logger log = LoggerFactory.getLogger(PairMetricJob.class);
//     private static final FlinkConfig config = FlinkConfig.getInstance();
    
//     // 统一的水印策略
//     private static final WatermarkStrategy<ProcessEvent> PROCESS_EVENT_WATERMARK_STRATEGY = 
//         WatermarkStrategy.<ProcessEvent>forBoundedOutOfOrderness(Duration.ofMillis(500))
//             .withTimestampAssigner((event, timestamp) -> {
//                 Long eventTimestamp = event.getTimestamp();
//                 return eventTimestamp != null ? eventTimestamp : System.currentTimeMillis();
//             })
//             .withIdleness(Duration.ofSeconds(5));
    
//     private static final WatermarkStrategy<Pair> PAIR_WATERMARK_STRATEGY = 
//         WatermarkStrategy.<Pair>forBoundedOutOfOrderness(Duration.ofMillis(500))
//             .withTimestampAssigner((pair, timestamp) -> {
//                 Long pairTimestamp = pair.getTimestamp();
//                 return pairTimestamp != null ? pairTimestamp : System.currentTimeMillis();
//             })
//             .withIdleness(Duration.ofSeconds(5));
    
//     public static void main(String[] args) throws Exception {
//         log.info("🚀 Starting Streamlined DeFi Pair Analysis Aggregator");
        
//         // 设置执行环境
//         final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
//         // 设置并行度
//         int parallelism = config.getParallelism();
//         env.setParallelism(parallelism);
//         log.info("🔧 Set parallelism to {}", parallelism);
        
//         // ===== Step 1: Kafka源配置 =====
//         KafkaSource<KafkaMessage> kafkaSource = KafkaSource.<KafkaMessage>builder()
//             .setBootstrapServers(config.getKafkaBootstrapServers())
//             .setTopics(config.getKafkaTopic())
//             .setGroupId(config.getKafkaGroupId() + "-pair-streamlined")
//             .setStartingOffsets(OffsetsInitializer.latest())
//             .setValueOnlyDeserializer(new KafkaMessageDeserializer())
//             .build();
        
//         // Kafka消息水印策略
//         WatermarkStrategy<KafkaMessage> kafkaWatermarkStrategy = WatermarkStrategy
//             .<KafkaMessage>forBoundedOutOfOrderness(Duration.ofMillis(500))
//             .withTimestampAssigner((message, timestamp) -> {
//                 Long msgTimestamp = message.getTransaction().getTimestamp();
//                 return msgTimestamp != null ? msgTimestamp : System.currentTimeMillis();
//             })
//             .withIdleness(Duration.ofSeconds(5));
        
//         // ===== Step 2: 统一事件过滤 (所有DEX事件) =====
//         log.info("🔧 Setting up unified event filtering for pair analysis");
//         DataStream<ProcessEvent> filteredEventStream = env
//             .fromSource(kafkaSource, kafkaWatermarkStrategy, "Kafka Source")
//             .flatMap(UnifiedFilterOperator.Factory.forPairAnalysis())
//             .name("Unified Filter Operator")
//             .assignTimestampsAndWatermarks(PROCESS_EVENT_WATERMARK_STRATEGY);
        
//         // ===== Step 3: 异步元数据增强 (pairMetadata, accountMetadata, tokenMetadata) =====
//         log.info("🔧 Setting up async metadata enrichment");
//         DataStream<ProcessEvent> metadataEnrichedStream = AsyncDataStream.unorderedWait(
//             filteredEventStream,
//             new AsyncEventEnrichmentProcessor(),
//             5000, // 5秒超时
//             TimeUnit.MILLISECONDS,
//             500 // 容量
//         ).name("Async Metadata Enrichment");
        
//         // ===== Step 4: Token指标广播流设置 =====
//         log.info("🔧 Setting up Redis token metrics broadcast stream");
//         RedisTokenMetricsSource metricsSource = new RedisTokenMetricsSource(config.getPriceRefreshInterval());
//         BroadcastStream<Map<String, TokenMetrics>> metricsBroadcastStream = env
//             .addSource(metricsSource)
//             .setParallelism(1)
//             .name("Redis Token Metrics Source")
//             .broadcast(RedisTokenMetricsBroadcaster.TOKEN_METRICS_STATE_DESCRIPTOR);
        
//         // ===== Step 5: Token指标增强 (price, mcap, fdv, liquidity) =====
//         log.info("🔧 Setting up token metrics enrichment with broadcast");
//         DataStream<ProcessEvent> enrichedEventStream = metadataEnrichedStream
//             .connect(metricsBroadcastStream)
//             .process(new RedisTokenMetricsBroadcaster())
//             .name("Redis Token Metrics Broadcaster");
        
//         // ===== Step 6: Pair事件提取 =====
//         log.info("🔧 Setting up pair event extraction");
//         DataStream<Pair> pairStream = enrichedEventStream
//             .flatMap(new PairEventExtractor())
//             .name("Pair Event Extractor")
//             .assignTimestampsAndWatermarks(PAIR_WATERMARK_STRATEGY);
        
//         // ===== Step 7: Pair流处理和层级化窗口聚合 =====
//         log.info("🔧 Setting up pair stream processing with hierarchical windowing");
//         KeyedStream<Pair, Long> keyedPairStream = pairStream
//             .keyBy(Pair::getPairId);
        
//         // 使用PairWindowManager进行层级化窗口聚合 (20s -> 1min -> 5min -> 1h)
//         DataStream<PairMetric> pairMetrics = PairWindowManager.createHierarchicalWindows(keyedPairStream);
        
//         // ===== Step 8: 输出到ClickHouse =====
//         log.info("🔧 Setting up ClickHouse sink for pair metrics");
//         pairMetrics
//             .addSink(ClickHouseSink.createPairMetricSink())
//             .name("Pair Metrics ClickHouse Sink");
        
//         // ===== 执行任务 =====
//         log.info("✅ Hierarchical pair windowing pipeline setup completed");
//         log.info("📊 PairMetricJob pipeline summary:");
//         log.info("  ├─ Kafka Source (dex_transaction)");
//         log.info("  ├─ Unified Filter Operator (extract + filter DEX events)");
//         log.info("  ├─ Async Metadata Enrichment (pairMetadata, accountMetadata, tokenMetadata)");
//         log.info("  ├─ Redis Token Metrics Source (broadcast price + metrics)");
//         log.info("  ├─ Redis Token Metrics Broadcaster (add price, mcap, fdv, liquidity)");
//         log.info("  ├─ Pair Event Extractor (ProcessEvent -> Pair)");
//         log.info("  ├─ PairWindowManager (hierarchical windowing: 20s -> 1min -> 5min -> 1h)");
//         log.info("  └─ ClickHouse Sink (pair_metric_ch)");
//         log.info("");
//         log.info("🎯 Hierarchical Window Architecture:");
//         log.info("  📊 Base Window: 20s tumbling windows");
//         log.info("  📊 1-minute: aggregated from 20s windows");
//         log.info("  📊 5-minute: aggregated from 1-minute windows");
//         log.info("  📊 1-hour: aggregated from 5-minute windows");
//         log.info("🚀 Starting job execution: Streamlined DeFi Pair Analysis Aggregator");
        
//         env.execute("Streamlined DeFi Pair Analysis Aggregator");
//     }
// }
