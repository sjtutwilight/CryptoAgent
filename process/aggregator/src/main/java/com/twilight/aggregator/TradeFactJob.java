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
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twilight.aggregator.config.FlinkConfig;
import com.twilight.aggregator.model.ProcessEvent;
import com.twilight.aggregator.model.TradeFact;
import com.twilight.aggregator.model.TokenMetrics;
import com.twilight.aggregator.model.dexTransaction.KafkaMessage;
import com.twilight.aggregator.process.common.EventEnrichmentMap;
import com.twilight.aggregator.process.common.RedisTokenMetricsBroadcaster;
import com.twilight.aggregator.process.common.UnifiedFilterOperator;
import com.twilight.aggregator.process.trade.TradeFactProcessor;
import com.twilight.aggregator.serialization.KafkaMessageDeserializer;
import com.twilight.aggregator.sink.ClickHouseSink;
import com.twilight.aggregator.source.RedisTokenMetricsSource;

/**
 * TradeFactJob - 账户交易事实表处理作业
 * 
 * 标准化流程：
 * 1. KafkaMessage -> UnifiedFilterOperator (仅Swap事件) -> ProcessEvent
 * 2. ProcessEvent -> AsyncEventEnrichmentProcessor (异步查询元数据) -> ProcessEvent  
 * 3. ProcessEvent -> RedisTokenMetricsBroadcaster (广播Token价格指标) -> ProcessEvent
 * 4. ProcessEvent -> AccountTradeExtractor -> AccountTrade (复用现有)
 * 5. AccountTrade -> TradeFactEnrichmentProcessor -> TradeFact
 * 6. TradeFact -> ClickHouseSink (ch_account_trade_fact)
 * 
 * 数据输出：
 * - 将所有账户的Token交易事实数据写入ClickHouse
 * - 支持Token和Account两个维度的查询优化
 * - 包含标签位图用于用户分层分析
 */
public class TradeFactJob {
    private static final Logger log = LoggerFactory.getLogger(TradeFactJob.class);
    private static final FlinkConfig config = FlinkConfig.getInstance();
    
    // 统一的水印策略
    private static final WatermarkStrategy<ProcessEvent> PROCESS_EVENT_WATERMARK_STRATEGY = 
        WatermarkStrategy.<ProcessEvent>forBoundedOutOfOrderness(Duration.ofMillis(500))
            .withTimestampAssigner((event, timestamp) -> {
                Long eventTimestamp = event.getTimestamp();
                return eventTimestamp != null ? eventTimestamp : System.currentTimeMillis();
            })
            .withIdleness(Duration.ofSeconds(5));
    
    
    public static void main(String[] args) throws Exception {
        log.info("🚀 Starting Trade Fact Processing Job (迭代版本)");
        
        // ===== Step 1: 设置执行环境 =====
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // 设置并行度
        int parallelism = config.getParallelism();
        env.setParallelism(parallelism);
        log.info("🔧 Set parallelism to {}", parallelism);
        
        // ===== Step 2: Kafka源配置 =====
        log.info("📨 Setting up Kafka source for trade fact processing");
        KafkaSource<KafkaMessage> kafkaSource = KafkaSource.<KafkaMessage>builder()
            .setBootstrapServers(config.getKafkaBootstrapServers())
            .setTopics(config.getKafkaTopic())
            .setGroupId(config.getKafkaGroupId() + "-trade-fact")
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
        
        // ===== Step 3: 统一事件过滤 - 仅处理Swap事件 =====
        log.info("🔧 Setting up unified event filtering for trade fact analysis");
        DataStream<ProcessEvent> filteredEventStream = env
            .fromSource(kafkaSource, kafkaWatermarkStrategy, "Kafka Source")
            .flatMap(UnifiedFilterOperator.Factory.forTradeFactProcessing()) // 使用专门的交易事实过滤器
            .name("Unified Filter Operator")
            .assignTimestampsAndWatermarks(PROCESS_EVENT_WATERMARK_STRATEGY);
        
        // ===== Step 4: 异步元数据增强 (pairMetadata, accountMetadata, tokenMetadata) =====
        log.info("🔧 Setting up async metadata enrichment");
        DataStream<ProcessEvent> metadataEnrichedStream = filteredEventStream.map(new EventEnrichmentMap()).name("Event Enrichment Map");
        
        // ===== Step 5: Token指标广播流设置 =====
        log.info("🔧 Setting up Redis token metrics broadcast stream");
        RedisTokenMetricsSource metricsSource = new RedisTokenMetricsSource(config.getPriceRefreshInterval());
        BroadcastStream<Map<String, TokenMetrics>> metricsBroadcastStream = env
            .addSource(metricsSource)
            .setParallelism(1)
            .name("Redis Token Metrics Source")
            .broadcast(RedisTokenMetricsBroadcaster.TOKEN_METRICS_STATE_DESCRIPTOR);
        
        // ===== Step 6: Token指标增强 (price, mcap, fdv, liquidity) =====
        log.info("🔧 Setting up token metrics enrichment with broadcast");
        DataStream<ProcessEvent> enrichedEventStream = metadataEnrichedStream
            .connect(metricsBroadcastStream)
            .process(new RedisTokenMetricsBroadcaster())
            .name("Redis Token Metrics Broadcaster");
        
        // ===== Step 7: 直接从ProcessEvent提取交易事实 (整合逻辑) =====
        log.info("🔧 Setting up trade fact processing");
        DataStream<TradeFact> tradeFactStream = enrichedEventStream
            .flatMap(new TradeFactProcessor())
            .name("Trade Fact Processor");
        
        // ===== Step 8: 输出到ClickHouse =====
        log.info("💾 Setting up ClickHouse sink for trade facts");
        tradeFactStream
            .addSink(ClickHouseSink.createTradeFactSink())
            .name("Trade Fact ClickHouse Sink");
        
        // ===== 执行任务 =====
        log.info("✅ Trade Fact Pipeline setup completed");
        log.info("📊 TradeFactJob pipeline summary (简化算子流):");
        log.info("  ├─ Kafka Source (dex_transaction)");
        log.info("  ├─ Unified Filter Operator (仅Swap事件)");
        log.info("  ├─ Async Metadata Enrichment (pairMetadata, accountMetadata, tokenMetadata)");
        log.info("  ├─ Redis Token Metrics Source (broadcast price + metrics)");
        log.info("  ├─ Redis Token Metrics Broadcaster (add price, mcap, fdv, liquidity)");
        log.info("  ├─ Trade Fact Processor (ProcessEvent -> TradeFact，整合提取+增强逻辑)");
        log.info("  └─ ClickHouse Sink (ch_account_trade_fact)");
        log.info("");
        log.info("🎯 Trade Fact数据模型特性:");
        log.info("  📊 维度支持: Token维度 + Account维度查询优化");
        log.info("  🏷️ 标签位图: 支持EX/SM/WH/PF/FR/TP用户分层");
        log.info("  🔗 关联字段: pairId支持与交易对数据联动");
        log.info("  ⚡ 性能优化: 投影索引加速Token和Account页面查询");
        log.info("🚀 Starting job execution: Trade Fact Processing Job");
        
        env.execute("Trade Fact Processing Job");
    }
}
