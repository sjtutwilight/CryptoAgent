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
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twilight.aggregator.config.FlinkConfig;
import com.twilight.aggregator.model.AccountPnLSnapshot;
import com.twilight.aggregator.model.AccountTrade;
import com.twilight.aggregator.model.PnLRealizedEvent;
import com.twilight.aggregator.model.ProcessEvent;
import com.twilight.aggregator.model.TokenMetrics;
import com.twilight.aggregator.model.dexTransaction.KafkaMessage;
import com.twilight.aggregator.process.common.EventEnrichmentMap;
import com.twilight.aggregator.process.common.RedisTokenMetricsBroadcaster;
import com.twilight.aggregator.process.common.UnifiedFilterOperator;
import com.twilight.aggregator.process.pnl.AccountTradeExtractor;
import com.twilight.aggregator.process.pnl.PnLProcessor;
import com.twilight.aggregator.serialization.KafkaMessageDeserializer;
import com.twilight.aggregator.sink.ClickHouseSink;
import com.twilight.aggregator.source.RedisTokenMetricsSource;

/**
 * PnL聚合器作业 - 使用标准化算子流
 * 专注于账户-Token维度的盈亏分析，基于移动平均成本算法
 * 
 * 标准化流程：
 * 1. KafkaMessage -> UnifiedFilterOperator (仅Swap事件) -> ProcessEvent
 * 2. ProcessEvent -> AsyncEventEnrichmentProcessor (异步查询元数据) -> ProcessEvent
 * 3. ProcessEvent -> RedisTokenMetricsBroadcaster (广播Token价格指标) -> ProcessEvent
 * 4. ProcessEvent -> AccountTradeExtractor -> AccountTrade
 * 5. AccountTrade -> PnLProcessor -> AccountPnLSnapshot + PnLRealizedEvent
 * 6. 输出到ClickHouse
 */
public class PnLAggregatorJob {
    private static final Logger log = LoggerFactory.getLogger(PnLAggregatorJob.class);
    private static final FlinkConfig config = FlinkConfig.getInstance();
    
    // 统一的水印策略
    private static final WatermarkStrategy<ProcessEvent> PROCESS_EVENT_WATERMARK_STRATEGY = 
        WatermarkStrategy.<ProcessEvent>forBoundedOutOfOrderness(Duration.ofMillis(500))
            .withTimestampAssigner((event, timestamp) -> {
                Long eventTimestamp = event.getTimestamp();
                return eventTimestamp != null ? eventTimestamp : System.currentTimeMillis();
            })
            .withIdleness(Duration.ofSeconds(5));
    
    private static final WatermarkStrategy<AccountTrade> TRADE_WATERMARK_STRATEGY = 
        WatermarkStrategy.<AccountTrade>forBoundedOutOfOrderness(Duration.ofMillis(500))
            .withTimestampAssigner((trade, timestamp) -> {
                Long tradeTimestamp = trade.getBlockTimeMs();
                return tradeTimestamp != null ? tradeTimestamp : System.currentTimeMillis();
            })
            .withIdleness(Duration.ofSeconds(5));
    
    public static void main(String[] args) throws Exception {
        log.info("🚀 Starting DeFi Account PnL Aggregator (Moving Average Cost Algorithm)");
        
        // 设置执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // 设置并行度
        int parallelism = config.getParallelism();
        env.setParallelism(parallelism);
        log.info("🔧 Set parallelism to {}", parallelism);
        
        // 配置Kafka源
        KafkaSource<KafkaMessage> kafkaSource = KafkaSource.<KafkaMessage>builder()
            .setBootstrapServers(config.getKafkaBootstrapServers())
            .setTopics(config.getKafkaTopic())
            .setGroupId(config.getKafkaGroupId() + "-pnl")
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
        
        // ===== Step 1: 统一事件过滤 - 仅处理Swap事件 =====
        log.info("🔧 Setting up unified event filtering for PnL analysis");
        DataStream<ProcessEvent> filteredEventStream = env
            .fromSource(kafkaSource, kafkaWatermarkStrategy, "Kafka Source")
            .flatMap(UnifiedFilterOperator.Factory.forPnLAnalysis())
            .name("Unified Filter Operator")
            .assignTimestampsAndWatermarks(PROCESS_EVENT_WATERMARK_STRATEGY);
        
        // ===== Step 2: 异步元数据增强 (pairMetadata, accountMetadata, tokenMetadata) =====
        log.info("🔧 Setting up async metadata enrichment");
        DataStream<ProcessEvent> metadataEnrichedStream = filteredEventStream.map(new EventEnrichmentMap()).name("Event Enrichment Map");
        
        
        // ===== Step 3: Token指标广播流设置 =====
        log.info("🔧 Setting up Redis token metrics broadcast stream");
        RedisTokenMetricsSource metricsSource = new RedisTokenMetricsSource(config.getPriceRefreshInterval());
        BroadcastStream<Map<String, TokenMetrics>> metricsBroadcastStream = env
            .addSource(metricsSource)
            .setParallelism(1)
            .name("Redis Token Metrics Source")
            .broadcast(RedisTokenMetricsBroadcaster.TOKEN_METRICS_STATE_DESCRIPTOR);
        
        // ===== Step 4: Token指标增强 (price, mcap, fdv, liquidity) =====
        log.info("🔧 Setting up token metrics enrichment with broadcast");
        DataStream<ProcessEvent> enrichedEventStream = metadataEnrichedStream
            .connect(metricsBroadcastStream)
            .process(new RedisTokenMetricsBroadcaster())
            .name("Redis Token Metrics Broadcaster");
        
        // ===== Step 5: 提取账户交易事件 =====
        log.info("🔧 Setting up account trade extraction");
        DataStream<AccountTrade> accountTradeStream = enrichedEventStream
            .flatMap(new AccountTradeExtractor())
            .name("Account Trade Extractor")
            .assignTimestampsAndWatermarks(TRADE_WATERMARK_STRATEGY);
        
        // ===== Step 6: 按账户-Token分组 =====
        log.info("🔧 Setting up account trade stream keying");
        KeyedStream<AccountTrade, String> keyedTradeStream = accountTradeStream
            .keyBy(trade -> PnLProcessor.generateStateKey(trade.getAccountAddress(), trade.getTokenAddress()));
        
        // ===== Step 7: PnL状态处理 (核心移动平均成本算法) =====
        // 注意：价格信息已在AccountTrade中（由上游RedisTokenMetricsBroadcaster注入），无需BroadcastState
        log.info("🔧 Setting up PnL processing with moving average cost algorithm");
        SingleOutputStreamOperator<AccountPnLSnapshot> pnlSnapshotStream = keyedTradeStream
            .process(new PnLProcessor())
            .name("PnL Processor (Moving Average Cost)");
        
        // ===== Step 7.1: 提取已实现盈亏事件侧输出流 =====
        DataStream<PnLRealizedEvent> realizedEventStream = pnlSnapshotStream
            .getSideOutput(PnLProcessor.REALIZED_EVENT_TAG);
        
        // ===== Step 8: 输出到ClickHouse =====
        log.info("🔧 Setting up ClickHouse sink for PnL snapshots");
        pnlSnapshotStream
            .addSink(ClickHouseSink.createAccountPnLSink())
            .name("PnL Snapshot ClickHouse Sink");
        
        log.info("🔧 Setting up ClickHouse sink for realized PnL events");
        realizedEventStream
            .addSink(ClickHouseSink.createPnLRealizedEventSink())
            .name("PnL Realized Event ClickHouse Sink");
        
        // ===== 执行任务 =====
        env.execute("DeFi Account PnL Aggregator (Moving Average Cost)");
    }
}