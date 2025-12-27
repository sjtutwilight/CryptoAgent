package com.twilight.aggregator;

import java.time.Duration;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twilight.aggregator.config.FlinkConfig;
import com.twilight.aggregator.model.perp.OrderBookData;
import com.twilight.aggregator.model.perp.TradeData;
import com.twilight.aggregator.model.perp.OrderBookSummary;
import com.twilight.aggregator.model.perp.TradesSummary;
import com.twilight.aggregator.model.perp.ExecutionMetrics;
import com.twilight.aggregator.model.perp.PerpSignal;
import com.twilight.aggregator.process.perp.OrderBookProcessor;
import com.twilight.aggregator.process.perp.TradesAggregator;
import com.twilight.aggregator.process.perp.ExecutionMetricsBuilder;
import com.twilight.aggregator.process.perp.ExecutionSignalDetector;
import com.twilight.aggregator.serialization.perp.OrderBookDeserializer;
import com.twilight.aggregator.serialization.perp.TradeDeserializer;
import com.twilight.aggregator.serialization.perp.ExecutionMetricsSerializer;
import com.twilight.aggregator.serialization.perp.PerpSignalSerializer;
import com.twilight.aggregator.sink.ClickHouseSink;

/**
 * 永续合约执行面指标Job（快流 - 秒级）
 * 
 * 数据流架构：
 * <pre>
 * OrderBook Stream (binance.perp.orderbook)
 *   → OrderBookProcessor (重建订单簿，维护Top-N)
 *   → 1s Tumbling Window
 *   → OrderBookSummary (spread/depth/impact/imbalance)
 * 
 * Trades Stream (binance.perp.trades)
 *   → 1s Tumbling Window
 *   → TradesAggregator (volume/vwap/buy-sell ratio)
 *   → TradesSummary
 * 
 * Connect (OrderBookSummary + TradesSummary)
 *   → ExecutionMetricsBuilder (组装完整指标，含OFI)
 *   → ExecutionMetrics
 *   ├─→ ClickHouse Sink (dws_exec_1s)
 *   └─→ ExecutionSignalDetector → ClickHouse Sink (perp_signals)
 * </pre>
 * 
 * 关键特性：
 * - L1版OFI计算（符合Kyle/Cont-Kukanov-Stoikov定义）
 * - Top-N订单簿（控制内存，200档）
 * - CoProcessFunction关联（低延迟）
 * - 水印策略：300ms乱序容忍，60s空闲超时
 * - Checkpoint：5s间隔
 * - 并行度：12（高吞吐）
 * 
 * 性能目标：
 * - 吞吐量：10k events/s
 * - 端到端延迟：p95 < 1s
 */
public class PerpExecutionMetricsJob {
    private static final Logger log = LoggerFactory.getLogger(PerpExecutionMetricsJob.class);
    private static final FlinkConfig config = FlinkConfig.getInstance();
    
    // Kafka Topics - Input
    private static final String ORDERBOOK_TOPIC = "perp.orderbook";  // 多交易所共用topic（通过exchange区分）
    private static final String TRADES_TOPIC = "perp.trades";  // 多交易所共用topic
    
    // Kafka Topics - Output
    private static final String EXEC_METRICS_OUTPUT_TOPIC = "perp.exec.1s";  // Job3输入
    private static final String SIGNALS_OUTPUT_TOPIC = "perp.signals";  // 信号输出
    
    // 窗口大小
    private static final Time WINDOW_SIZE = Time.seconds(1);
    
    public static void main(String[] args) throws Exception {
        log.info("========================================");
        log.info("🚀 Starting Perpetual Contract Execution Metrics Job (Fast Stream)");
        log.info("========================================");
        
        // ===== Step 1: 设置执行环境 =====
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // 本机测试：降低并行度和checkpoint频率
        env.setParallelism(2);  // 降低到2，减少资源占用
        
        // Checkpoint配置（本机测试：30秒，减少磁盘IO）
        env.enableCheckpointing(30000);  // 30秒checkpoint
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(15000);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        
        log.info("✅ Environment configured: parallelism=2 (local test), checkpoint=30s");
        
        // ===== Step 2: 配置OrderBook数据源 =====
        log.info("📊 Configuring OrderBook source: {}", ORDERBOOK_TOPIC);
        
        KafkaSource<OrderBookData> orderbookSource = KafkaSource.<OrderBookData>builder()
            .setBootstrapServers(config.getKafkaBootstrapServers())
            .setTopics(ORDERBOOK_TOPIC)
            .setGroupId(config.getKafkaGroupId() + "-perp-exec")
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(new OrderBookDeserializer())
            .build();
        
        // OrderBook水印策略（快流：300ms乱序容忍）
        WatermarkStrategy<OrderBookData> orderbookWatermark = WatermarkStrategy
            .<OrderBookData>forBoundedOutOfOrderness(Duration.ofMillis(300))
            .withTimestampAssigner((event, ts) -> {
                return event.getExchangeTs() != null ? event.getExchangeTs() : System.currentTimeMillis();
            })
            .withIdleness(Duration.ofSeconds(5)); // 5s空闲超时
        
        DataStream<OrderBookData> orderbookStream = env
            .fromSource(orderbookSource, orderbookWatermark, "OrderBook Source")
            .name("OrderBook Kafka Source");
        
        // ===== Step 3: 配置Trades数据源 =====
        log.info("💰 Configuring Trades source: {}", TRADES_TOPIC);
        
        KafkaSource<TradeData> tradesSource = KafkaSource.<TradeData>builder()
            .setBootstrapServers(config.getKafkaBootstrapServers())
            .setTopics(TRADES_TOPIC)
            .setGroupId(config.getKafkaGroupId() + "-perp-exec")
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(new TradeDeserializer())
            .build();
        
        // Trades水印策略（与OrderBook一致）
        WatermarkStrategy<TradeData> tradesWatermark = WatermarkStrategy
            .<TradeData>forBoundedOutOfOrderness(Duration.ofMillis(300))
            .withTimestampAssigner((event, ts) -> {
                return event.getExchangeTs() != null ? event.getExchangeTs() : System.currentTimeMillis();
            })
            .withIdleness(Duration.ofSeconds(5));
        
        DataStream<TradeData> tradesStream = env
            .fromSource(tradesSource, tradesWatermark, "Trades Source")
            .name("Trades Kafka Source");
        
        // ===== Step 4: OrderBook 处理流 =====
        log.info("🔧 Setting up OrderBook processing pipeline");
        
        // 按symbol分组 + 1秒滚动窗口 + OrderBook指标计算
        SingleOutputStreamOperator<OrderBookSummary> orderbookSummaryStream = orderbookStream
            .keyBy(ob -> ob.getSymbol())
            .window(TumblingEventTimeWindows.of(WINDOW_SIZE))
            .process(new OrderBookProcessor())
            .name("OrderBook Processor (1s Window)");
        
        // ===== Step 5: Trades 处理流 =====
        log.info("🔧 Setting up Trades processing pipeline");
        
        // 按symbol分组 + 1秒滚动窗口 + Trades指标聚合
        SingleOutputStreamOperator<TradesSummary> tradesSummaryStream = tradesStream
            .keyBy(trade -> trade.getSymbol())
            .window(TumblingEventTimeWindows.of(WINDOW_SIZE))
            .process(new TradesAggregator())
            .name("Trades Aggregator (1s Window)");
        
        // ===== Step 6: 连接OrderBook和Trades流 =====
        log.info("🔗 Connecting OrderBook and Trades streams");
        
        // 使用CoProcessFunction连接（确保在同一1秒窗口内）
        SingleOutputStreamOperator<ExecutionMetrics> executionMetricsStream = orderbookSummaryStream
            .connect(tradesSummaryStream)
            .keyBy(
                summary -> summary.getSymbol(),
                summary -> summary.getSymbol()
            )
            .process(new ExecutionMetricsBuilder())
            .name("Execution Metrics Builder");
        
        // ===== Step 7: 双写输出（Kafka + ClickHouse） =====
        log.info("💾 Setting up dual sink: Kafka + ClickHouse");
        
        // 7.1 Kafka Sink（低延迟，给Job3消费）
        KafkaSink<ExecutionMetrics> execMetricsKafkaSink = KafkaSink.<ExecutionMetrics>builder()
            .setBootstrapServers(config.getKafkaBootstrapServers())
            .setRecordSerializer(
                KafkaRecordSerializationSchema.builder()
                    .setTopic(EXEC_METRICS_OUTPUT_TOPIC)
                    .setValueSerializationSchema(new ExecutionMetricsSerializer())
                    .build()
            )
            .build();
        
        executionMetricsStream
            .sinkTo(execMetricsKafkaSink)
            .name("Kafka Sink (perp.exec.1s)");
        
        // 7.2 ClickHouse Sink（历史存储，用于查询回测）
        executionMetricsStream
            .addSink(ClickHouseSink.createExecutionMetricsSink())
            .name("ClickHouse Sink (dws_exec_1s)");
        
        // 7.3 Console输出（调试观测）
        executionMetricsStream
            .map(metrics -> String.format(
                "[EXEC_METRICS] %s@%s | time=%s | mid=%.2f, spread=%.2f bps, depth_50k=%.2f, impact_50k=%.2f bps, " +
                "ofi=%.4f | trades=%d, vol=%.2f, vwap=%.2f, buy=%.2f, sell=%.2f",
                metrics.getSymbol(),
                metrics.getExchange(),
                new java.sql.Timestamp(metrics.getEndTime()),
                metrics.getMidPrice() != null ? metrics.getMidPrice().doubleValue() : 0,
                metrics.getSpreadBps() != null ? metrics.getSpreadBps() : 0,
                metrics.getDepth50k() != null ? metrics.getDepth50k().doubleValue() : 0,
                metrics.getImpact50kBps() != null ? metrics.getImpact50kBps() : 0,
                metrics.getOfi() != null ? metrics.getOfi() : 0,
                metrics.getTradeCount() != null ? metrics.getTradeCount() : 0,
                metrics.getVolumeUsd() != null ? metrics.getVolumeUsd().doubleValue() : 0,
                metrics.getVwap() != null ? metrics.getVwap().doubleValue() : 0,
                metrics.getBuyVolumeUsd() != null ? metrics.getBuyVolumeUsd().doubleValue() : 0,
                metrics.getSellVolumeUsd() != null ? metrics.getSellVolumeUsd().doubleValue() : 0
            ))
            .print()
            .name("Execution Metrics Console Output");
        
        // ===== Step 8: 信号检测与双写 =====
        log.info("🚨 Setting up signal detection");
        
        SingleOutputStreamOperator<PerpSignal> signalStream = executionMetricsStream
            .process(new ExecutionSignalDetector())
            .name("Execution Signal Detector");
        
        // 8.1 Kafka Sink（实时信号，供下游消费）
        KafkaSink<PerpSignal> signalKafkaSink = KafkaSink.<PerpSignal>builder()
            .setBootstrapServers(config.getKafkaBootstrapServers())
            .setRecordSerializer(
                KafkaRecordSerializationSchema.builder()
                    .setTopic(SIGNALS_OUTPUT_TOPIC)
                    .setValueSerializationSchema(new PerpSignalSerializer())
                    .build()
            )
            .build();
        
        signalStream
            .sinkTo(signalKafkaSink)
            .name("Kafka Sink (perp.signals)");
        
        // 8.2 ClickHouse Sink（历史信号存储）
        signalStream
            .addSink(ClickHouseSink.createPerpSignalSink())
            .name("ClickHouse Sink (perp_signals)");
        
        // 8.3 信号打印（调试）
        signalStream
            .map(signal -> String.format(
                "[SIGNAL] %s@%s %s - %s | %s | metric=%.2f, threshold=%.2f | %s",
                signal.getSymbol(),
                signal.getExchange(),
                signal.getSignalType(),
                signal.getSignalLevel(),
                signal.getMetricName(),
                signal.getMetricValue(),
                signal.getThreshold(),
                signal.getSignalDetail()
            ))
            .print()
            .name("Signal Print Sink (Debug)");
        
        // ===== 执行任务 =====
        log.info("========================================");
        log.info("✅ Pipeline setup completed");
        log.info("");
        log.info("📊 Data Flow Summary:");
        log.info("  OrderBook ({}) → 1s Window → OrderBookSummary", ORDERBOOK_TOPIC);
        log.info("  Trades ({}) → 1s Window → TradesSummary", TRADES_TOPIC);
        log.info("  Connect → ExecutionMetrics → ClickHouse (dws_exec_1s)");
        log.info("  ExecutionMetrics → Signal Detector → ClickHouse (perp_signals)");
        log.info("");
        log.info("⚙️  Configuration:");
        log.info("  - Parallelism: 2 (local test)");
        log.info("  - Window: 1 second tumbling");
        log.info("  - Watermark: 300ms out-of-orderness");
        log.info("  - Checkpoint: 5 seconds");
        log.info("  - Idle Timeout: 60 seconds");
        log.info("");
        log.info("🎯 Performance Target:");
        log.info("  - Throughput: 10k events/s");
        log.info("  - Latency: p95 < 1s");
        log.info("========================================");
        log.info("🚀 Starting job execution...");
        
        env.execute("Perpetual Contract Execution Metrics Job (Fast Stream)");
    }
}

