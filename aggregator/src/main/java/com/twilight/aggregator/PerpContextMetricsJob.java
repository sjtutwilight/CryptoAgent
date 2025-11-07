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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twilight.aggregator.config.FlinkConfig;
import com.twilight.aggregator.model.perp.MarkIndexData;
import com.twilight.aggregator.model.perp.FundingData;
import com.twilight.aggregator.model.perp.OpenInterestData;
import com.twilight.aggregator.model.perp.ContextMetrics;
import com.twilight.aggregator.process.perp.ContextSnapshotTimer;
import com.twilight.aggregator.serialization.perp.MarkIndexDeserializer;
import com.twilight.aggregator.serialization.perp.FundingDeserializer;
import com.twilight.aggregator.serialization.perp.OpenInterestDeserializer;
import com.twilight.aggregator.serialization.perp.ContextMetricsSerializer;
import com.twilight.aggregator.sink.ClickHouseSink;

/**
 * 永续合约语境面指标Job（慢流 - 分钟级）
 * 
 * 数据流架构（采用GPT建议的分钟快照器方案）：
 * <pre>
 * Mark/Index Stream (binance.perp.mark_index)
 *   → ContextSnapshotTimer (维护最新状态)
 * 
 * Funding Stream (binance.perp.funding_rate)
 *   → ContextSnapshotTimer (维护最新状态 + 在线EMA)
 * 
 * OI Stream (binance.perp.open_interest)
 *   → ContextSnapshotTimer (维护最新状态 + 前值填充)
 * 
 * 定时器触发（每分钟整点）
 *   → ContextMetrics (basis/funding_ema/oi_delta)
 *   → ClickHouse Sink (dws_perps_ctx_1m)
 * </pre>
 * 
 * 关键特性（GPT P0修复）：
 * - 分钟快照器：使用定时器而非窗口，确保输出最新值
 * - 在线EMA：单值状态计算24h funding EMA，适应不规则更新
 * - OI前值填充：标记is_oi_carried，处理5分钟更新间隙
 * - 水印策略：5s乱序容忍，2min空闲超时
 * - Checkpoint：10s间隔
 * - 并行度：4（低吞吐）
 * 
 * 性能目标：
 * - 吞吐量：1k events/s
 * - 端到端延迟：p95 < 5s
 */
public class PerpContextMetricsJob {
    private static final Logger log = LoggerFactory.getLogger(PerpContextMetricsJob.class);
    private static final FlinkConfig config = FlinkConfig.getInstance();
    
    // Kafka Topics - Input
    private static final String MARK_INDEX_TOPIC = "perp.mark_index";  // 多交易所共用topic
    private static final String FUNDING_TOPIC = "perp.funding_rate";  // 多交易所共用topic
    private static final String OI_TOPIC = "perp.open_interest";  // 多交易所共用topic
    
    // Kafka Topics - Output
    private static final String CTX_METRICS_OUTPUT_TOPIC = "perp.ctx.1m";  // Job3输入
    
    // 并行度配置
    private static final int PARALLELISM = 4;
    
    public static void main(String[] args) throws Exception {
        log.info("========================================");
        log.info("🚀 Starting Perpetual Contract Context Metrics Job (Slow Stream)");
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
        
        // ===== Step 2: 配置Mark/Index数据源 =====
        log.info("📊 Configuring Mark/Index source: {}", MARK_INDEX_TOPIC);
        
        KafkaSource<MarkIndexData> markIndexSource = KafkaSource.<MarkIndexData>builder()
            .setBootstrapServers(config.getKafkaBootstrapServers())
            .setTopics(MARK_INDEX_TOPIC)
            .setGroupId(config.getKafkaGroupId() + "-perp-context")
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(new MarkIndexDeserializer())
            .build();
        
        // Mark/Index水印策略（慢流：5s乱序容忍）
        WatermarkStrategy<MarkIndexData> markIndexWatermark = WatermarkStrategy
            .<MarkIndexData>forBoundedOutOfOrderness(Duration.ofSeconds(1))  // 降低到1s
            .withTimestampAssigner((event, ts) -> {
                return event.getExchangeTs() != null ? event.getExchangeTs() : System.currentTimeMillis();
            })
            .withIdleness(Duration.ofSeconds(2)); // 2s空闲超时（本机测试）
        
        DataStream<MarkIndexData> markIndexStream = env
            .fromSource(markIndexSource, markIndexWatermark, "Mark/Index Source")
            .name("Mark/Index Kafka Source");
        
        // ===== Step 3: 配置Funding数据源 =====
        log.info("💵 Configuring Funding source: {}", FUNDING_TOPIC);
        
        KafkaSource<FundingData> fundingSource = KafkaSource.<FundingData>builder()
            .setBootstrapServers(config.getKafkaBootstrapServers())
            .setTopics(FUNDING_TOPIC)
            .setGroupId(config.getKafkaGroupId() + "-perp-context")
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(new FundingDeserializer())
            .build();
        
        WatermarkStrategy<FundingData> fundingWatermark = WatermarkStrategy
            .<FundingData>forBoundedOutOfOrderness(Duration.ofSeconds(1))
            .withTimestampAssigner((event, ts) -> {
                return event.getExchangeTs() != null ? event.getExchangeTs() : System.currentTimeMillis();
            })
            .withIdleness(Duration.ofSeconds(2));  // 本机测试：降低到2s
        
        DataStream<FundingData> fundingStream = env
            .fromSource(fundingSource, fundingWatermark, "Funding Source")
            .name("Funding Kafka Source");
        
        // ===== Step 4: 配置Open Interest数据源 =====
        log.info("📈 Configuring Open Interest source: {}", OI_TOPIC);
        
        KafkaSource<OpenInterestData> oiSource = KafkaSource.<OpenInterestData>builder()
            .setBootstrapServers(config.getKafkaBootstrapServers())
            .setTopics(OI_TOPIC)
            .setGroupId(config.getKafkaGroupId() + "-perp-context")
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(new OpenInterestDeserializer())
            .build();
        
        WatermarkStrategy<OpenInterestData> oiWatermark = WatermarkStrategy
            .<OpenInterestData>forBoundedOutOfOrderness(Duration.ofSeconds(1))
            .withTimestampAssigner((event, ts) -> {
                return event.getExchangeTs() != null ? event.getExchangeTs() : System.currentTimeMillis();
            })
            .withIdleness(Duration.ofSeconds(2));  // 本机测试：降低到2s
        
        DataStream<OpenInterestData> oiStream = env
            .fromSource(oiSource, oiWatermark, "OI Source")
            .name("Open Interest Kafka Source");
        
        // ===== Step 5: 合并所有流到统一处理器 =====
        log.info("🔗 Unifying all streams into ContextSnapshotTimer");
        
        // 将三个流union成一个，使用Object作为公共类型
        DataStream<Object> unifiedStream = markIndexStream
            .map(data -> (Object) data).name("Cast MarkIndex")
            .union(
                fundingStream.map(data -> (Object) data).name("Cast Funding"),
                oiStream.map(data -> (Object) data).name("Cast OI")
            );
        
        // ===== Step 6: 分钟快照器处理 =====
        log.info("🔧 Setting up Context Snapshot Timer (GPT P0 fix)");
        
        // 按symbol@exchange分组 + 定时器触发分钟快照
        SingleOutputStreamOperator<ContextMetrics> contextMetricsStream = unifiedStream
            .keyBy(obj -> {
                // 从各类型中提取symbol@exchange
                if (obj instanceof MarkIndexData) {
                    MarkIndexData data = (MarkIndexData) obj;
                    return data.getSymbol() + "@" + data.getExchange();
                } else if (obj instanceof FundingData) {
                    FundingData data = (FundingData) obj;
                    return data.getSymbol() + "@" + data.getExchange();
                } else if (obj instanceof OpenInterestData) {
                    OpenInterestData data = (OpenInterestData) obj;
                    return data.getSymbol() + "@" + data.getExchange();
                }
                return "unknown@unknown";
            })
            .process(new ContextSnapshotTimer())
            .name("Context Snapshot Timer (1min)");
        
        // ===== Step 7: 双写输出（Kafka + ClickHouse） =====
        log.info("💾 Setting up dual sink: Kafka + ClickHouse");
        
        // 7.1 Kafka Sink（低延迟，给Job3消费 - 本机测试优化）
        KafkaSink<ContextMetrics> ctxMetricsKafkaSink = KafkaSink.<ContextMetrics>builder()
            .setBootstrapServers(config.getKafkaBootstrapServers())
            .setRecordSerializer(
                KafkaRecordSerializationSchema.builder()
                    .setTopic(CTX_METRICS_OUTPUT_TOPIC)
                    .setValueSerializationSchema(new ContextMetricsSerializer())
                    .build()
            )
            .build();
        
        contextMetricsStream
            .sinkTo(ctxMetricsKafkaSink)
            .name("Kafka Sink (perp.ctx.1m)");
        
        // 7.2 ClickHouse Sink（历史存储）
        contextMetricsStream
            .addSink(ClickHouseSink.createContextMetricsSink())
            .name("ClickHouse Sink (dws_perps_ctx_1m)");
        
        // 7.3 Console输出（调试）
        contextMetricsStream
            .map(metrics -> String.format(
                "[CONTEXT] %s@%s | mark=%.2f, basis=%.2f bps, funding=%.6f, ema=%.6f, oi_delta=%.2f (%.2f%%), carried=%s",
                metrics.getSymbol(),
                metrics.getExchange(),
                metrics.getMarkPrice() != null ? metrics.getMarkPrice().doubleValue() : 0,
                metrics.getBasisBps() != null ? metrics.getBasisBps() : 0,
                metrics.getFundingRate() != null ? metrics.getFundingRate().doubleValue() : 0,
                metrics.getFundingEma24h() != null ? metrics.getFundingEma24h().doubleValue() : 0,
                metrics.getOiDelta1m() != null ? metrics.getOiDelta1m().doubleValue() : 0,
                metrics.getOiDeltaPct() != null ? metrics.getOiDeltaPct() : 0,
                metrics.getIsOiCarried()
            ))
            .print()
            .name("Context Print Sink (Debug)");
        
        // ===== 执行任务 =====
        log.info("========================================");
        log.info("✅ Pipeline setup completed");
        log.info("");
        log.info("📊 Data Flow Summary:");
        log.info("  Mark/Index ({}) → ValueState", MARK_INDEX_TOPIC);
        log.info("  Funding ({}) → ValueState + Online EMA", FUNDING_TOPIC);
        log.info("  OI ({}) → ValueState + Forward Fill", OI_TOPIC);
        log.info("  Timer (1min boundary) → ContextMetrics → ClickHouse (dws_perps_ctx_1m)");
        log.info("");
        log.info("⚙️  Configuration:");
        log.info("  - Parallelism: {}", PARALLELISM);
        log.info("  - Timer: 1 minute boundary (UTC aligned)");
        log.info("  - Watermark: 5s out-of-orderness");
        log.info("  - Checkpoint: 10 seconds");
        log.info("  - Idle Timeout: 2 minutes");
        log.info("");
        log.info("🎯 Key Features (GPT P0 Fixes):");
        log.info("  ✓ Minute Snapshot Timer (not window-based)");
        log.info("  ✓ Online EMA for funding (single-value state)");
        log.info("  ✓ OI forward fill with is_carried flag");
        log.info("");
        log.info("🎯 Performance Target:");
        log.info("  - Throughput: 1k events/s");
        log.info("  - Latency: p95 < 5s");
        log.info("========================================");
        log.info("🚀 Starting job execution...");
        
        env.execute("Perpetual Contract Context Metrics Job (Slow Stream)");
    }
}


