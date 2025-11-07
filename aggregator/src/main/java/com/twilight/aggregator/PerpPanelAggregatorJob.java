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
import com.twilight.aggregator.model.perp.ExecutionMetrics;
import com.twilight.aggregator.model.perp.ContextMetrics;
import com.twilight.aggregator.model.perp.PanelMetrics;
import com.twilight.aggregator.model.perp.PerpSignal;
import com.twilight.aggregator.process.perp.ExecutionMetricsRollup;
import com.twilight.aggregator.process.perp.PanelJoiner;
import com.twilight.aggregator.process.perp.LiquidityRegimeClassifier;
import com.twilight.aggregator.process.perp.CrowdingScoreCalculator;
import com.twilight.aggregator.process.perp.TrendSignalDetector;
import com.twilight.aggregator.serialization.perp.ExecutionMetricsDeserializer;
import com.twilight.aggregator.serialization.perp.ContextMetricsDeserializer;
import com.twilight.aggregator.serialization.perp.PanelMetricsSerializer;
import com.twilight.aggregator.serialization.perp.PerpSignalSerializer;
import com.twilight.aggregator.sink.ClickHouseSink;

/**
 * 永续合约面板汇合Job（Job3 - 分钟级）
 * 
 * 数据流架构（GPT优化方案）：
 * <pre>
 * ExecutionMetrics Stream (perp.exec.1s) - Job1输出
 *   → 1min Tumbling Window
 *   → ExecutionMetricsRollup (avg/max/sum聚合)
 *   → ExecutionMetrics(1min)
 * 
 * ContextMetrics Stream (perp.ctx.1m) - Job2输出
 *   → ContextMetrics(1min)
 * 
 * Connect (Exec1min + Ctx1min)
 *   → PanelJoiner (CoProcessFunction, 时间对齐)
 *   → PanelMetrics (基础汇合面板)
 *   → LiquidityRegimeClassifier (THICK/NORMAL/THIN)
 *   → CrowdingScoreCalculator (T-Digest Z-score)
 *   → TrendSignalDetector (拥挤度/清算风险信号)
 *   ├─→ ClickHouse Sink (dws_perps_panel_1m)
 *   └─→ Kafka Sink (perp.signals) & ClickHouse Sink (perp_signals)
 * </pre>
 * 
 * 关键特性（GPT建议实现）：
 * - 从Kafka读取Job1/Job2输出（不从ClickHouse读）
 * - 1秒数据rollup到1分钟（avg/max/sum）
 * - CoProcessFunction实现快慢流join（时间对齐）
 * - T-Digest算法计算Z-score（24小时滚动窗口）
 * - 固定阈值信号检测（后续可迭代为动态分位数）
 * - 双写输出：Kafka + ClickHouse
 * 
 * 配置：
 * - 并行度：6
 * - Checkpoint：10s
 * - 水印：快流1-2s，慢流3-5s
 * 
 * 性能目标：
 * - 吞吐量：500 panels/s
 * - 端到端延迟：p95 < 10s
 */
public class PerpPanelAggregatorJob {
    private static final Logger log = LoggerFactory.getLogger(PerpPanelAggregatorJob.class);
    private static final FlinkConfig config = FlinkConfig.getInstance();
    
    // Kafka Topics - Input (Job1/Job2输出)
    private static final String EXEC_METRICS_INPUT_TOPIC = "perp.exec.1s";
    private static final String CTX_METRICS_INPUT_TOPIC = "perp.ctx.1m";
    
    // Kafka Topics - Output
    private static final String PANEL_METRICS_OUTPUT_TOPIC = "perp.panel.1m";  // Panel指标输出
    private static final String SIGNALS_OUTPUT_TOPIC = "perp.signals";  // 信号输出
    
    // 并行度配置
    private static final int PARALLELISM = 6;
    
    // 窗口大小（Exec需要rollup到1分钟）
    private static final Time ROLLUP_WINDOW_SIZE = Time.minutes(1);
    
    public static void main(String[] args) throws Exception {
        log.info("========================================");
        log.info("🚀 Starting Perpetual Contract Panel Aggregator Job (Job3)");
        log.info("========================================");
        
        // ===== Step 1: 设置执行环境 =====
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);
        
        // Checkpoint配置（汇合层：10秒）
        env.enableCheckpointing(10000);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        
        log.info("✅ Environment configured: parallelism={}, checkpoint=10s", PARALLELISM);
        
        // ===== Step 2: 配置ExecutionMetrics数据源（Job1输出） =====
        log.info("📊 Configuring ExecutionMetrics source: {}", EXEC_METRICS_INPUT_TOPIC);
        
        KafkaSource<ExecutionMetrics> execMetricsSource = KafkaSource.<ExecutionMetrics>builder()
            .setBootstrapServers(config.getKafkaBootstrapServers())
            .setTopics(EXEC_METRICS_INPUT_TOPIC)
            .setGroupId("perp-panel-aggregator-exec")
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(new ExecutionMetricsDeserializer())
            .build();
        
        WatermarkStrategy<ExecutionMetrics> execWatermarkStrategy = WatermarkStrategy
            .<ExecutionMetrics>forBoundedOutOfOrderness(Duration.ofSeconds(2))
            .withIdleness(Duration.ofMinutes(2))
            .withTimestampAssigner((event, timestamp) -> event.getEndTime());
        
        DataStream<ExecutionMetrics> execMetricsStream = env
            .fromSource(execMetricsSource, execWatermarkStrategy, "ExecutionMetrics Source (Job1 Output)")
            .name("ExecutionMetrics Source");
        
        // ===== Step 3: 配置ContextMetrics数据源（Job2输出） =====
        log.info("📊 Configuring ContextMetrics source: {}", CTX_METRICS_INPUT_TOPIC);
        
        KafkaSource<ContextMetrics> ctxMetricsSource = KafkaSource.<ContextMetrics>builder()
            .setBootstrapServers(config.getKafkaBootstrapServers())
            .setTopics(CTX_METRICS_INPUT_TOPIC)
            .setGroupId("perp-panel-aggregator-ctx")
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(new ContextMetricsDeserializer())
            .build();
        
        WatermarkStrategy<ContextMetrics> ctxWatermarkStrategy = WatermarkStrategy
            .<ContextMetrics>forBoundedOutOfOrderness(Duration.ofSeconds(5))
            .withIdleness(Duration.ofMinutes(2))
            .withTimestampAssigner((event, timestamp) -> event.getEndTime());
        
        DataStream<ContextMetrics> ctxMetricsStream = env
            .fromSource(ctxMetricsSource, ctxWatermarkStrategy, "ContextMetrics Source (Job2 Output)")
            .name("ContextMetrics Source");
        
        // ===== Step 4: Rollup ExecutionMetrics (1s → 1min) =====
        log.info("🔄 Setting up ExecutionMetrics rollup (1s → 1min)");
        
        SingleOutputStreamOperator<ExecutionMetrics> execRollupStream = execMetricsStream
            .keyBy(metrics -> metrics.getSymbol() + "@" + metrics.getExchange())
            .window(TumblingEventTimeWindows.of(ROLLUP_WINDOW_SIZE))
            .process(new ExecutionMetricsRollup())
            .name("Execution Metrics Rollup (1s → 1min)");
        
        // ===== Step 5: Join执行面和语境面 =====
        log.info("🔗 Connecting Execution and Context streams (Panel Join)");
        
        SingleOutputStreamOperator<PanelMetrics> panelStream = execRollupStream
            .connect(ctxMetricsStream)
            .keyBy(
                exec -> exec.getSymbol() + "@" + exec.getExchange(),
                ctx -> ctx.getSymbol() + "@" + ctx.getExchange()
            )
            .process(new PanelJoiner())
            .name("Panel Joiner (Exec + Ctx)");
        
        // ===== Step 6: 流动性制度分类 =====
        log.info("📐 Setting up Liquidity Regime Classification");
        
        SingleOutputStreamOperator<PanelMetrics> classifiedPanelStream = panelStream
            .process(new LiquidityRegimeClassifier())
            .name("Liquidity Regime Classifier");
        
        // ===== Step 7: 拥挤度得分计算（T-Digest） =====
        log.info("📊 Setting up Crowding Score Calculation (T-Digest)");
        
        SingleOutputStreamOperator<PanelMetrics> scoredPanelStream = classifiedPanelStream
            .keyBy(panel -> panel.getSymbol() + "@" + panel.getExchange())
            .process(new CrowdingScoreCalculator())
            .name("Crowding Score Calculator (T-Digest)");
        
        // ===== Step 8: Panel Metrics双写输出（Kafka + ClickHouse） =====
        log.info("💾 Setting up Panel Metrics dual sink: Kafka + ClickHouse");
        
        // 8.1 Kafka Sink（低延迟，给下游消费）
        KafkaSink<PanelMetrics> panelKafkaSink = KafkaSink.<PanelMetrics>builder()
            .setBootstrapServers(config.getKafkaBootstrapServers())
            .setRecordSerializer(
                KafkaRecordSerializationSchema.builder()
                    .setTopic(PANEL_METRICS_OUTPUT_TOPIC)
                    .setValueSerializationSchema(new PanelMetricsSerializer())
                    .build()
            )
            .build();
        
        scoredPanelStream
            .sinkTo(panelKafkaSink)
            .name("Kafka Sink (perp.panel.1m)");
        
        // 8.2 ClickHouse Sink（历史存储，用于回测和分析）
        scoredPanelStream
            .addSink(ClickHouseSink.createPanelMetricsSink())
            .name("ClickHouse Sink (dws_perps_panel_1m)");
        
        // ===== Step 9: 趋势信号检测 =====
        log.info("🚨 Setting up Trend Signal Detection");
        
        SingleOutputStreamOperator<PerpSignal> trendSignalStream = scoredPanelStream
            .process(new TrendSignalDetector())
            .name("Trend Signal Detector");
        
        // 信号双写：Kafka + ClickHouse
        KafkaSink<PerpSignal> signalKafkaSink = KafkaSink.<PerpSignal>builder()
            .setBootstrapServers(config.getKafkaBootstrapServers())
            .setRecordSerializer(
                KafkaRecordSerializationSchema.builder()
                    .setTopic(SIGNALS_OUTPUT_TOPIC)
                    .setValueSerializationSchema(new PerpSignalSerializer())
                    .build()
            )
            .build();
        
        trendSignalStream
            .sinkTo(signalKafkaSink)
            .name("Kafka Sink (perp.signals)");
        
        trendSignalStream
            .addSink(ClickHouseSink.createPerpSignalSink())
            .name("ClickHouse Sink (perp_signals)");
        
        // ===== 执行任务 =====
        log.info("========================================");
        log.info("✅ Pipeline setup completed");
        log.info("");
        log.info("📊 Data Flow Summary:");
        log.info("  Exec1s ({}) → 1min Rollup → ExecMetrics(1min)", EXEC_METRICS_INPUT_TOPIC);
        log.info("  Ctx1m ({}) → ContextMetrics(1min)", CTX_METRICS_INPUT_TOPIC);
        log.info("  Join → PanelMetrics → LiquidityRegime → CrowdingScore → TrendSignals");
        log.info("  Output → ClickHouse (dws_perps_panel_1m) + Kafka (perp.signals)");
        log.info("");
        log.info("⚙️  Configuration:");
        log.info("  - Parallelism: {}", PARALLELISM);
        log.info("  - Rollup Window: 1 minute");
        log.info("  - Watermark: Exec 2s, Ctx 5s");
        log.info("  - Checkpoint: 10 seconds");
        log.info("");
        log.info("🎯 Performance Target:");
        log.info("  - Throughput: 500 panels/s");
        log.info("  - Latency: p95 < 10s");
        log.info("========================================");
        log.info("🚀 Starting job execution...");
        
        env.execute("Perpetual Contract Panel Aggregator Job (Job3)");
    }
}

