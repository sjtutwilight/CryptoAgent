package com.twilight.aggregator;

import java.time.Duration;
import java.util.Properties;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twilight.aggregator.config.FlinkConfig;
import com.twilight.aggregator.model.KlineData;
import com.twilight.aggregator.model.KlineSignal;
import com.twilight.aggregator.process.kline.indicators.IndicatorConfig;
import com.twilight.aggregator.process.kline.indicators.oscillator.KDJProcessor;
import com.twilight.aggregator.process.kline.indicators.oscillator.RSIProcessor;
import com.twilight.aggregator.process.kline.indicators.trend.EMAProcessor;
import com.twilight.aggregator.process.kline.indicators.trend.MACDProcessor;
import com.twilight.aggregator.process.kline.indicators.volatility.ATRProcessor;
import com.twilight.aggregator.process.kline.indicators.volatility.BollingerBandsProcessor;
import com.twilight.aggregator.serialization.KlineDataDeserializer;
import com.twilight.aggregator.serialization.KlineSignalSerializer;

/**
 * 多指标K线信号生成作业
 * 
 * 同时运行多个技术指标处理器，生成综合交易信号
 * 
 * 数据流架构：
 * <pre>
 * Kafka Source (binance.kline) 
 *   → KeyBy (symbol + interval)
 *   ├─ MACD Processor → signals
 *   ├─ RSI Processor → signals
 *   ├─ KDJ Processor → signals
 *   ├─ Bollinger Bands Processor → signals
 *   ├─ ATR Processor → signals
 *   └─ EMA(20/50/200) Processors → signals
 *   → Union All Signals
 *   → Kafka Sink (kline.signal)
 * </pre>
 * 
 * 支持的指标：
 * - 趋势类：MACD、EMA（多周期）
 * - 震荡类：RSI、KDJ
 * - 波动率类：Bollinger Bands、ATR
 * 
 * 特点：
 * - 多指标并行计算，互不干扰
 * - 统一输出到kline.signal topic
 * - 通过strategy字段区分不同指标
 * - 支持动态配置指标参数
 */
public class MultiIndicatorJob {
    private static final Logger log = LoggerFactory.getLogger(MultiIndicatorJob.class);
    private static final FlinkConfig config = FlinkConfig.getInstance();
    
    // Kafka配置
    private static final String KLINE_SOURCE_TOPIC = "binance.kline";
    private static final String SIGNAL_SINK_TOPIC = "kline.signal";
    
    public static void main(String[] args) throws Exception {
        log.info("🚀 Starting Multi-Indicator Kline Signal Generation Job");
        
        // ===== Step 1: 设置执行环境 =====
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // 设置并行度
        int parallelism = config.getParallelism();
        env.setParallelism(parallelism);
        log.info("🔧 Set parallelism to {}", parallelism);
        
        // ===== Step 2: 配置Kafka源 =====
        log.info("📨 Setting up Kafka source for kline data: {}", KLINE_SOURCE_TOPIC);
        
        KafkaSource<KlineData> klineSource = KafkaSource.<KlineData>builder()
            .setBootstrapServers(config.getKafkaBootstrapServers())
            .setTopics(KLINE_SOURCE_TOPIC)
            .setGroupId(config.getKafkaGroupId() + "-multi-indicator")
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(new KlineDataDeserializer())
            .build();
        
        // K线数据水印策略
        WatermarkStrategy<KlineData> klineWatermarkStrategy = WatermarkStrategy
            .<KlineData>forBoundedOutOfOrderness(Duration.ofSeconds(10))
            .withTimestampAssigner((kline, timestamp) -> {
                Long klineTime = kline.getStartTime();
                return klineTime != null ? klineTime : System.currentTimeMillis();
            })
            .withIdleness(Duration.ofMinutes(1));
        
        // ===== Step 3: 创建K线数据流 =====
        DataStream<KlineData> klineStream = env
            .fromSource(klineSource, klineWatermarkStrategy, "Kline Kafka Source")
            .name("Kline Source");
        
        // ===== Step 4: 按交易对分组 =====
        log.info("🔧 Setting up keyed stream by symbol + interval");
        KeyedStream<KlineData, String> keyedKlineStream = klineStream
            .keyBy(kline -> kline.getSymbol() + "_" + kline.getInterval());
        
        // ===== Step 5: 应用多个指标处理器 =====
        log.info("🔧 Setting up multiple indicator processors");
        
        // 趋势类指标
        log.info("  📈 Trend Indicators:");
        log.info("    - MACD (12, 26, 9)");
        DataStream<KlineSignal> macdSignals = keyedKlineStream
            .process(new MACDProcessor(IndicatorConfig.macdDefault()))
            .name("MACD Processor");
        
        log.info("    - EMA (20, 50, 200)");
        DataStream<KlineSignal> ema20Signals = keyedKlineStream
            .process(new EMAProcessor(20))
            .name("EMA20 Processor");
        
        DataStream<KlineSignal> ema50Signals = keyedKlineStream
            .process(new EMAProcessor(50))
            .name("EMA50 Processor");
        
        DataStream<KlineSignal> ema200Signals = keyedKlineStream
            .process(new EMAProcessor(200))
            .name("EMA200 Processor");
        
        // 震荡类指标
        log.info("  📊 Oscillator Indicators:");
        log.info("    - RSI (14)");
        DataStream<KlineSignal> rsiSignals = keyedKlineStream
            .process(new RSIProcessor(IndicatorConfig.rsiDefault()))
            .name("RSI Processor");
        
        log.info("    - KDJ (9, 3, 3)");
        DataStream<KlineSignal> kdjSignals = keyedKlineStream
            .process(new KDJProcessor(IndicatorConfig.kdjDefault()))
            .name("KDJ Processor");
        
        // 波动率类指标
        log.info("  📉 Volatility Indicators:");
        log.info("    - Bollinger Bands (20, 2.0)");
        DataStream<KlineSignal> bbSignals = keyedKlineStream
            .process(new BollingerBandsProcessor(IndicatorConfig.bollingerDefault()))
            .name("Bollinger Bands Processor");
        
        log.info("    - ATR (14)");
        DataStream<KlineSignal> atrSignals = keyedKlineStream
            .process(new ATRProcessor(IndicatorConfig.atrDefault()))
            .name("ATR Processor");
        
        // ===== Step 6: 合并所有信号流 =====
        log.info("🔀 Unioning all indicator signals");
        DataStream<KlineSignal> allSignals = macdSignals
            .union(ema20Signals, ema50Signals, ema200Signals)
            .union(rsiSignals, kdjSignals)
            .union(bbSignals, atrSignals);
        
        // ===== Step 7: 输出到Kafka =====
        log.info("📤 Setting up Kafka sink for signals: {}", SIGNAL_SINK_TOPIC);
        
        // 创建Kafka生产者配置
        Properties producerProps = new Properties();
        producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, 
                                  config.getKafkaBootstrapServers());
        producerProps.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "900000");
        
        // 构建Kafka Sink
        KafkaSink<KlineSignal> kafkaSink = KafkaSink.<KlineSignal>builder()
            .setBootstrapServers(config.getKafkaBootstrapServers())
            .setRecordSerializer(
                KafkaRecordSerializationSchema.builder()
                    .setTopic(SIGNAL_SINK_TOPIC)
                    .setValueSerializationSchema(new KlineSignalSerializer())
                    .build()
            )
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .setKafkaProducerConfig(producerProps)
            .build();
        
        // 添加Print Sink用于调试
        allSignals
            .map(signal -> String.format(
                "[%s] %s %s - %s | Price: %.4f | Strength: %.2f | %s",
                signal.getStrategy(),
                signal.getSymbol(),
                signal.getInterval(),
                signal.getSignalType(),
                signal.getCurrentPrice(),
                signal.getSignalStrength(),
                signal.getSignalDetail()
            ))
            .print()
            .name("Signal Print Sink (Debug)");
        
        // Kafka Sink输出信号
        allSignals
            .sinkTo(kafkaSink)
            .name("Signal Kafka Sink");
        
        // ===== 执行任务 =====
        log.info("✅ Multi-Indicator Pipeline setup completed");
        log.info("");
        log.info("📊 Multi-Indicator Job Summary:");
        log.info("┌─────────────────────────────────────────────────┐");
        log.info("│              Indicator List                     │");
        log.info("├─────────────────────────────────────────────────┤");
        log.info("│ Trend:                                          │");
        log.info("│   • MACD (12,26,9) - 趋势动量指标                │");
        log.info("│   • EMA20 - 短期趋势                             │");
        log.info("│   • EMA50 - 中期趋势                             │");
        log.info("│   • EMA200 - 长期趋势                            │");
        log.info("├─────────────────────────────────────────────────┤");
        log.info("│ Oscillator:                                     │");
        log.info("│   • RSI (14) - 超买超卖指标                      │");
        log.info("│   • KDJ (9,3,3) - 随机震荡指标                   │");
        log.info("├─────────────────────────────────────────────────┤");
        log.info("│ Volatility:                                     │");
        log.info("│   • Bollinger Bands (20,2) - 波动通道           │");
        log.info("│   • ATR (14) - 真实波幅                          │");
        log.info("└─────────────────────────────────────────────────┘");
        log.info("");
        log.info("🎯 Data Flow:");
        log.info("  Kafka ({}) → 8 Indicators → Kafka ({})", 
                KLINE_SOURCE_TOPIC, SIGNAL_SINK_TOPIC);
        log.info("");
        log.info("🚀 Starting job execution: Multi-Indicator Job");
        
        env.execute("Multi-Indicator Kline Signal Generation Job");
    }
}

