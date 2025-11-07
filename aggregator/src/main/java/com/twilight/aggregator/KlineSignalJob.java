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
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twilight.aggregator.config.FlinkConfig;
import com.twilight.aggregator.model.KlineData;
import com.twilight.aggregator.model.KlineSignal;
import com.twilight.aggregator.model.KlineMetrics;
import com.twilight.aggregator.model.IndicatorMetric;
import com.twilight.aggregator.process.kline.MultipleMAProcessor;
import com.twilight.aggregator.process.kline.IndicatorOutputTags;
import com.twilight.aggregator.process.kline.indicators.oscillator.RSIProcessor;
import com.twilight.aggregator.process.kline.indicators.oscillator.KDJProcessor;
import com.twilight.aggregator.process.kline.indicators.trend.MACDProcessor;
import com.twilight.aggregator.process.kline.indicators.trend.EMAProcessor;
import com.twilight.aggregator.process.kline.indicators.volatility.BollingerBandsProcessor;
import com.twilight.aggregator.process.kline.indicators.volatility.ATRProcessor;
import com.twilight.aggregator.serialization.KlineDataDeserializer;
import com.twilight.aggregator.serialization.KlineSignalSerializer;
import com.twilight.aggregator.source.KlineDataGenerator;
import com.twilight.aggregator.sink.ClickHouseSink;

/**
 * K线信号生成作业 - 基于多重移动平均策略
 * 
 * 数据流：
 * 1. Kafka Source (binance.kline) -> KlineData
 * 2. 按symbol分组 -> KeyBy(symbol)
 * 3. 多重移动平均策略处理 -> MultipleMAProcessor -> KlineSignal
 * 4. Kafka Sink (kline.signal) -> 输出交易信号
 * 
 * 特点：
 * - 实时处理K线数据，计算多重移动平均线
 * - 检测金叉/死叉信号，生成买入/卖出建议
 * - 支持多交易对并行处理
 * - 有状态处理，为每个交易对维护价格历史
 */
public class KlineSignalJob {
    private static final Logger log = LoggerFactory.getLogger(KlineSignalJob.class);
    private static final FlinkConfig config = FlinkConfig.getInstance();
    
    // Kafka配置
    private static final String KLINE_SOURCE_TOPIC = "binance.kline";
    private static final String SIGNAL_SINK_TOPIC = "kline.signal";
    
    // MA策略参数（可通过配置文件调整）
    private static final int SHORT_MA_PERIOD = 5;   // 短期MA周期
    private static final int MEDIUM_MA_PERIOD = 10;  // 中期MA周期
    private static final int LONG_MA_PERIOD = 20;    // 长期MA周期
    
    public static void main(String[] args) throws Exception {
        log.info("🚀 Starting Kline Signal Generation Job (Multiple MA Strategy)");
        
        // ===== Step 1: 设置执行环境 =====
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // 设置并行度
        int parallelism = config.getParallelism();
        env.setParallelism(parallelism);
        log.info("🔧 Set parallelism to {}", parallelism);
        
        // ===== Step 2: 使用DataGen源生成模拟K线数据（用于测试）=====
        log.info("📨 Setting up DataGen source for kline data (TEST MODE)");
        
        // 创建K线数据生成器 - 上涨趋势便于观察MA交叉信号
        // KlineDataGenerator klineGenerator = new KlineDataGenerator(
        //     "binance", 
        //     "BTCUSDT", 
        //     "1m", 
        //     KlineDataGenerator.TrendType.UPTREND
        // );
        
        // K线数据水印策略
        WatermarkStrategy<KlineData> klineWatermarkStrategy = WatermarkStrategy
            .<KlineData>forBoundedOutOfOrderness(Duration.ofSeconds(10)) // K线数据允许10秒乱序
            .withTimestampAssigner((kline, timestamp) -> {
                // 使用K线开始时间作为事件时间
                Long klineTime = kline.getStartTime();
                return klineTime != null ? klineTime : System.currentTimeMillis();
            })
            .withIdleness(Duration.ofMinutes(1)); // 1分钟无数据则标记空闲
        
        // // ===== Step 3: 创建K线数据流（使用DataGen）=====
        // DataStream<KlineData> klineStream = env
        //     .addSource(klineGenerator)
        //     .name("Kline DataGen Source")
        //     .assignTimestampsAndWatermarks(klineWatermarkStrategy);
        
       
        log.info("📨 Setting up Kafka source for kline data: {}", KLINE_SOURCE_TOPIC);
        KafkaSource<KlineData> klineSource = KafkaSource.<KlineData>builder()
            .setBootstrapServers(config.getKafkaBootstrapServers())
            .setTopics(KLINE_SOURCE_TOPIC)
            .setGroupId(config.getKafkaGroupId() + "-kline-signal")
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(new KlineDataDeserializer())
            .build();
        
        DataStream<KlineData> klineStream = env
            .fromSource(klineSource, klineWatermarkStrategy, "Kline Kafka Source")
            .name("Kline Source");
    
        
        // ===== Step 4: 按交易对分组 =====
        log.info("🔧 Setting up keyed stream by symbol");
        KeyedStream<KlineData, String> keyedKlineStream = klineStream
            .keyBy(kline -> kline.getSymbol() + "_" + kline.getInterval()); // 按symbol+interval分组
        
        // ===== Step 5: 指标处理链 =====
        log.info("🔧 Setting up Multiple MA Processor (short={}, medium={}, long={})", 
                 SHORT_MA_PERIOD, MEDIUM_MA_PERIOD, LONG_MA_PERIOD);
        SingleOutputStreamOperator<KlineSignal> maSignalStream = keyedKlineStream
            .process(new MultipleMAProcessor(SHORT_MA_PERIOD, MEDIUM_MA_PERIOD, LONG_MA_PERIOD))
            .name("Multiple MA Processor");

        log.info("🧮 Enabling oscillator/trend/volatility processors (RSI, MACD, EMA, BOLL, ATR, KDJ)");
        SingleOutputStreamOperator<KlineSignal> rsiSignalStream = keyedKlineStream
            .process(new RSIProcessor())
            .name("RSI Processor");

        SingleOutputStreamOperator<KlineSignal> macdSignalStream = keyedKlineStream
            .process(new MACDProcessor())
            .name("MACD Processor");

        SingleOutputStreamOperator<KlineSignal> emaSignalStream = keyedKlineStream
            .process(new EMAProcessor(20))
            .name("EMA20 Processor");

        SingleOutputStreamOperator<KlineSignal> bollingerSignalStream = keyedKlineStream
            .process(new BollingerBandsProcessor())
            .name("Bollinger Bands Processor");

        SingleOutputStreamOperator<KlineSignal> atrSignalStream = keyedKlineStream
            .process(new ATRProcessor())
            .name("ATR Processor");

        SingleOutputStreamOperator<KlineSignal> kdjSignalStream = keyedKlineStream
            .process(new KDJProcessor())
            .name("KDJ Processor");

        DataStream<KlineSignal> combinedSignalStream = maSignalStream
            .union(rsiSignalStream, macdSignalStream, emaSignalStream, bollingerSignalStream, atrSignalStream, kdjSignalStream);

        DataStream<KlineMetrics> metricsStream = maSignalStream
            .getSideOutput(IndicatorOutputTags.KLINE_METRICS_TAG);

        DataStream<IndicatorMetric> indicatorMetricsStream = maSignalStream
            .getSideOutput(IndicatorOutputTags.INDICATOR_METRICS_TAG)
            .union(
                rsiSignalStream.getSideOutput(IndicatorOutputTags.INDICATOR_METRICS_TAG),
                macdSignalStream.getSideOutput(IndicatorOutputTags.INDICATOR_METRICS_TAG),
                emaSignalStream.getSideOutput(IndicatorOutputTags.INDICATOR_METRICS_TAG),
                bollingerSignalStream.getSideOutput(IndicatorOutputTags.INDICATOR_METRICS_TAG),
                atrSignalStream.getSideOutput(IndicatorOutputTags.INDICATOR_METRICS_TAG),
                kdjSignalStream.getSideOutput(IndicatorOutputTags.INDICATOR_METRICS_TAG)
            );
        
        // ===== Step 6: 输出到Kafka - 信号主题 =====
        log.info("📤 Setting up Kafka sink for signals: {}", SIGNAL_SINK_TOPIC);
        
        // 创建Kafka生产者配置
        Properties producerProps = new Properties();
        producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, 
                                  config.getKafkaBootstrapServers());
        producerProps.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "900000"); // 15分钟
        
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
        
        // 添加Print Sink用于调试（观察生成的信号）
        combinedSignalStream
            .map(signal -> String.format(
                "[SIGNAL] %s %s - %s | Price: %.2f | Strength: %.2f | Detail: %s",
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
        combinedSignalStream
            .sinkTo(kafkaSink)
            .name("Signal Kafka Sink");

        // ===== Step 7: 输出到ClickHouse - 指标落库 =====
        log.info("💾 Setting up ClickHouse sink for kline metrics (kline_metrics)");
        metricsStream
            .addSink(ClickHouseSink.createKlineMetricsSink())
            .name("ClickHouse Sink (kline_metrics)");

        log.info("💾 Setting up ClickHouse sink for indicator metrics (kline_indicator_metrics)");
        indicatorMetricsStream
            .addSink(ClickHouseSink.createKlineIndicatorMetricsSink())
            .name("ClickHouse Sink (kline_indicator_metrics)");
        
        // ===== 执行任务 =====
        log.info("✅ Kline Signal Pipeline setup completed");
        log.info("📊 KlineSignalJob pipeline summary (TEST MODE - Using DataGen):");
        log.info("  ├─ DataGen Source (模拟BTCUSDT上涨趋势)");
        log.info("  ├─ KeyBy (symbol + interval)");
        log.info("  ├─ Multiple MA Processor (MA5/MA10/MA20)");
        log.info("  ├─ RSI/MACD/EMA/BOLL/ATR/KDJ processors");
        log.info("  ├─ Kafka Sink ({})", SIGNAL_SINK_TOPIC);
        log.info("  ├─ ClickHouse Sink (kline_metrics)");
        log.info("  └─ ClickHouse Sink (kline_indicator_metrics)");
        log.info("");
        log.info("🎯 策略说明:");
        log.info("  📈 短期MA: {} 周期", SHORT_MA_PERIOD);
        log.info("  📊 中期MA: {} 周期", MEDIUM_MA_PERIOD);
        log.info("  📉 长期MA: {} 周期", LONG_MA_PERIOD);
        log.info("  💡 买入信号: 短期MA上穿中期MA，且中期MA在长期MA之上");
        log.info("  ⚠️  卖出信号: 短期MA下穿中期MA，或中期MA下穿长期MA");
        log.info("");
        log.info("⚠️  注意: 当前使用DataGen模拟数据源，生产环境请切换到Kafka源");
        log.info("🚀 Starting job execution: Kline Signal Generation Job (TEST MODE)");
        
        env.execute("Kline Signal & Indicator Pipeline");
    }
}
