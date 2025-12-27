package com.twilight.aggregator.process.kline;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twilight.aggregator.model.KlineData;
import com.twilight.aggregator.model.KlineMetrics;
import com.twilight.aggregator.model.IndicatorMetric;
import com.twilight.aggregator.model.KlineSignal;
import com.twilight.aggregator.process.kline.IndicatorOutputTags;

/**
 * 多重移动平均策略处理器
 * 
 * 策略说明：
 * 使用三条移动平均线（短期、中期、长期）生成交易信号
 * - 短期MA（默认5周期）
 * - 中期MA（默认10周期）
 * - 长期MA（默认20周期）
 * 
 * 信号规则：
 * 1. 买入信号：短期MA上穿中期MA，且中期MA在长期MA之上
 * 2. 卖出信号：短期MA下穿中期MA，或中期MA下穿长期MA
 * 3. 持有信号：无明确趋势变化
 * 
 * 状态管理：
 * - 为每个交易对维护价格队列，用于计算移动平均
 * - 记录上一次的MA值，用于判断金叉/死叉
 */
public class MultipleMAProcessor extends KeyedProcessFunction<String, KlineData, KlineSignal> {
    private static final long serialVersionUID = 1L;
    private static final Logger log = LoggerFactory.getLogger(MultipleMAProcessor.class);

    // MA周期配置
    private final int shortPeriod;   // 短期MA周期
    private final int mediumPeriod;  // 中期MA周期
    private final int longPeriod;    // 长期MA周期
    
    // EMA周期配置（用于kline_metrics表）
    private static final int EMA_SHORT_PERIOD = 12;  // 短期EMA周期
    private static final int EMA_LONG_PERIOD = 26;   // 长期EMA周期
    
    // 状态：价格队列（用于计算MA）
    private transient ValueState<Deque<BigDecimal>> priceQueueState;
    
    // 状态：上一次的MA值（用于判断交叉）
    private transient ValueState<MAValues> lastMAState;
    
    // 状态：EMA值（用于计算指数移动平均）
    private transient ValueState<EMAValues> lastEMAState;
    
    /**
     * 构造函数（使用默认MA周期）
     */
    public MultipleMAProcessor() {
        this(5, 10, 20);
    }

    private Map<String, Object> buildStrategyParams(BigDecimal shortMA, BigDecimal mediumMA, BigDecimal longMA) {
        Map<String, Object> strategyParams = new HashMap<>();
        strategyParams.put("ma_short", shortPeriod);
        strategyParams.put("ma_medium", mediumPeriod);
        strategyParams.put("ma_long", longPeriod);
        strategyParams.put("ma_short_value", shortMA != null ? shortMA.doubleValue() : null);
        strategyParams.put("ma_medium_value", mediumMA != null ? mediumMA.doubleValue() : null);
        strategyParams.put("ma_long_value", longMA != null ? longMA.doubleValue() : null);
        return strategyParams;
    }

    /**
     * 构造函数（自定义MA周期）
     */
    public MultipleMAProcessor(int shortPeriod, int mediumPeriod, int longPeriod) {
        this.shortPeriod = shortPeriod;
        this.mediumPeriod = mediumPeriod;
        this.longPeriod = longPeriod;
        
        if (shortPeriod >= mediumPeriod || mediumPeriod >= longPeriod) {
            throw new IllegalArgumentException(
                "MA周期必须满足: shortPeriod < mediumPeriod < longPeriod");
        }
    }
    
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        
        // 初始化价格队列状态
        ValueStateDescriptor<Deque<BigDecimal>> priceQueueDescriptor = 
            new ValueStateDescriptor<>(
                "price-queue",
                TypeInformation.of(new TypeHint<Deque<BigDecimal>>() {})
            );
        priceQueueState = getRuntimeContext().getState(priceQueueDescriptor);
        
        // 初始化上一次MA值状态
        ValueStateDescriptor<MAValues> lastMADescriptor = 
            new ValueStateDescriptor<>(
                "last-ma-values",
                TypeInformation.of(MAValues.class)
            );
        lastMAState = getRuntimeContext().getState(lastMADescriptor);
        
        // 初始化EMA值状态
        ValueStateDescriptor<EMAValues> lastEMADescriptor = 
            new ValueStateDescriptor<>(
                "last-ema-values",
                TypeInformation.of(EMAValues.class)
            );
        lastEMAState = getRuntimeContext().getState(lastEMADescriptor);
        
        log.info("MultipleMAProcessor initialized with periods: short={}, medium={}, long={}, emaShort={}, emaLong={}", 
                 shortPeriod, mediumPeriod, longPeriod, EMA_SHORT_PERIOD, EMA_LONG_PERIOD);
    }
    
    @Override
    public void processElement(
            KlineData klineData,
            Context ctx,
            Collector<KlineSignal> out) throws Exception {
        
        // 快速验证模式：处理所有K线数据（包括未完成的）
        // 生产环境应该只处理已完成的K线：if (!klineData.isClosed()) return;
        // if (!klineData.isClosed()) {
        //     return;
        // }
        
        BigDecimal closePrice = klineData.getClosePrice();
        if (closePrice == null || closePrice.compareTo(BigDecimal.ZERO) <= 0) {
            log.warn("Invalid close price for {}: {}", klineData.getSymbol(), closePrice);
            return;
        }
        
        // 获取或初始化价格队列
        Deque<BigDecimal> priceQueue = priceQueueState.value();
        if (priceQueue == null) {
            priceQueue = new ArrayDeque<>(longPeriod);
        }
        
        // 添加新价格到队列
        priceQueue.addLast(closePrice);
        
        // 保持队列长度不超过最长MA周期
        while (priceQueue.size() > longPeriod) {
            priceQueue.removeFirst();
        }
        
        // 更新状态
        priceQueueState.update(priceQueue);
        
        // 数据不足，无法计算长期MA，暂不生成信号
        if (priceQueue.size() < longPeriod) {
            log.debug("Insufficient data for {}: {}/{} prices collected", 
                     klineData.getSymbol(), priceQueue.size(), longPeriod);
            return;
        }
        
        // 计算三条MA
        BigDecimal shortMA = calculateMA(priceQueue, shortPeriod);
        BigDecimal mediumMA = calculateMA(priceQueue, mediumPeriod);
        BigDecimal longMA = calculateMA(priceQueue, longPeriod);
        
        // 计算EMA（短期和长期）
        EMAValues lastEMA = lastEMAState.value();
        BigDecimal emaShort = calculateEMA(closePrice, lastEMA != null ? lastEMA.emaShort : null, EMA_SHORT_PERIOD);
        BigDecimal emaLong = calculateEMA(closePrice, lastEMA != null ? lastEMA.emaLong : null, EMA_LONG_PERIOD);
        
        // 更新EMA状态
        EMAValues currentEMA = new EMAValues(emaShort, emaLong);
        lastEMAState.update(currentEMA);
        
        // 获取上一次的MA值
        MAValues lastMA = lastMAState.value();
        
        // 生成交易信号及指标
        SignalResult signalResult = evaluateSignal(
            klineData, 
            shortMA, mediumMA, longMA, 
            lastMA,
            closePrice
        );
        
        // 更新上一次MA值
        MAValues currentMA = new MAValues(shortMA, mediumMA, longMA);
        lastMAState.update(currentMA);

        if (signalResult != null) {
            KlineMetrics metrics = KlineMetrics.builder()
                    .exchange(klineData.getExchange())
                    .symbol(klineData.getSymbol())
                    .interval(klineData.getInterval())
                    .eventTime(klineData.getEventTime())
                    .startTime(klineData.getStartTime())
                    .closeTime(klineData.getCloseTime())
                    .closed(klineData.isClosed())
                    .ingestTime(klineData.getIngestTime())
                    .openPrice(klineData.getOpenPrice())
                    .highPrice(klineData.getHighPrice())
                    .lowPrice(klineData.getLowPrice())
                    .closePrice(closePrice)
                    .baseVolume(klineData.getBaseVolume())
                    .quoteVolume(klineData.getQuoteVolume())
                    .tradeCount(klineData.getKline() != null ? klineData.getKline().getTradeCount() : null)
                    .amplitudePercent(klineData.getAmplitude())
                    .changePercent(klineData.getChangePercent())
                    .shortPeriod(shortPeriod)
                    .mediumPeriod(mediumPeriod)
                    .longPeriod(longPeriod)
                    .shortMa(shortMA)
                    .mediumMa(mediumMA)
                    .longMa(longMA)
                    .emaShortValue(emaShort)
                    .emaLongValue(emaLong)
                    .signalType(signalResult.getSignalType())
                    .signalStrength(signalResult.getSignalStrength())
                    .signalDetail(signalResult.getSignalDetail())
                    .signalTimestamp(signalResult.getSignalTimestamp())
                    .build();

            ctx.output(IndicatorOutputTags.KLINE_METRICS_TAG, metrics);

            Map<String, BigDecimal> components = new HashMap<>();
            if (shortMA != null) {
                components.put("ma_short", shortMA);
            }
            if (mediumMA != null) {
                components.put("ma_medium", mediumMA);
            }
            if (longMA != null) {
                components.put("ma_long", longMA);
            }
            components.put("price", closePrice);

            IndicatorMetric indicatorMetric = IndicatorMetric.builder()
                    .exchange(klineData.getExchange())
                    .symbol(klineData.getSymbol())
                    .interval(klineData.getInterval())
                    .eventTime(klineData.getEventTime())
                    .startTime(klineData.getStartTime())
                    .endTime(klineData.getCloseTime())
                    .ingestTime(klineData.getIngestTime())
                    .indicator("MA")
                    .variant(String.format("short=%d,medium=%d,long=%d", shortPeriod, mediumPeriod, longPeriod))
                    .value(shortMA)
                    .components(components)
                    .thresholds(null)
                    .signalType(signalResult.getSignalType() != null ? signalResult.getSignalType() : KlineSignal.SignalType.HOLD)
                    .signalStrength(signalResult.getSignalStrength() != null ? signalResult.getSignalStrength() : BigDecimal.ZERO)
                    .signalDetail(signalResult.getSignalDetail())
                    .signalTimestamp(signalResult.getSignalTimestamp())
                    .processTime(System.currentTimeMillis())
                    .build();

            ctx.output(IndicatorOutputTags.INDICATOR_METRICS_TAG, indicatorMetric);

            // 输出信号
            if (signalResult.getSignal() != null) {
                out.collect(signalResult.getSignal());
                log.info("Generated signal for {}: {} at price {}, strength: {}", 
                         signalResult.getSignal().getSymbol(), 
                         signalResult.getSignal().getSignalType(), 
                         signalResult.getSignal().getCurrentPrice(),
                         signalResult.getSignal().getSignalStrength());
            }
        }
    }
    
    /**
     * 计算移动平均值
     */
    private BigDecimal calculateMA(Deque<BigDecimal> priceQueue, int period) {
        if (priceQueue.size() < period) {
            return null;
        }
        
        BigDecimal sum = BigDecimal.ZERO;
        int count = 0;
        
        // 从队尾向前取period个元素
        for (BigDecimal price : priceQueue) {
            if (count >= priceQueue.size() - period) {
                sum = sum.add(price);
            }
            count++;
        }
        
        return sum.divide(BigDecimal.valueOf(period), 8, RoundingMode.HALF_UP);
    }
    
    /**
     * 计算指数移动平均（EMA）
     * EMA = Price * k + EMA(previous) * (1 - k)
     * 其中 k = 2 / (period + 1)
     */
    private BigDecimal calculateEMA(BigDecimal currentPrice, BigDecimal previousEMA, int period) {
        if (previousEMA == null) {
            // 第一次计算，使用当前价格作为初始EMA
            return currentPrice;
        }
        
        // k = 2 / (period + 1)
        BigDecimal k = BigDecimal.valueOf(2)
            .divide(BigDecimal.valueOf(period + 1), 8, RoundingMode.HALF_UP);
        
        // EMA = Price * k + EMA(previous) * (1 - k)
        return currentPrice.multiply(k)
            .add(previousEMA.multiply(BigDecimal.ONE.subtract(k)))
            .setScale(8, RoundingMode.HALF_UP);
    }
    
    /**
     * 生成交易信号
     */
    private SignalResult evaluateSignal(
            KlineData klineData,
            BigDecimal shortMA,
            BigDecimal mediumMA,
            BigDecimal longMA,
            MAValues lastMA,
            BigDecimal currentPrice) {
        
        KlineSignal.SignalType signalType = KlineSignal.SignalType.HOLD;
        BigDecimal signalStrength = BigDecimal.ZERO;
        String signalDetail = "";
        Long signalTimestamp = null;

        // 第一次计算，无历史数据，不生成信号，但输出指标
        if (lastMA == null) {
            log.debug("First MA calculation for {}, no signal generated", klineData.getSymbol());
            Map<String, Object> initialStrategyParams = buildStrategyParams(shortMA, mediumMA, longMA);
            return new SignalResult(null, signalType, signalStrength, signalDetail, initialStrategyParams, null);
        }

        // 判断MA交叉情况
        boolean shortCrossMediumUp = isCrossUp(lastMA.shortMA, shortMA, lastMA.mediumMA, mediumMA);
        boolean shortCrossMediumDown = isCrossDown(lastMA.shortMA, shortMA, lastMA.mediumMA, mediumMA);
        boolean mediumCrossLongDown = isCrossDown(lastMA.mediumMA, mediumMA, lastMA.longMA, longMA);
        
        // 判断MA排列顺序
        boolean bullishAlignment = shortMA.compareTo(mediumMA) > 0 && mediumMA.compareTo(longMA) > 0;
        boolean bearishAlignment = shortMA.compareTo(mediumMA) < 0 || mediumMA.compareTo(longMA) < 0;
        
        // 买入信号：短期MA上穿中期MA，且中期MA在长期MA之上
        if (shortCrossMediumUp && mediumMA.compareTo(longMA) > 0) {
            signalType = KlineSignal.SignalType.BUY;
            signalStrength = calculateSignalStrength(shortMA, mediumMA, longMA, true);
            signalDetail = String.format("短期MA(%.4f)上穿中期MA(%.4f)，中期MA高于长期MA(%.4f)，多头排列", 
                                        shortMA, mediumMA, longMA);
            signalTimestamp = System.currentTimeMillis();
        }
        // 强买入信号：三线多头排列
        else if (bullishAlignment && !shortCrossMediumDown && shortMA.compareTo(lastMA.shortMA) > 0) {
            signalType = KlineSignal.SignalType.BUY;
            signalStrength = calculateSignalStrength(shortMA, mediumMA, longMA, true)
                            .multiply(BigDecimal.valueOf(0.7)); // 弱于金叉信号
            signalDetail = String.format("三线多头排列，短期MA(%.4f) > 中期MA(%.4f) > 长期MA(%.4f)", 
                                        shortMA, mediumMA, longMA);
            signalTimestamp = System.currentTimeMillis();
        }
        // 卖出信号：短期MA下穿中期MA
        else if (shortCrossMediumDown) {
            signalType = KlineSignal.SignalType.SELL;
            signalStrength = calculateSignalStrength(shortMA, mediumMA, longMA, false);
            signalDetail = String.format("短期MA(%.4f)下穿中期MA(%.4f)，趋势转弱", 
                                        shortMA, mediumMA);
            signalTimestamp = System.currentTimeMillis();
        }
        // 卖出信号：中期MA下穿长期MA
        else if (mediumCrossLongDown) {
            signalType = KlineSignal.SignalType.SELL;
            signalStrength = calculateSignalStrength(shortMA, mediumMA, longMA, false)
                            .multiply(BigDecimal.valueOf(1.2)); // 强于短期死叉
            signalDetail = String.format("中期MA(%.4f)下穿长期MA(%.4f)，主趋势转空", 
                                        mediumMA, longMA);
            signalTimestamp = System.currentTimeMillis();
        }
        // 弱卖出信号：空头排列
        else if (bearishAlignment && shortMA.compareTo(lastMA.shortMA) < 0) {
            signalType = KlineSignal.SignalType.SELL;
            signalStrength = calculateSignalStrength(shortMA, mediumMA, longMA, false)
                            .multiply(BigDecimal.valueOf(0.6)); // 弱于死叉信号
            signalDetail = String.format("空头排列，短期MA(%.4f) 弱于中期MA或长期MA(%.4f)", 
                                        shortMA, longMA);
            signalTimestamp = System.currentTimeMillis();
        }

        Map<String, Object> strategyParams = buildStrategyParams(shortMA, mediumMA, longMA);

        if (signalType == KlineSignal.SignalType.HOLD) {
            return new SignalResult(null, signalType, signalStrength, signalDetail, strategyParams, null);
        }

        // 构建信号
        KlineSignal signal = KlineSignal.builder()
                .exchange(klineData.getExchange())
                .symbol(klineData.getSymbol())
                .interval(klineData.getInterval())
                .strategy("MultipleMA")
                .signalType(signalType)
                .signalStrength(signalStrength)
                .currentPrice(currentPrice)
                .klineTimestamp(klineData.getStartTime())
                .signalTimestamp(signalTimestamp)
                .strategyParams(strategyParams)
                .signalDetail(signalDetail)
                .build();

        return new SignalResult(signal, signalType, signalStrength, signalDetail, strategyParams, signalTimestamp);
    }
    
    /**
     * 判断是否发生上穿（金叉）
     * fastLine从下方穿越slowLine
     */
    private boolean isCrossUp(BigDecimal lastFast, BigDecimal currentFast, 
                              BigDecimal lastSlow, BigDecimal currentSlow) {
        return lastFast.compareTo(lastSlow) <= 0 && currentFast.compareTo(currentSlow) > 0;
    }
    
    /**
     * 判断是否发生下穿（死叉）
     * fastLine从上方穿越slowLine
     */
    private boolean isCrossDown(BigDecimal lastFast, BigDecimal currentFast,
                                BigDecimal lastSlow, BigDecimal currentSlow) {
        return lastFast.compareTo(lastSlow) >= 0 && currentFast.compareTo(currentSlow) < 0;
    }
    
    /**
     * 计算信号强度（0.0-1.0）
     * 基于MA之间的距离，距离越大信号越强
     */
    private BigDecimal calculateSignalStrength(BigDecimal shortMA, BigDecimal mediumMA, 
                                               BigDecimal longMA, boolean isBuy) {
        try {
            BigDecimal shortMediumGap = shortMA.subtract(mediumMA).abs()
                .divide(mediumMA, 8, RoundingMode.HALF_UP);
            BigDecimal mediumLongGap = mediumMA.subtract(longMA).abs()
                .divide(longMA, 8, RoundingMode.HALF_UP);
            
            // 综合两个gap计算强度，gap越大强度越高
            BigDecimal strength = shortMediumGap.add(mediumLongGap)
                .multiply(BigDecimal.valueOf(10)); // 放大系数
            
            // 限制在0-1之间
            if (strength.compareTo(BigDecimal.ONE) > 0) {
                strength = BigDecimal.ONE;
            }
            
            // 最小强度0.3
            if (strength.compareTo(BigDecimal.valueOf(0.3)) < 0) {
                strength = BigDecimal.valueOf(0.3);
            }
            
            return strength.setScale(4, RoundingMode.HALF_UP);
        } catch (Exception e) {
            log.warn("Failed to calculate signal strength: {}", e.getMessage());
            return BigDecimal.valueOf(0.5); // 默认中等强度
        }
    }

    /**
     * 信号计算结果（包含信号本身及衍生指标）
     */
    private static class SignalResult {
        private final KlineSignal signal;
        private final KlineSignal.SignalType signalType;
        private final BigDecimal signalStrength;
        private final String signalDetail;
        private final Map<String, Object> strategyParams;
        private final Long signalTimestamp;

        private SignalResult(KlineSignal signal,
                             KlineSignal.SignalType signalType,
                             BigDecimal signalStrength,
                             String signalDetail,
                             Map<String, Object> strategyParams,
                             Long signalTimestamp) {
            this.signal = signal;
            this.signalType = signalType;
            this.signalStrength = signalStrength;
            this.signalDetail = signalDetail;
            this.strategyParams = strategyParams;
            this.signalTimestamp = signalTimestamp;
        }

        public KlineSignal getSignal() {
            return signal;
        }

        public KlineSignal.SignalType getSignalType() {
            return signalType;
        }

        public BigDecimal getSignalStrength() {
            return signalStrength;
        }

        public String getSignalDetail() {
            return signalDetail;
        }

        public Map<String, Object> getStrategyParams() {
            return strategyParams;
        }

        public Long getSignalTimestamp() {
            return signalTimestamp;
        }
    }
    
    /**
     * MA值的数据结构
     */
    public static class MAValues implements java.io.Serializable {
        private static final long serialVersionUID = 1L;
        
        public BigDecimal shortMA;
        public BigDecimal mediumMA;
        public BigDecimal longMA;
        
        public MAValues() {}
        
        public MAValues(BigDecimal shortMA, BigDecimal mediumMA, BigDecimal longMA) {
            this.shortMA = shortMA;
            this.mediumMA = mediumMA;
            this.longMA = longMA;
        }
    }
    
    /**
     * EMA值的数据结构
     */
    public static class EMAValues implements java.io.Serializable {
        private static final long serialVersionUID = 1L;
        
        public BigDecimal emaShort;
        public BigDecimal emaLong;
        
        public EMAValues() {}
        
        public EMAValues(BigDecimal emaShort, BigDecimal emaLong) {
            this.emaShort = emaShort;
            this.emaLong = emaLong;
        }
    }
}
