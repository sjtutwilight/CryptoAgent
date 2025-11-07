package com.twilight.aggregator.process.kline.indicators.trend;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.common.typeinfo.TypeInformation;

import com.twilight.aggregator.model.KlineData;
import com.twilight.aggregator.model.KlineSignal;
import com.twilight.aggregator.model.IndicatorMetric;
import com.twilight.aggregator.process.kline.indicators.BaseIndicatorProcessor;
import com.twilight.aggregator.process.kline.indicators.IndicatorConfig;

/**
 * MACD指标处理器（Moving Average Convergence Divergence - 指数平滑异同移动平均线）
 * 
 * MACD是趋势类指标，用于判断价格趋势的强度和方向
 * 
 * 计算方法：
 * 1. DIF（差离值）= 短期EMA - 长期EMA
 * 2. DEA（信号线）= DIF的EMA
 * 3. MACD柱 = (DIF - DEA) * 2
 * 
 * 信号规则：
 * - 金叉买入：DIF上穿DEA，且MACD柱由负转正
 * - 死叉卖出：DIF下穿DEA，且MACD柱由正转负
 * - 零轴判断：DIF在零轴上方为多头市场，下方为空头市场
 * 
 * 默认参数：
 * - 快线周期：12
 * - 慢线周期：26
 * - 信号线周期：9
 */
public class MACDProcessor extends BaseIndicatorProcessor<MACDProcessor.MACDValues> {
    private static final long serialVersionUID = 1L;
    
    private final int fastPeriod;   // 快线EMA周期
    private final int slowPeriod;   // 慢线EMA周期
    private final int signalPeriod; // 信号线EMA周期
    
    // 用于计算EMA的中间状态
    private transient org.apache.flink.api.common.state.ValueState<EMAState> emaState;
    
    public MACDProcessor() {
        this(IndicatorConfig.macdDefault());
    }
    
    public MACDProcessor(IndicatorConfig config) {
        super(config);
        this.fastPeriod = config.getIntParam("fast_period", 12);
        this.slowPeriod = config.getIntParam("slow_period", 26);
        this.signalPeriod = config.getIntParam("signal_period", 9);
        
        if (fastPeriod >= slowPeriod) {
            throw new IllegalArgumentException("快线周期必须小于慢线周期");
        }
    }

    @Override
    protected IndicatorMetric buildIndicatorMetric(
            MACDValues currentIndicator,
            KlineData klineData,
            PriceQueue priceQueue,
            KlineSignal currentSignal) {
        if (currentIndicator == null) {
            return null;
        }

        Map<String, BigDecimal> components = new HashMap<>();
        components.put("dif", currentIndicator.dif);
        components.put("dea", currentIndicator.dea);
        components.put("macd", currentIndicator.macd);

        Map<String, BigDecimal> thresholds = new HashMap<>();
        thresholds.put("zero_line", BigDecimal.ZERO);

        BigDecimal strength = currentSignal != null ? currentSignal.getSignalStrength() : BigDecimal.ZERO;

        return IndicatorMetric.builder()
                .exchange(klineData.getExchange())
                .symbol(klineData.getSymbol())
                .interval(klineData.getInterval())
                .eventTime(klineData.getEventTime())
                .startTime(klineData.getStartTime())
                .endTime(klineData.getCloseTime())
                .ingestTime(klineData.getIngestTime())
                .indicator("MACD")
                .variant(String.format("fast=%d,slow=%d,signal=%d", fastPeriod, slowPeriod, signalPeriod))
                .value(currentIndicator.dif)
                .components(components)
                .thresholds(thresholds)
                .signalType(currentSignal != null ? currentSignal.getSignalType() : KlineSignal.SignalType.HOLD)
                .signalStrength(strength)
                .signalDetail(currentSignal != null ? currentSignal.getSignalDetail() : null)
                .signalTimestamp(currentSignal != null ? currentSignal.getSignalTimestamp() : null)
                .processTime(System.currentTimeMillis())
                .build();
    }
    
    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
        super.open(parameters);
        
        // 初始化EMA状态
        org.apache.flink.api.common.state.ValueStateDescriptor<EMAState> emaDescriptor = 
            new org.apache.flink.api.common.state.ValueStateDescriptor<>(
                "ema-state-macd",
                TypeInformation.of(EMAState.class)
            );
        emaState = getRuntimeContext().getState(emaDescriptor);
    }
    
    @Override
    protected int getRequiredPeriod() {
        // 需要慢线周期 + 信号线周期的数据
        return slowPeriod + signalPeriod;
    }
    
    @Override
    protected String getStrategyName() {
        return "MACD";
    }
    
    @Override
    protected TypeInformation<MACDValues> getIndicatorTypeInformation() {
        return TypeInformation.of(MACDValues.class);
    }
    
    @Override
    protected MACDValues calculateIndicator(PriceQueue priceQueue, KlineData currentKline) {
        try {
            BigDecimal closePrice = currentKline.getClosePrice();
            
            // 获取或初始化EMA状态
            EMAState state = emaState.value();
            if (state == null) {
                // 使用初始SMA作为EMA的起始值
                BigDecimal fastSMA = calculateSMA(priceQueue.getClosePrices(), fastPeriod);
                BigDecimal slowSMA = calculateSMA(priceQueue.getClosePrices(), slowPeriod);
                
                if (fastSMA == null || slowSMA == null) {
                    return null;
                }
                
                state = new EMAState();
                state.fastEMA = fastSMA;
                state.slowEMA = slowSMA;
                state.dif = fastSMA.subtract(slowSMA);
                state.dea = state.dif; // 第一个DIF作为DEA的初始值
            }
            
            // 计算新的EMA值
            BigDecimal newFastEMA = calculateEMA(closePrice, state.fastEMA, fastPeriod);
            BigDecimal newSlowEMA = calculateEMA(closePrice, state.slowEMA, slowPeriod);
            
            // 计算DIF
            BigDecimal dif = newFastEMA.subtract(newSlowEMA);
            
            // 计算DEA（信号线）
            BigDecimal dea = calculateEMA(dif, state.dea, signalPeriod);
            
            // 计算MACD柱
            BigDecimal macd = dif.subtract(dea).multiply(BigDecimal.valueOf(2));
            
            // 更新状态
            state.fastEMA = newFastEMA;
            state.slowEMA = newSlowEMA;
            state.dif = dif;
            state.dea = dea;
            emaState.update(state);
            
            return new MACDValues(dif, dea, macd);
            
        } catch (Exception e) {
            log.error("Failed to calculate MACD: {}", e.getMessage(), e);
            return null;
        }
    }
    
    @Override
    protected KlineSignal generateSignal(
            MACDValues current,
            MACDValues last,
            KlineData klineData,
            PriceQueue priceQueue) {
        
        // 判断DIF和DEA的交叉
        boolean goldenCross = isCrossUp(last.dif, current.dif, last.dea, current.dea);
        boolean deathCross = isCrossDown(last.dif, current.dif, last.dea, current.dea);
        
        // 判断MACD柱的正负变化
        boolean macdTurnPositive = last.macd.compareTo(BigDecimal.ZERO) <= 0 
                                   && current.macd.compareTo(BigDecimal.ZERO) > 0;
        boolean macdTurnNegative = last.macd.compareTo(BigDecimal.ZERO) >= 0 
                                   && current.macd.compareTo(BigDecimal.ZERO) < 0;
        
        // 判断DIF所在位置（零轴上方/下方）
        boolean difAboveZero = current.dif.compareTo(BigDecimal.ZERO) > 0;
        
        KlineSignal.SignalType signalType = null;
        BigDecimal signalStrength = BigDecimal.ZERO;
        String signalDetail = "";
        
        // 买入信号：金叉且MACD柱转正
        if (goldenCross && macdTurnPositive) {
            signalType = KlineSignal.SignalType.BUY;
            signalStrength = calculateMACDStrength(current, true);
            signalDetail = String.format(
                "MACD金叉：DIF(%.6f)上穿DEA(%.6f)，MACD柱转正(%.6f)，强烈买入信号",
                current.dif, current.dea, current.macd
            );
        }
        // 买入信号：金叉（MACD柱未必转正）
        else if (goldenCross) {
            signalType = KlineSignal.SignalType.BUY;
            signalStrength = calculateMACDStrength(current, true).multiply(BigDecimal.valueOf(0.7));
            signalDetail = String.format(
                "MACD金叉：DIF(%.6f)上穿DEA(%.6f)，买入信号",
                current.dif, current.dea
            );
        }
        // 买入信号：零轴上方的二次金叉（更强的买入信号）
        else if (goldenCross && difAboveZero) {
            signalType = KlineSignal.SignalType.BUY;
            signalStrength = calculateMACDStrength(current, true).multiply(BigDecimal.valueOf(1.2));
            signalDetail = String.format(
                "MACD零轴上方金叉：DIF(%.6f)上穿DEA(%.6f)，强势上涨确认",
                current.dif, current.dea
            );
        }
        // 卖出信号：死叉且MACD柱转负
        else if (deathCross && macdTurnNegative) {
            signalType = KlineSignal.SignalType.SELL;
            signalStrength = calculateMACDStrength(current, false);
            signalDetail = String.format(
                "MACD死叉：DIF(%.6f)下穿DEA(%.6f)，MACD柱转负(%.6f)，强烈卖出信号",
                current.dif, current.dea, current.macd
            );
        }
        // 卖出信号：死叉
        else if (deathCross) {
            signalType = KlineSignal.SignalType.SELL;
            signalStrength = calculateMACDStrength(current, false).multiply(BigDecimal.valueOf(0.7));
            signalDetail = String.format(
                "MACD死叉：DIF(%.6f)下穿DEA(%.6f)，卖出信号",
                current.dif, current.dea
            );
        }
        // 卖出信号：零轴下方死叉（更强的卖出信号）
        else if (deathCross && !difAboveZero) {
            signalType = KlineSignal.SignalType.SELL;
            signalStrength = calculateMACDStrength(current, false).multiply(BigDecimal.valueOf(1.2));
            signalDetail = String.format(
                "MACD零轴下方死叉：DIF(%.6f)下穿DEA(%.6f)，弱势下跌确认",
                current.dif, current.dea
            );
        }
        
        // 没有明确信号
        if (signalType == null) {
            return null;
        }
        
        // 构建策略参数
        Map<String, Object> strategyParams = new HashMap<>();
        strategyParams.put("fast_period", fastPeriod);
        strategyParams.put("slow_period", slowPeriod);
        strategyParams.put("signal_period", signalPeriod);
        strategyParams.put("dif", current.dif.doubleValue());
        strategyParams.put("dea", current.dea.doubleValue());
        strategyParams.put("macd", current.macd.doubleValue());
        
        return KlineSignal.builder()
                .exchange(klineData.getExchange())
                .symbol(klineData.getSymbol())
                .interval(klineData.getInterval())
                .strategy(getStrategyName())
                .signalType(signalType)
                .signalStrength(signalStrength)
                .currentPrice(klineData.getClosePrice())
                .klineTimestamp(klineData.getStartTime())
                .signalTimestamp(System.currentTimeMillis())
                .strategyParams(strategyParams)
                .signalDetail(signalDetail)
                .build();
    }
    
    /**
     * 计算MACD信号强度
     * 基于MACD柱的绝对值和DIF、DEA的距离
     */
    private BigDecimal calculateMACDStrength(MACDValues macd, boolean isBuy) {
        try {
            // MACD柱的绝对值（归一化）
            BigDecimal macdAbs = macd.macd.abs();
            
            // DIF和DEA的距离
            BigDecimal difDeaGap = macd.dif.subtract(macd.dea).abs();
            
            // 综合计算强度
            BigDecimal strength = macdAbs.add(difDeaGap)
                .multiply(BigDecimal.valueOf(50)); // 放大系数
            
            // 限制在0.3-1.0之间
            if (strength.compareTo(BigDecimal.ONE) > 0) {
                strength = BigDecimal.ONE;
            }
            if (strength.compareTo(BigDecimal.valueOf(0.3)) < 0) {
                strength = BigDecimal.valueOf(0.3);
            }
            
            return strength.setScale(4, RoundingMode.HALF_UP);
        } catch (Exception e) {
            return BigDecimal.valueOf(0.5);
        }
    }
    
    /**
     * MACD指标值
     */
    public static class MACDValues implements java.io.Serializable {
        private static final long serialVersionUID = 1L;
        
        public BigDecimal dif;  // 差离值
        public BigDecimal dea;  // 信号线
        public BigDecimal macd; // MACD柱
        
        public MACDValues() {}
        
        public MACDValues(BigDecimal dif, BigDecimal dea, BigDecimal macd) {
            this.dif = dif;
            this.dea = dea;
            this.macd = macd;
        }
    }
    
    /**
     * EMA计算状态
     */
    public static class EMAState implements java.io.Serializable {
        private static final long serialVersionUID = 1L;
        
        public BigDecimal fastEMA;  // 快线EMA
        public BigDecimal slowEMA;  // 慢线EMA
        public BigDecimal dif;      // 上一个DIF
        public BigDecimal dea;      // 上一个DEA
    }
}



