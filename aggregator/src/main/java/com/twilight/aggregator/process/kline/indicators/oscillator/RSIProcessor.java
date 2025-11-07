package com.twilight.aggregator.process.kline.indicators.oscillator;

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
 * RSI指标处理器（Relative Strength Index - 相对强弱指标）
 * 
 * RSI是震荡类指标，用于判断市场的超买超卖状态
 * 取值范围：0-100
 * 
 * 计算方法：
 * 1. 计算价格变化：UP = max(close - previous_close, 0)
 *                  DOWN = max(previous_close - close, 0)
 * 2. 计算平均涨幅和跌幅的EMA
 * 3. RS = Average Gain / Average Loss
 * 4. RSI = 100 - (100 / (1 + RS))
 * 
 * 信号规则：
 * - RSI > 70：超买区域，可能回调，卖出信号
 * - RSI < 30：超卖区域，可能反弹，买入信号
 * - RSI上穿50：多头确认
 * - RSI下穿50：空头确认
 * - 背离信号：价格创新高但RSI不创新高（顶背离），价格创新低但RSI不创新低（底背离）
 * 
 * 默认参数：
 * - 周期：14
 * - 超买阈值：70
 * - 超卖阈值：30
 */
public class RSIProcessor extends BaseIndicatorProcessor<RSIProcessor.RSIValue> {
    private static final long serialVersionUID = 1L;
    
    private final int period;
    private final int overbought;  // 超买阈值
    private final int oversold;    // 超卖阈值
    
    // 用于计算RSI的中间状态
    private transient org.apache.flink.api.common.state.ValueState<RSIState> rsiState;
    
    public RSIProcessor() {
        this(IndicatorConfig.rsiDefault());
    }
    
    public RSIProcessor(IndicatorConfig config) {
        super(config);
        this.period = config.getIntParam("period", 14);
        this.overbought = config.getIntParam("overbought", 70);
        this.oversold = config.getIntParam("oversold", 30);
    }
    
    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
        super.open(parameters);
        
        // 初始化RSI状态
        org.apache.flink.api.common.state.ValueStateDescriptor<RSIState> rsiDescriptor = 
            new org.apache.flink.api.common.state.ValueStateDescriptor<>(
                "rsi-state",
                TypeInformation.of(RSIState.class)
            );
        rsiState = getRuntimeContext().getState(rsiDescriptor);
    }
    
    @Override
    protected int getRequiredPeriod() {
        return period + 1; // 需要额外一个数据点来计算价格变化
    }
    
    @Override
    protected String getStrategyName() {
        return "RSI" + period;
    }
    
    @Override
    protected TypeInformation<RSIValue> getIndicatorTypeInformation() {
        return TypeInformation.of(RSIValue.class);
    }
    
    @Override
    protected RSIValue calculateIndicator(PriceQueue priceQueue, KlineData currentKline) {
        try {
            BigDecimal closePrice = currentKline.getClosePrice();
            
            // 获取或初始化RSI状态
            RSIState state = rsiState.value();
            if (state == null) {
                state = new RSIState();
                state.previousClose = closePrice;
                // 第一次计算，需要累积period个价格变化来计算初始平均值
                rsiState.update(state);
                return null;
            }
            
            // 计算价格变化
            BigDecimal priceChange = closePrice.subtract(state.previousClose);
            BigDecimal gain = priceChange.compareTo(BigDecimal.ZERO) > 0 ? priceChange : BigDecimal.ZERO;
            BigDecimal loss = priceChange.compareTo(BigDecimal.ZERO) < 0 ? priceChange.abs() : BigDecimal.ZERO;
            
            // 如果是第一次计算平均值
            if (state.avgGain == null) {
                // 累积初始数据
                if (state.tempGains == null) {
                    state.tempGains = new java.util.ArrayList<>();
                    state.tempLosses = new java.util.ArrayList<>();
                }
                
                
                state.tempGains.add(gain);
                state.tempLosses.add(loss);
                
                // 数据足够时计算初始平均值
                if (state.tempGains.size() >= period) {
                    BigDecimal sumGain = BigDecimal.ZERO;
                    BigDecimal sumLoss = BigDecimal.ZERO;
                    
                    for (int i = state.tempGains.size() - period; i < state.tempGains.size(); i++) {
                        sumGain = sumGain.add(state.tempGains.get(i));
                        sumLoss = sumLoss.add(state.tempLosses.get(i));
                    }
                    
                    state.avgGain = sumGain.divide(BigDecimal.valueOf(period), 8, RoundingMode.HALF_UP);
                    state.avgLoss = sumLoss.divide(BigDecimal.valueOf(period), 8, RoundingMode.HALF_UP);
                    
                    // 清理临时数据
                    state.tempGains = null;
                    state.tempLosses = null;
                }
            } else {
                // 使用EMA方式更新平均值
                // Average Gain = ((previous Average Gain) * (period - 1) + current Gain) / period
                state.avgGain = state.avgGain.multiply(BigDecimal.valueOf(period - 1))
                    .add(gain)
                    .divide(BigDecimal.valueOf(period), 8, RoundingMode.HALF_UP);
                
                state.avgLoss = state.avgLoss.multiply(BigDecimal.valueOf(period - 1))
                    .add(loss)
                    .divide(BigDecimal.valueOf(period), 8, RoundingMode.HALF_UP);
            }
            
            // 更新前一个收盘价
            state.previousClose = closePrice;
            rsiState.update(state);
            
            // 如果平均值还未计算完成
            if (state.avgGain == null) {
                return null;
            }
            
            // 计算RSI
            BigDecimal rsi;
            if (state.avgLoss.compareTo(BigDecimal.ZERO) == 0) {
                // 避免除零，全部上涨时RSI=100
                rsi = BigDecimal.valueOf(100);
            } else {
                BigDecimal rs = state.avgGain.divide(state.avgLoss, 8, RoundingMode.HALF_UP);
                rsi = BigDecimal.valueOf(100)
                    .subtract(
                        BigDecimal.valueOf(100).divide(
                            BigDecimal.ONE.add(rs), 8, RoundingMode.HALF_UP
                        )
                    );
            }
            
            return new RSIValue(rsi, closePrice);
            
        } catch (Exception e) {
            log.error("Failed to calculate RSI: {}", e.getMessage(), e);
            return null;
        }
    }
    
    @Override
    protected KlineSignal generateSignal(
            RSIValue current,
            RSIValue last,
            KlineData klineData,
            PriceQueue priceQueue) {
        
        BigDecimal rsi = current.rsi;
        BigDecimal lastRsi = last.rsi;
        
        // 判断超买超卖区域
        boolean isOverbought = rsi.compareTo(BigDecimal.valueOf(overbought)) >= 0;
        boolean isOversold = rsi.compareTo(BigDecimal.valueOf(oversold)) <= 0;
        boolean wasOverbought = lastRsi.compareTo(BigDecimal.valueOf(overbought)) >= 0;
        boolean wasOversold = lastRsi.compareTo(BigDecimal.valueOf(oversold)) <= 0;
        
        // 判断RSI与50线的关系
        boolean rsiAbove50 = rsi.compareTo(BigDecimal.valueOf(50)) > 0;
        boolean rsiWasAbove50 = lastRsi.compareTo(BigDecimal.valueOf(50)) > 0;
        boolean rsiCrossUp50 = !rsiWasAbove50 && rsiAbove50;
        boolean rsiCrossDown50 = rsiWasAbove50 && !rsiAbove50;
        
        // 判断进入/离开超买超卖区
        boolean enterOversold = !wasOversold && isOversold;
        boolean leaveOversold = wasOversold && !isOversold && rsi.compareTo(lastRsi) > 0;
        boolean enterOverbought = !wasOverbought && isOverbought;
        boolean leaveOverbought = wasOverbought && !isOverbought && rsi.compareTo(lastRsi) < 0;
        
        KlineSignal.SignalType signalType = null;
        BigDecimal signalStrength = BigDecimal.ZERO;
        String signalDetail = "";
        
        // 买入信号：离开超卖区且RSI回升
        if (leaveOversold) {
            signalType = KlineSignal.SignalType.BUY;
            signalStrength = calculateRSIStrength(current, last, true);
            signalDetail = String.format(
                "RSI从超卖区(%.2f)回升至%.2f，买入信号",
                lastRsi, rsi
            );
        }
        // 买入信号：RSI上穿50线
        else if (rsiCrossUp50) {
            signalType = KlineSignal.SignalType.BUY;
            signalStrength = calculateRSIStrength(current, last, true).multiply(BigDecimal.valueOf(0.8));
            signalDetail = String.format(
                "RSI(%.2f)上穿50线，多头确认",
                rsi
            );
        }
        // 弱买入信号：进入超卖区（等待反弹）
        else if (enterOversold) {
            signalType = KlineSignal.SignalType.BUY;
            signalStrength = calculateRSIStrength(current, last, true).multiply(BigDecimal.valueOf(0.6));
            signalDetail = String.format(
                "RSI进入超卖区(%.2f)，等待反弹机会",
                rsi
            );
        }
        // 卖出信号：离开超买区且RSI回落
        else if (leaveOverbought) {
            signalType = KlineSignal.SignalType.SELL;
            signalStrength = calculateRSIStrength(current, last, false);
            signalDetail = String.format(
                "RSI从超买区(%.2f)回落至%.2f，卖出信号",
                lastRsi, rsi
            );
        }
        // 卖出信号：RSI下穿50线
        else if (rsiCrossDown50) {
            signalType = KlineSignal.SignalType.SELL;
            signalStrength = calculateRSIStrength(current, last, false).multiply(BigDecimal.valueOf(0.8));
            signalDetail = String.format(
                "RSI(%.2f)下穿50线，空头确认",
                rsi
            );
        }
        // 弱卖出信号：进入超买区（注意回调）
        else if (enterOverbought) {
            signalType = KlineSignal.SignalType.SELL;
            signalStrength = calculateRSIStrength(current, last, false).multiply(BigDecimal.valueOf(0.6));
            signalDetail = String.format(
                "RSI进入超买区(%.2f)，注意回调风险",
                rsi
            );
        }
        
        // 没有明确信号
        if (signalType == null) {
            return null;
        }
        
        // 构建策略参数
        Map<String, Object> strategyParams = new HashMap<>();
        strategyParams.put("period", period);
        strategyParams.put("rsi_value", rsi.doubleValue());
        strategyParams.put("overbought_threshold", overbought);
        strategyParams.put("oversold_threshold", oversold);
        
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

    @Override
    protected IndicatorMetric buildIndicatorMetric(
            RSIValue currentIndicator,
            KlineData klineData,
            PriceQueue priceQueue,
            KlineSignal currentSignal) {
        if (currentIndicator == null) {
            return null;
        }

        Map<String, BigDecimal> components = new HashMap<>();
        components.put("rsi", currentIndicator.rsi);
        if (currentIndicator.price != null) {
            components.put("price", currentIndicator.price);
        }

        Map<String, BigDecimal> thresholds = new HashMap<>();
        thresholds.put("overbought", BigDecimal.valueOf(overbought));
        thresholds.put("oversold", BigDecimal.valueOf(oversold));

        BigDecimal strength = currentSignal != null ? currentSignal.getSignalStrength() : BigDecimal.ZERO;

        return IndicatorMetric.builder()
                .exchange(klineData.getExchange())
                .symbol(klineData.getSymbol())
                .interval(klineData.getInterval())
                .eventTime(klineData.getEventTime())
                .startTime(klineData.getStartTime())
                .endTime(klineData.getCloseTime())
                .ingestTime(klineData.getIngestTime())
                .indicator("RSI")
                .variant("period=" + period)
                .value(currentIndicator.rsi)
                .components(components)
                .thresholds(thresholds)
                .signalType(currentSignal != null ? currentSignal.getSignalType() : KlineSignal.SignalType.HOLD)
                .signalStrength(strength)
                .signalDetail(currentSignal != null ? currentSignal.getSignalDetail() : null)
                .signalTimestamp(currentSignal != null ? currentSignal.getSignalTimestamp() : null)
                .processTime(System.currentTimeMillis())
                .build();
    }
    
    /**
     * 计算RSI信号强度
     * 基于RSI的位置和变化速度
     */
    private BigDecimal calculateRSIStrength(RSIValue current, RSIValue last, boolean isBuy) {
        try {
            BigDecimal rsi = current.rsi;
            BigDecimal rsiChange = current.rsi.subtract(last.rsi).abs();
            
            BigDecimal strength;
            if (isBuy) {
                // 买入信号：RSI越接近超卖区，强度越大
                BigDecimal distanceFromOversold = BigDecimal.valueOf(oversold).subtract(rsi).abs();
                strength = distanceFromOversold.divide(BigDecimal.valueOf(50), 4, RoundingMode.HALF_UP);
            } else {
                // 卖出信号：RSI越接近超买区，强度越大
                BigDecimal distanceFromOverbought = rsi.subtract(BigDecimal.valueOf(overbought)).abs();
                strength = distanceFromOverbought.divide(BigDecimal.valueOf(50), 4, RoundingMode.HALF_UP);
            }
            
            // 考虑RSI变化速度
            BigDecimal changeBonus = rsiChange.divide(BigDecimal.valueOf(20), 4, RoundingMode.HALF_UP);
            strength = strength.add(changeBonus);
            
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
     * RSI指标值
     */
    public static class RSIValue implements java.io.Serializable {
        private static final long serialVersionUID = 1L;
        
        public BigDecimal rsi;   // RSI值 (0-100)
        public BigDecimal price; // 当前价格
        
        public RSIValue() {}
        
        public RSIValue(BigDecimal rsi, BigDecimal price) {
            this.rsi = rsi;
            this.price = price;
        }
    }
    
    /**
     * RSI计算状态
     */
    public static class RSIState implements java.io.Serializable {
        private static final long serialVersionUID = 1L;
        
        public BigDecimal previousClose;  // 前一个收盘价
        public BigDecimal avgGain;        // 平均涨幅
        public BigDecimal avgLoss;        // 平均跌幅
        
        // 用于初始化计算的临时数据
        public transient java.util.List<BigDecimal> tempGains;
        public transient java.util.List<BigDecimal> tempLosses;
    }
}



