package com.twilight.aggregator.process.kline.indicators.volatility;

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
 * 布林带指标处理器（Bollinger Bands）
 * 
 * 布林带是波动率类指标，由三条线组成：中轨（SMA）、上轨、下轨
 * 用于判断价格的波动范围和趋势强度
 * 
 * 计算方法：
 * 1. 中轨（Middle Band）= N周期SMA
 * 2. 上轨（Upper Band）= 中轨 + K * N周期标准差
 * 3. 下轨（Lower Band）= 中轨 - K * N周期标准差
 * 
 * 信号规则：
 * - 价格突破上轨：超买，可能回调，卖出信号
 * - 价格跌破下轨：超卖，可能反弹，买入信号
 * - 价格从下轨反弹：买入确认
 * - 价格从上轨回落：卖出确认
 * - 布林带收窄：波动率降低，可能酝酿突破
 * - 布林带扩张：波动率增加，趋势强化
 * 
 * 默认参数：
 * - 周期：20
 * - 标准差倍数：2.0
 */
public class BollingerBandsProcessor extends BaseIndicatorProcessor<BollingerBandsProcessor.BBValues> {
    private static final long serialVersionUID = 1L;
    
    private final int period;
    private final double stdDevMultiplier;
    
    public BollingerBandsProcessor() {
        this(IndicatorConfig.bollingerDefault());
    }
    
    public BollingerBandsProcessor(IndicatorConfig config) {
        super(config);
        this.period = config.getIntParam("period", 20);
        this.stdDevMultiplier = config.getDoubleParam("std_dev_multiplier", 2.0);
    }
    
    @Override
    protected int getRequiredPeriod() {
        return period;
    }
    
    @Override
    protected String getStrategyName() {
        return "BollingerBands";
    }
    
    @Override
    protected TypeInformation<BBValues> getIndicatorTypeInformation() {
        return TypeInformation.of(BBValues.class);
    }
    
    @Override
    protected BBValues calculateIndicator(PriceQueue priceQueue, KlineData currentKline) {
        try {
            BigDecimal closePrice = currentKline.getClosePrice();
            
            // 计算中轨（SMA）
            BigDecimal middle = calculateSMA(priceQueue.getClosePrices(), period);
            if (middle == null) {
                return null;
            }
            
            // 计算标准差
            BigDecimal stdDev = calculateStdDev(priceQueue.getClosePrices(), period, middle);
            if (stdDev == null) {
                return null;
            }
            
            // 计算上轨和下轨
            BigDecimal multiplier = BigDecimal.valueOf(stdDevMultiplier);
            BigDecimal upper = middle.add(stdDev.multiply(multiplier));
            BigDecimal lower = middle.subtract(stdDev.multiply(multiplier));
            
            // 计算带宽（Bandwidth）= (上轨 - 下轨) / 中轨
            BigDecimal bandwidth = upper.subtract(lower)
                .divide(middle, 8, RoundingMode.HALF_UP);
            
            // 计算%B指标 = (价格 - 下轨) / (上轨 - 下轨)
            BigDecimal percentB = closePrice.subtract(lower)
                .divide(upper.subtract(lower), 8, RoundingMode.HALF_UP);
            
            return new BBValues(upper, middle, lower, closePrice, bandwidth, percentB);
            
        } catch (Exception e) {
            log.error("Failed to calculate Bollinger Bands: {}", e.getMessage(), e);
            return null;
        }
    }

    @Override
    protected IndicatorMetric buildIndicatorMetric(
            BBValues currentIndicator,
            KlineData klineData,
            PriceQueue priceQueue,
            KlineSignal currentSignal) {
        if (currentIndicator == null) {
            return null;
        }

        Map<String, BigDecimal> components = new HashMap<>();
        components.put("upper", currentIndicator.upper);
        components.put("middle", currentIndicator.middle);
        components.put("lower", currentIndicator.lower);
        components.put("price", currentIndicator.price);
        components.put("bandwidth", currentIndicator.bandwidth);
        components.put("percent_b", currentIndicator.percentB);

        Map<String, BigDecimal> thresholds = new HashMap<>();
        thresholds.put("std_dev_multiplier", BigDecimal.valueOf(stdDevMultiplier));

        BigDecimal strength = currentSignal != null ? currentSignal.getSignalStrength() : BigDecimal.ZERO;

        return IndicatorMetric.builder()
                .exchange(klineData.getExchange())
                .symbol(klineData.getSymbol())
                .interval(klineData.getInterval())
                .eventTime(klineData.getEventTime())
                .startTime(klineData.getStartTime())
                .endTime(klineData.getCloseTime())
                .ingestTime(klineData.getIngestTime())
                .indicator("BOLL")
                .variant(String.format("period=%d,std=%.2f", period, stdDevMultiplier))
                .value(currentIndicator.middle)
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
    protected KlineSignal generateSignal(
            BBValues current,
            BBValues last,
            KlineData klineData,
            PriceQueue priceQueue) {
        
        // 判断价格与布林带的位置关系
        boolean priceAboveUpper = current.price.compareTo(current.upper) > 0;
        boolean priceBelowLower = current.price.compareTo(current.lower) < 0;
        boolean priceWasAboveUpper = last.price.compareTo(last.upper) > 0;
        boolean priceWasBelowLower = last.price.compareTo(last.lower) < 0;
        
        // 判断突破和回归
        boolean breakoutUpper = !priceWasAboveUpper && priceAboveUpper;
        boolean breakoutLower = !priceWasBelowLower && priceBelowLower;
        boolean returnFromUpper = priceWasAboveUpper && !priceAboveUpper && 
                                  current.price.compareTo(last.price) < 0;
        boolean returnFromLower = priceWasBelowLower && !priceBelowLower && 
                                  current.price.compareTo(last.price) > 0;
        
        // 判断%B的位置
        boolean percentBOverbought = current.percentB.compareTo(BigDecimal.ONE) > 0;
        boolean percentBOversold = current.percentB.compareTo(BigDecimal.ZERO) < 0;
        
        // 判断带宽变化（收窄/扩张）
        BigDecimal bandwidthChange = current.bandwidth.subtract(last.bandwidth);
        boolean bandwidthExpanding = bandwidthChange.compareTo(BigDecimal.ZERO) > 0;
        boolean bandwidthNarrowing = bandwidthChange.compareTo(BigDecimal.ZERO) < 0;
        
        KlineSignal.SignalType signalType = null;
        BigDecimal signalStrength = BigDecimal.ZERO;
        String signalDetail = "";
        
        // 买入信号：价格从下轨反弹
        if (returnFromLower) {
            signalType = KlineSignal.SignalType.BUY;
            signalStrength = calculateBBStrength(current, last, true);
            signalDetail = String.format(
                "价格(%.4f)从下轨(%.4f)反弹，%%B=%.4f，买入信号",
                current.price, current.lower, current.percentB
            );
        }
        // 买入信号：价格触及下轨
        else if (breakoutLower) {
            signalType = KlineSignal.SignalType.BUY;
            signalStrength = calculateBBStrength(current, last, true).multiply(BigDecimal.valueOf(0.8));
            signalDetail = String.format(
                "价格(%.4f)触及下轨(%.4f)，超卖，等待反弹",
                current.price, current.lower
            );
        }
        // 买入信号：价格在下轨附近且带宽收窄后扩张（可能突破）
        else if (priceBelowLower && bandwidthExpanding && last.bandwidth.compareTo(current.bandwidth) < 0) {
            signalType = KlineSignal.SignalType.BUY;
            signalStrength = calculateBBStrength(current, last, true).multiply(BigDecimal.valueOf(0.7));
            signalDetail = String.format(
                "价格(%.4f)在下轨附近，带宽扩张(%.6f→%.6f)，突破信号",
                current.price, last.bandwidth, current.bandwidth
            );
        }
        // 卖出信号：价格从上轨回落
        else if (returnFromUpper) {
            signalType = KlineSignal.SignalType.SELL;
            signalStrength = calculateBBStrength(current, last, false);
            signalDetail = String.format(
                "价格(%.4f)从上轨(%.4f)回落，%%B=%.4f，卖出信号",
                current.price, current.upper, current.percentB
            );
        }
        // 卖出信号：价格突破上轨
        else if (breakoutUpper) {
            signalType = KlineSignal.SignalType.SELL;
            signalStrength = calculateBBStrength(current, last, false).multiply(BigDecimal.valueOf(0.8));
            signalDetail = String.format(
                "价格(%.4f)突破上轨(%.4f)，超买，注意回调",
                current.price, current.upper
            );
        }
        // 卖出信号：价格在上轨附近且带宽收窄后扩张（可能下跌）
        else if (priceAboveUpper && bandwidthExpanding && last.bandwidth.compareTo(current.bandwidth) < 0) {
            signalType = KlineSignal.SignalType.SELL;
            signalStrength = calculateBBStrength(current, last, false).multiply(BigDecimal.valueOf(0.7));
            signalDetail = String.format(
                "价格(%.4f)在上轨附近，带宽扩张(%.6f→%.6f)，回调信号",
                current.price, last.bandwidth, current.bandwidth
            );
        }
        
        // 没有明确信号
        if (signalType == null) {
            return null;
        }
        
        // 构建策略参数
        Map<String, Object> strategyParams = new HashMap<>();
        strategyParams.put("period", period);
        strategyParams.put("std_dev_multiplier", stdDevMultiplier);
        strategyParams.put("upper_band", current.upper.doubleValue());
        strategyParams.put("middle_band", current.middle.doubleValue());
        strategyParams.put("lower_band", current.lower.doubleValue());
        strategyParams.put("bandwidth", current.bandwidth.doubleValue());
        strategyParams.put("percent_b", current.percentB.doubleValue());
        
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
     * 计算布林带信号强度
     */
    private BigDecimal calculateBBStrength(BBValues current, BBValues last, boolean isBuy) {
        try {
            // %B的极端程度
            BigDecimal percentBExtreme;
            if (isBuy) {
                // 买入：%B越小（负值越大）越极端
                percentBExtreme = BigDecimal.ZERO.subtract(current.percentB).max(BigDecimal.ZERO);
            } else {
                // 卖出：%B越大（超过1越多）越极端
                percentBExtreme = current.percentB.subtract(BigDecimal.ONE).max(BigDecimal.ZERO);
            }
            
            // 带宽扩张速度
            BigDecimal bandwidthChange = current.bandwidth.subtract(last.bandwidth).abs();
            
            // 综合强度
            BigDecimal strength = percentBExtreme.multiply(BigDecimal.valueOf(2))
                .add(bandwidthChange.multiply(BigDecimal.valueOf(10)));
            
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
     * 布林带指标值
     */
    public static class BBValues implements java.io.Serializable {
        private static final long serialVersionUID = 1L;
        
        public BigDecimal upper;     // 上轨
        public BigDecimal middle;    // 中轨
        public BigDecimal lower;     // 下轨
        public BigDecimal price;     // 当前价格
        public BigDecimal bandwidth; // 带宽
        public BigDecimal percentB;  // %B指标
        
        public BBValues() {}
        
        public BBValues(BigDecimal upper, BigDecimal middle, BigDecimal lower, 
                       BigDecimal price, BigDecimal bandwidth, BigDecimal percentB) {
            this.upper = upper;
            this.middle = middle;
            this.lower = lower;
            this.price = price;
            this.bandwidth = bandwidth;
            this.percentB = percentB;
        }
    }
}





