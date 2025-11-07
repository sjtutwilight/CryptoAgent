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
 * EMA指标处理器（Exponential Moving Average - 指数移动平均线）
 * 
 * EMA是趋势类指标，相比SMA更注重近期价格的变化
 * 常用于判断价格趋势和支撑/阻力位
 * 
 * 计算方法：
 * EMA(t) = Price(t) * k + EMA(t-1) * (1 - k)
 * 其中 k = 2 / (period + 1)
 * 
 * 信号规则：
 * - 价格上穿EMA：买入信号
 * - 价格下穿EMA：卖出信号
 * - EMA向上且价格在EMA之上：持续多头
 * - EMA向下且价格在EMA之下：持续空头
 * 
 * 可配置多个EMA周期实现多均线策略
 */
public class EMAProcessor extends BaseIndicatorProcessor<EMAProcessor.EMAValue> {
    private static final long serialVersionUID = 1L;
    
    private final int period;
    
    public EMAProcessor(int period) {
        super(IndicatorConfig.emaDefault(period));
        this.period = period;
    }
    
    public EMAProcessor(IndicatorConfig config) {
        super(config);
        this.period = config.getIntParam("period", 20);
    }
    
    @Override
    protected int getRequiredPeriod() {
        return period;
    }
    
    @Override
    protected String getStrategyName() {
        return "EMA" + period;
    }
    
    @Override
    protected TypeInformation<EMAValue> getIndicatorTypeInformation() {
        return TypeInformation.of(EMAValue.class);
    }
    
    @Override
    protected EMAValue calculateIndicator(PriceQueue priceQueue, KlineData currentKline) {
        try {
            BigDecimal closePrice = currentKline.getClosePrice();
            
            // 如果是第一次计算，使用SMA作为初始EMA
            if (priceQueue.size() == period) {
                BigDecimal sma = calculateSMA(priceQueue.getClosePrices(), period);
                if (sma == null) {
                    return null;
                }
                return new EMAValue(sma, closePrice);
            }
            
            // 获取上一个EMA值
            EMAValue lastEMA = lastIndicatorState.value();
            if (lastEMA == null) {
                // 使用当前价格作为初始EMA
                return new EMAValue(closePrice, closePrice);
            }
            
            // 计算新的EMA
            BigDecimal newEMA = calculateEMA(closePrice, lastEMA.ema, period);
            
            return new EMAValue(newEMA, closePrice);
            
        } catch (Exception e) {
            log.error("Failed to calculate EMA: {}", e.getMessage(), e);
            return null;
        }
    }
    
    @Override
    protected KlineSignal generateSignal(
            EMAValue current,
            EMAValue last,
            KlineData klineData,
            PriceQueue priceQueue) {
        
        // 判断价格与EMA的交叉
        boolean priceAboveEMA = current.price.compareTo(current.ema) > 0;
        boolean priceWasAboveEMA = last.price.compareTo(last.ema) > 0;
        
        // 判断EMA的趋势方向
        boolean emaRising = current.ema.compareTo(last.ema) > 0;
        boolean emaFalling = current.ema.compareTo(last.ema) < 0;
        
        // 判断交叉
        boolean priceCrossUpEMA = !priceWasAboveEMA && priceAboveEMA;
        boolean priceCrossDownEMA = priceWasAboveEMA && !priceAboveEMA;
        
        KlineSignal.SignalType signalType = null;
        BigDecimal signalStrength = BigDecimal.ZERO;
        String signalDetail = "";
        
        // 买入信号：价格上穿EMA
        if (priceCrossUpEMA) {
            signalType = KlineSignal.SignalType.BUY;
            signalStrength = calculateEMAStrength(current, last, true);
            signalDetail = String.format(
                "价格(%.4f)上穿EMA%d(%.4f)，买入信号",
                current.price, period, current.ema
            );
            
            // 如果EMA也在上升，信号更强
            if (emaRising) {
                signalStrength = signalStrength.multiply(BigDecimal.valueOf(1.2));
                if (signalStrength.compareTo(BigDecimal.ONE) > 0) {
                    signalStrength = BigDecimal.ONE;
                }
                signalDetail = String.format(
                    "价格(%.4f)上穿上升中的EMA%d(%.4f)，强买入信号",
                    current.price, period, current.ema
                );
            }
        }
        // 卖出信号：价格下穿EMA
        else if (priceCrossDownEMA) {
            signalType = KlineSignal.SignalType.SELL;
            signalStrength = calculateEMAStrength(current, last, false);
            signalDetail = String.format(
                "价格(%.4f)下穿EMA%d(%.4f)，卖出信号",
                current.price, period, current.ema
            );
            
            // 如果EMA也在下降，信号更强
            if (emaFalling) {
                signalStrength = signalStrength.multiply(BigDecimal.valueOf(1.2));
                if (signalStrength.compareTo(BigDecimal.ONE) > 0) {
                    signalStrength = BigDecimal.ONE;
                }
                signalDetail = String.format(
                    "价格(%.4f)下穿下降中的EMA%d(%.4f)，强卖出信号",
                    current.price, period, current.ema
                );
            }
        }
        // 持续多头信号：价格在上升的EMA之上
        else if (priceAboveEMA && emaRising && 
                 current.price.subtract(current.ema).compareTo(last.price.subtract(last.ema)) > 0) {
            signalType = KlineSignal.SignalType.BUY;
            signalStrength = calculateEMAStrength(current, last, true).multiply(BigDecimal.valueOf(0.6));
            signalDetail = String.format(
                "价格(%.4f)持续在上升的EMA%d(%.4f)之上，多头趋势",
                current.price, period, current.ema
            );
        }
        // 持续空头信号：价格在下降的EMA之下
        else if (!priceAboveEMA && emaFalling &&
                 current.ema.subtract(current.price).compareTo(last.ema.subtract(last.price)) > 0) {
            signalType = KlineSignal.SignalType.SELL;
            signalStrength = calculateEMAStrength(current, last, false).multiply(BigDecimal.valueOf(0.6));
            signalDetail = String.format(
                "价格(%.4f)持续在下降的EMA%d(%.4f)之下，空头趋势",
                current.price, period, current.ema
            );
        }
        
        // 没有明确信号
        if (signalType == null) {
            return null;
        }
        
        // 构建策略参数
        Map<String, Object> strategyParams = new HashMap<>();
        strategyParams.put("period", period);
        strategyParams.put("ema_value", current.ema.doubleValue());
        strategyParams.put("price_ema_distance", 
            current.price.subtract(current.ema).divide(current.ema, 6, RoundingMode.HALF_UP).doubleValue());
        
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
            EMAValue currentIndicator,
            KlineData klineData,
            PriceQueue priceQueue,
            KlineSignal currentSignal) {
        if (currentIndicator == null) {
            return null;
        }

        Map<String, BigDecimal> components = new HashMap<>();
        components.put("ema", currentIndicator.ema);
        components.put("price", currentIndicator.price);

        BigDecimal strength = currentSignal != null ? currentSignal.getSignalStrength() : BigDecimal.ZERO;

        return IndicatorMetric.builder()
                .exchange(klineData.getExchange())
                .symbol(klineData.getSymbol())
                .interval(klineData.getInterval())
                .eventTime(klineData.getEventTime())
                .startTime(klineData.getStartTime())
                .endTime(klineData.getCloseTime())
                .ingestTime(klineData.getIngestTime())
                .indicator("EMA")
                .variant("period=" + period)
                .value(currentIndicator.ema)
                .components(components)
                .thresholds(null)
                .signalType(currentSignal != null ? currentSignal.getSignalType() : KlineSignal.SignalType.HOLD)
                .signalStrength(strength)
                .signalDetail(currentSignal != null ? currentSignal.getSignalDetail() : null)
                .signalTimestamp(currentSignal != null ? currentSignal.getSignalTimestamp() : null)
                .processTime(System.currentTimeMillis())
                .build();
    }
    
    /**
     * 计算EMA信号强度
     * 基于价格与EMA的距离
     */
    private BigDecimal calculateEMAStrength(EMAValue current, EMAValue last, boolean isBuy) {
        try {
            // 价格与EMA的距离（百分比）
            BigDecimal distance = current.price.subtract(current.ema)
                .divide(current.ema, 8, RoundingMode.HALF_UP)
                .abs();
            
            // EMA的斜率（变化速度）
            BigDecimal emaSlope = current.ema.subtract(last.ema)
                .divide(last.ema, 8, RoundingMode.HALF_UP)
                .abs();
            
            // 综合强度
            BigDecimal strength = distance.add(emaSlope)
                .multiply(BigDecimal.valueOf(20)); // 放大系数
            
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
     * EMA指标值
     */
    public static class EMAValue implements java.io.Serializable {
        private static final long serialVersionUID = 1L;
        
        public BigDecimal ema;   // EMA值
        public BigDecimal price; // 当前价格
        
        public EMAValue() {}
        
        public EMAValue(BigDecimal ema, BigDecimal price) {
            this.ema = ema;
            this.price = price;
        }
    }
}



