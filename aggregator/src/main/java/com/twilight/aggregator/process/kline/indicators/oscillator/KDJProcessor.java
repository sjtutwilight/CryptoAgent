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
 * KDJ指标处理器（随机指标）
 * 
 * KDJ是震荡类指标，由KD指标演化而来，用于判断超买超卖和趋势转折
 * 取值范围：0-100
 * 
 * 计算方法：
 * 1. RSV（未成熟随机值）= (Close - Lowest_Low) / (Highest_High - Lowest_Low) * 100
 * 2. K值 = 前一日K值 * 2/3 + 当日RSV * 1/3
 * 3. D值 = 前一日D值 * 2/3 + 当日K值 * 1/3
 * 4. J值 = 3 * K值 - 2 * D值
 * 
 * 信号规则：
 * - K线上穿D线：金叉，买入信号
 * - K线下穿D线：死叉，卖出信号
 * - KDJ > 80：超买区域
 * - KDJ < 20：超卖区域
 * - J值 > 100或 < 0：极端超买/超卖，注意反转
 * 
 * 默认参数：
 * - K周期：9（计算RSV的周期）
 * - D周期：3（平滑周期）
 * - J周期：3（平滑周期）
 * - 超买阈值：80
 * - 超卖阈值：20
 */
public class KDJProcessor extends BaseIndicatorProcessor<KDJProcessor.KDJValues> {
    private static final long serialVersionUID = 1L;
    
    private final int kPeriod;     // RSV计算周期
    private final int dPeriod;     // D值平滑周期
    private final int jPeriod;     // J值平滑周期
    private final int overbought;  // 超买阈值
    private final int oversold;    // 超卖阈值
    
    public KDJProcessor() {
        this(IndicatorConfig.kdjDefault());
    }
    
    public KDJProcessor(IndicatorConfig config) {
        super(config);
        this.kPeriod = config.getIntParam("k_period", 9);
        this.dPeriod = config.getIntParam("d_period", 3);
        this.jPeriod = config.getIntParam("j_period", 3);
        this.overbought = config.getIntParam("overbought", 80);
        this.oversold = config.getIntParam("oversold", 20);
    }
    
    @Override
    protected int getRequiredPeriod() {
        return kPeriod;
    }
    
    @Override
    protected String getStrategyName() {
        return "KDJ";
    }
    
    @Override
    protected TypeInformation<KDJValues> getIndicatorTypeInformation() {
        return TypeInformation.of(KDJValues.class);
    }
    
    @Override
    protected KDJValues calculateIndicator(PriceQueue priceQueue, KlineData currentKline) {
        try {
            BigDecimal closePrice = currentKline.getClosePrice();
            
            // 计算RSV：在kPeriod周期内，(收盘价 - 最低价) / (最高价 - 最低价) * 100
            BigDecimal lowestLow = getLowest(priceQueue.getLowPrices(), kPeriod);
            BigDecimal highestHigh = getHighest(priceQueue.getHighPrices(), kPeriod);
            
            if (lowestLow == null || highestHigh == null) {
                return null;
            }
            
            BigDecimal rsv;
            BigDecimal range = highestHigh.subtract(lowestLow);
            if (range.compareTo(BigDecimal.ZERO) == 0) {
                // 最高价等于最低价，横盘
                rsv = BigDecimal.valueOf(50);
            } else {
                rsv = closePrice.subtract(lowestLow)
                    .divide(range, 8, RoundingMode.HALF_UP)
                    .multiply(BigDecimal.valueOf(100));
            }
            
            // 获取前一个KDJ值
            KDJValues lastKDJ = lastIndicatorState.value();
            
            BigDecimal k, d, j;
            if (lastKDJ == null) {
                // 第一次计算，K和D都初始化为RSV
                k = rsv;
                d = rsv;
                j = rsv;
            } else {
                // K = 前K * 2/3 + RSV * 1/3
                k = lastKDJ.k.multiply(BigDecimal.valueOf(2))
                    .divide(BigDecimal.valueOf(3), 8, RoundingMode.HALF_UP)
                    .add(rsv.multiply(BigDecimal.valueOf(1))
                        .divide(BigDecimal.valueOf(3), 8, RoundingMode.HALF_UP));
                
                // D = 前D * 2/3 + K * 1/3
                d = lastKDJ.d.multiply(BigDecimal.valueOf(2))
                    .divide(BigDecimal.valueOf(3), 8, RoundingMode.HALF_UP)
                    .add(k.multiply(BigDecimal.valueOf(1))
                        .divide(BigDecimal.valueOf(3), 8, RoundingMode.HALF_UP));
                
                // J = 3K - 2D
                j = k.multiply(BigDecimal.valueOf(3))
                    .subtract(d.multiply(BigDecimal.valueOf(2)));
            }
            
            return new KDJValues(k, d, j, closePrice);
            
        } catch (Exception e) {
            log.error("Failed to calculate KDJ: {}", e.getMessage(), e);
            return null;
        }
    }

    @Override
    protected IndicatorMetric buildIndicatorMetric(
            KDJValues currentIndicator,
            KlineData klineData,
            PriceQueue priceQueue,
            KlineSignal currentSignal) {
        if (currentIndicator == null) {
            return null;
        }

        Map<String, BigDecimal> components = new HashMap<>();
        components.put("k", currentIndicator.k);
        components.put("d", currentIndicator.d);
        components.put("j", currentIndicator.j);
        components.put("price", currentIndicator.price);

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
                .indicator("KDJ")
                .variant(String.format("k=%d,d=%d,j=%d", kPeriod, dPeriod, jPeriod))
                .value(currentIndicator.k)
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
            KDJValues current,
            KDJValues last,
            KlineData klineData,
            PriceQueue priceQueue) {
        
        // 判断K、D线交叉
        boolean goldenCross = isCrossUp(last.k, current.k, last.d, current.d);
        boolean deathCross = isCrossDown(last.k, current.k, last.d, current.d);
        
        // 判断超买超卖区域
        boolean kInOversold = current.k.compareTo(BigDecimal.valueOf(oversold)) < 0;
        boolean dInOversold = current.d.compareTo(BigDecimal.valueOf(oversold)) < 0;
        boolean kInOverbought = current.k.compareTo(BigDecimal.valueOf(overbought)) > 0;
        boolean dInOverbought = current.d.compareTo(BigDecimal.valueOf(overbought)) > 0;
        
        // 判断J值极端情况
        boolean jExtremeOversold = current.j.compareTo(BigDecimal.ZERO) < 0;
        boolean jExtremeOverbought = current.j.compareTo(BigDecimal.valueOf(100)) > 0;
        
        KlineSignal.SignalType signalType = null;
        BigDecimal signalStrength = BigDecimal.ZERO;
        String signalDetail = "";
        
        // 买入信号：超卖区金叉
        if (goldenCross && kInOversold && dInOversold) {
            signalType = KlineSignal.SignalType.BUY;
            signalStrength = calculateKDJStrength(current, last, true);
            signalDetail = String.format(
                "KDJ超卖区金叉：K(%.2f)上穿D(%.2f)，J(%.2f)，强买入信号",
                current.k, current.d, current.j
            );
        }
        // 买入信号：普通金叉
        else if (goldenCross) {
            signalType = KlineSignal.SignalType.BUY;
            signalStrength = calculateKDJStrength(current, last, true).multiply(BigDecimal.valueOf(0.8));
            signalDetail = String.format(
                "KDJ金叉：K(%.2f)上穿D(%.2f)，J(%.2f)，买入信号",
                current.k, current.d, current.j
            );
        }
        // 买入信号：J值极端超卖反弹
        else if (jExtremeOversold && current.j.compareTo(last.j) > 0) {
            signalType = KlineSignal.SignalType.BUY;
            signalStrength = calculateKDJStrength(current, last, true).multiply(BigDecimal.valueOf(0.9));
            signalDetail = String.format(
                "J值(%.2f)极端超卖后反弹，K(%.2f)，D(%.2f)，买入机会",
                current.j, current.k, current.d
            );
        }
        // 卖出信号：超买区死叉
        else if (deathCross && kInOverbought && dInOverbought) {
            signalType = KlineSignal.SignalType.SELL;
            signalStrength = calculateKDJStrength(current, last, false);
            signalDetail = String.format(
                "KDJ超买区死叉：K(%.2f)下穿D(%.2f)，J(%.2f)，强卖出信号",
                current.k, current.d, current.j
            );
        }
        // 卖出信号：普通死叉
        else if (deathCross) {
            signalType = KlineSignal.SignalType.SELL;
            signalStrength = calculateKDJStrength(current, last, false).multiply(BigDecimal.valueOf(0.8));
            signalDetail = String.format(
                "KDJ死叉：K(%.2f)下穿D(%.2f)，J(%.2f)，卖出信号",
                current.k, current.d, current.j
            );
        }
        // 卖出信号：J值极端超买回落
        else if (jExtremeOverbought && current.j.compareTo(last.j) < 0) {
            signalType = KlineSignal.SignalType.SELL;
            signalStrength = calculateKDJStrength(current, last, false).multiply(BigDecimal.valueOf(0.9));
            signalDetail = String.format(
                "J值(%.2f)极端超买后回落，K(%.2f)，D(%.2f)，卖出机会",
                current.j, current.k, current.d
            );
        }
        
        // 没有明确信号
        if (signalType == null) {
            return null;
        }
        
        // 构建策略参数
        Map<String, Object> strategyParams = new HashMap<>();
        strategyParams.put("k_period", kPeriod);
        strategyParams.put("k_value", current.k.doubleValue());
        strategyParams.put("d_value", current.d.doubleValue());
        strategyParams.put("j_value", current.j.doubleValue());
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
    
    /**
     * 获取队列中最近period个数据的最低值
     */
    private BigDecimal getLowest(java.util.Deque<BigDecimal> values, int period) {
        if (values.size() < period) {
            return null;
        }
        
        BigDecimal lowest = null;
        int count = 0;
        
        for (BigDecimal value : values) {
            if (count >= values.size() - period) {
                if (lowest == null || value.compareTo(lowest) < 0) {
                    lowest = value;
                }
            }
            count++;
        }
        
        return lowest;
    }
    
    /**
     * 获取队列中最近period个数据的最高值
     */
    private BigDecimal getHighest(java.util.Deque<BigDecimal> values, int period) {
        if (values.size() < period) {
            return null;
        }
        
        BigDecimal highest = null;
        int count = 0;
        
        for (BigDecimal value : values) {
            if (count >= values.size() - period) {
                if (highest == null || value.compareTo(highest) > 0) {
                    highest = value;
                }
            }
            count++;
        }
        
        return highest;
    }
    
    /**
     * 计算KDJ信号强度
     */
    private BigDecimal calculateKDJStrength(KDJValues current, KDJValues last, boolean isBuy) {
        try {
            // K、D值的距离
            BigDecimal kdGap = current.k.subtract(current.d).abs();
            
            // J值的极端程度
            BigDecimal jExtreme;
            if (isBuy) {
                // 买入：J值越低越极端
                jExtreme = BigDecimal.valueOf(oversold).subtract(current.j).max(BigDecimal.ZERO);
            } else {
                // 卖出：J值越高越极端
                jExtreme = current.j.subtract(BigDecimal.valueOf(overbought)).max(BigDecimal.ZERO);
            }
            
            // 综合强度
            BigDecimal strength = kdGap.divide(BigDecimal.valueOf(20), 4, RoundingMode.HALF_UP)
                .add(jExtreme.divide(BigDecimal.valueOf(50), 4, RoundingMode.HALF_UP));
            
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
     * KDJ指标值
     */
    public static class KDJValues implements java.io.Serializable {
        private static final long serialVersionUID = 1L;
        
        public BigDecimal k;     // K值
        public BigDecimal d;     // D值
        public BigDecimal j;     // J值
        public BigDecimal price; // 当前价格
        
        public KDJValues() {}
        
        public KDJValues(BigDecimal k, BigDecimal d, BigDecimal j, BigDecimal price) {
            this.k = k;
            this.d = d;
            this.j = j;
            this.price = price;
        }
    }
}





