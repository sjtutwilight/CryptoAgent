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
 * ATR指标处理器（Average True Range - 平均真实波幅）
 * 
 * ATR是波动率类指标，用于衡量市场波动性的大小
 * ATR值越大，表示波动性越大；ATR值越小，表示波动性越小
 * 
 * 计算方法：
 * 1. TR（真实波幅）= max(高 - 低, abs(高 - 前收), abs(低 - 前收))
 * 2. ATR = TR的N周期EMA
 * 
 * 应用场景：
 * - 止损设置：ATR * 倍数作为止损距离
 * - 仓位管理：根据ATR调整仓位大小
 * - 趋势判断：ATR上升表示趋势加强，ATR下降表示趋势减弱
 * - 突破确认：价格突破配合ATR增加，突破有效性更高
 * 
 * 信号规则：
 * - ATR快速上升 + 价格上涨：强势上涨，买入信号
 * - ATR快速上升 + 价格下跌：恐慌下跌，观望
 * - ATR持续下降：波动率降低，可能酝酿突破
 * - ATR从低位回升：波动率恢复，趋势可能启动
 * 
 * 默认参数：
 * - 周期：14
 */
public class ATRProcessor extends BaseIndicatorProcessor<ATRProcessor.ATRValue> {
    private static final long serialVersionUID = 1L;
    
    private final int period;
    
    // 用于计算ATR的中间状态
    private transient org.apache.flink.api.common.state.ValueState<ATRState> atrState;
    
    public ATRProcessor() {
        this(IndicatorConfig.atrDefault());
    }
    
    public ATRProcessor(IndicatorConfig config) {
        super(config);
        this.period = config.getIntParam("period", 14);
    }
    
    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
        super.open(parameters);
        
        // 初始化ATR状态
        org.apache.flink.api.common.state.ValueStateDescriptor<ATRState> atrDescriptor = 
            new org.apache.flink.api.common.state.ValueStateDescriptor<>(
                "atr-state",
                TypeInformation.of(ATRState.class)
            );
        atrState = getRuntimeContext().getState(atrDescriptor);
    }
    
    @Override
    protected int getRequiredPeriod() {
        return period + 1; // 需要前一根K线来计算TR
    }
    
    @Override
    protected String getStrategyName() {
        return "ATR" + period;
    }
    
    @Override
    protected TypeInformation<ATRValue> getIndicatorTypeInformation() {
        return TypeInformation.of(ATRValue.class);
    }
    
    @Override
    protected ATRValue calculateIndicator(PriceQueue priceQueue, KlineData currentKline) {
        try {
            BigDecimal high = currentKline.getHighPrice();
            BigDecimal low = currentKline.getLowPrice();
            BigDecimal close = currentKline.getClosePrice();
            
            // 获取或初始化ATR状态
            ATRState state = atrState.value();
            if (state == null) {
                state = new ATRState();
                state.previousClose = close;
                atrState.update(state);
                return null;
            }
            
            // 计算真实波幅（TR）
            // TR = max(高-低, abs(高-前收), abs(低-前收))
            BigDecimal highLow = high.subtract(low);
            BigDecimal highPrevClose = high.subtract(state.previousClose).abs();
            BigDecimal lowPrevClose = low.subtract(state.previousClose).abs();
            
            BigDecimal tr = highLow.max(highPrevClose).max(lowPrevClose);
            
            // 如果是第一次计算ATR
            if (state.atr == null) {
                // 累积初始TR值
                if (state.tempTRs == null) {
                    state.tempTRs = new java.util.ArrayList<>();
                }
                
                state.tempTRs.add(tr);
                
                // 数据足够时计算初始ATR（使用SMA）
                if (state.tempTRs.size() >= period) {
                    BigDecimal sumTR = BigDecimal.ZERO;
                    for (int i = state.tempTRs.size() - period; i < state.tempTRs.size(); i++) {
                        sumTR = sumTR.add(state.tempTRs.get(i));
                    }
                    
                    state.atr = sumTR.divide(BigDecimal.valueOf(period), 8, RoundingMode.HALF_UP);
                    state.tempTRs = null; // 清理临时数据
                }
            } else {
                // 使用EMA方式更新ATR
                // ATR = ((previous ATR) * (period - 1) + current TR) / period
                state.atr = state.atr.multiply(BigDecimal.valueOf(period - 1))
                    .add(tr)
                    .divide(BigDecimal.valueOf(period), 8, RoundingMode.HALF_UP);
            }
            
            // 更新前一个收盘价
            state.previousClose = close;
            atrState.update(state);
            
            // 如果ATR还未计算完成
            if (state.atr == null) {
                return null;
            }
            
            // 计算ATR相对价格的百分比
            BigDecimal atrPercent = state.atr.divide(close, 6, RoundingMode.HALF_UP)
                .multiply(BigDecimal.valueOf(100));
            
            return new ATRValue(state.atr, tr, atrPercent, close);
            
        } catch (Exception e) {
            log.error("Failed to calculate ATR: {}", e.getMessage(), e);
            return null;
        }
    }

    @Override
    protected IndicatorMetric buildIndicatorMetric(
            ATRValue currentIndicator,
            KlineData klineData,
            PriceQueue priceQueue,
            KlineSignal currentSignal) {
        if (currentIndicator == null) {
            return null;
        }

        Map<String, BigDecimal> components = new HashMap<>();
        components.put("atr", currentIndicator.atr);
        components.put("tr", currentIndicator.tr);
        components.put("atr_percent", currentIndicator.atrPercent);
        components.put("price", currentIndicator.price);

        Map<String, BigDecimal> thresholds = new HashMap<>();
        thresholds.put("period", BigDecimal.valueOf(period));

        BigDecimal strength = currentSignal != null ? currentSignal.getSignalStrength() : BigDecimal.ZERO;

        return IndicatorMetric.builder()
                .exchange(klineData.getExchange())
                .symbol(klineData.getSymbol())
                .interval(klineData.getInterval())
                .eventTime(klineData.getEventTime())
                .startTime(klineData.getStartTime())
                .endTime(klineData.getCloseTime())
                .ingestTime(klineData.getIngestTime())
                .indicator("ATR")
                .variant("period=" + period)
                .value(currentIndicator.atr)
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
            ATRValue current,
            ATRValue last,
            KlineData klineData,
            PriceQueue priceQueue) {
        
        // 计算ATR变化率
        BigDecimal atrChange = current.atr.subtract(last.atr);
        BigDecimal atrChangePercent = atrChange.divide(last.atr, 6, RoundingMode.HALF_UP)
            .multiply(BigDecimal.valueOf(100));
        
        // 判断价格方向
        boolean priceRising = current.price.compareTo(last.price) > 0;
        boolean priceFalling = current.price.compareTo(last.price) < 0;
        
        // 判断ATR趋势
        boolean atrRising = atrChange.compareTo(BigDecimal.ZERO) > 0;
        boolean atrFalling = atrChange.compareTo(BigDecimal.ZERO) < 0;
        
        // 判断ATR的快速变化（阈值：5%）
        BigDecimal rapidChangeThreshold = BigDecimal.valueOf(5);
        boolean atrRapidIncrease = atrChangePercent.compareTo(rapidChangeThreshold) > 0;
        boolean atrRapidDecrease = atrChangePercent.compareTo(rapidChangeThreshold.negate()) < 0;
        
        // 判断ATR的相对水平（与历史比较）
        // 这里简化处理，可以扩展为维护ATR的历史区间
        boolean atrHigh = current.atrPercent.compareTo(BigDecimal.valueOf(3)) > 0; // 3%以上为高波动
        boolean atrLow = current.atrPercent.compareTo(BigDecimal.valueOf(1)) < 0;  // 1%以下为低波动
        
        KlineSignal.SignalType signalType = null;
        BigDecimal signalStrength = BigDecimal.ZERO;
        String signalDetail = "";
        
        // 买入信号：价格上涨 + ATR快速上升（强势突破）
        if (priceRising && atrRapidIncrease) {
            signalType = KlineSignal.SignalType.BUY;
            signalStrength = calculateATRStrength(current, last, true);
            signalDetail = String.format(
                "价格上涨(%.4f)配合ATR快速上升(%.6f，+%.2f%%)，强势突破信号",
                current.price, current.atr, atrChangePercent
            );
        }
        // 买入信号：ATR从低位回升 + 价格上涨（波动率恢复，趋势启动）
        else if (atrLow && atrRising && priceRising) {
            signalType = KlineSignal.SignalType.BUY;
            signalStrength = calculateATRStrength(current, last, true).multiply(BigDecimal.valueOf(0.8));
            signalDetail = String.format(
                "ATR从低位(%.6f)回升，价格上涨(%.4f)，趋势可能启动",
                current.atr, current.price
            );
        }
        // 观望/轻仓买入：ATR持续下降（低波动，等待突破）
        else if (atrRapidDecrease && !atrLow) {
            signalType = KlineSignal.SignalType.BUY;
            signalStrength = BigDecimal.valueOf(0.3); // 低强度信号
            signalDetail = String.format(
                "ATR持续下降(%.6f，%.2f%%)，波动率降低，可能酝酿突破，轻仓观望",
                current.atr, atrChangePercent
            );
        }
        // 卖出信号：价格下跌 + ATR快速上升（恐慌下跌）
        else if (priceFalling && atrRapidIncrease) {
            signalType = KlineSignal.SignalType.SELL;
            signalStrength = calculateATRStrength(current, last, false);
            signalDetail = String.format(
                "价格下跌(%.4f)配合ATR快速上升(%.6f，+%.2f%%)，恐慌性下跌",
                current.price, current.atr, atrChangePercent
            );
        }
        // 卖出信号：高ATR + 价格开始下跌（高位回落）
        else if (atrHigh && priceFalling && atrFalling) {
            signalType = KlineSignal.SignalType.SELL;
            signalStrength = calculateATRStrength(current, last, false).multiply(BigDecimal.valueOf(0.8));
            signalDetail = String.format(
                "高波动率(ATR=%.6f)后价格下跌，注意高位回落风险",
                current.atr
            );
        }
        
        // 没有明确信号
        if (signalType == null) {
            return null;
        }
        
        // 构建策略参数
        Map<String, Object> strategyParams = new HashMap<>();
        strategyParams.put("period", period);
        strategyParams.put("atr_value", current.atr.doubleValue());
        strategyParams.put("tr_value", current.tr.doubleValue());
        strategyParams.put("atr_percent", current.atrPercent.doubleValue());
        strategyParams.put("atr_change_percent", atrChangePercent.doubleValue());
        
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
     * 计算ATR信号强度
     */
    private BigDecimal calculateATRStrength(ATRValue current, ATRValue last, boolean isBuy) {
        try {
            // ATR变化率
            BigDecimal atrChangeRate = current.atr.subtract(last.atr)
                .divide(last.atr, 6, RoundingMode.HALF_UP)
                .abs();
            
            // ATR相对水平
            BigDecimal atrLevel = current.atrPercent.divide(BigDecimal.valueOf(5), 6, RoundingMode.HALF_UP);
            
            // 综合强度
            BigDecimal strength = atrChangeRate.multiply(BigDecimal.valueOf(5))
                .add(atrLevel);
            
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
     * ATR指标值
     */
    public static class ATRValue implements java.io.Serializable {
        private static final long serialVersionUID = 1L;
        
        public BigDecimal atr;        // ATR值
        public BigDecimal tr;         // 当前TR值
        public BigDecimal atrPercent; // ATR占价格的百分比
        public BigDecimal price;      // 当前价格
        
        public ATRValue() {}
        
        public ATRValue(BigDecimal atr, BigDecimal tr, BigDecimal atrPercent, BigDecimal price) {
            this.atr = atr;
            this.tr = tr;
            this.atrPercent = atrPercent;
            this.price = price;
        }
    }
    
    /**
     * ATR计算状态
     */
    public static class ATRState implements java.io.Serializable {
        private static final long serialVersionUID = 1L;
        
        public BigDecimal previousClose;  // 前一个收盘价
        public BigDecimal atr;            // 当前ATR值
        
        // 用于初始化计算的临时数据
        public transient java.util.List<BigDecimal> tempTRs;
    }
}



