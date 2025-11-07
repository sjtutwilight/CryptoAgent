package com.twilight.aggregator.model;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * K线信号模型
 * 输出到Kafka topic: kline.signal的交易信号数据
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class KlineSignal implements Serializable {
    private static final long serialVersionUID = 1L;

    // 交易所标识
    private String exchange;
    
    // 交易对符号，如BTCUSDT
    private String symbol;
    
    // K线时间间隔
    private String interval;
    
    // 策略名称，如"MultipleMA", "MACD", "RSI"
    private String strategy;
    
    // 信号类型：BUY(买入), SELL(卖出), HOLD(持有)
    private SignalType signalType;
    
    // 信号强度：0.0-1.0，表示信号的置信度
    private BigDecimal signalStrength;
    
    // 当前价格
    private BigDecimal currentPrice;
    
    // K线时间戳（使用K线开始时间）
    private Long klineTimestamp;
    
    // 信号生成时间
    private Long signalTimestamp;
    
    // 策略参数，JSON格式存储具体参数
    // 例如：{"ma_short": 5, "ma_medium": 10, "ma_long": 20}
    private Map<String, Object> strategyParams;
    
    // 信号详情说明
    private String signalDetail;
    
    /**
     * 信号类型枚举
     */
    public enum SignalType {
        BUY("买入"),
        SELL("卖出"),
        HOLD("持有");
        
        private final String description;
        
        SignalType(String description) {
            this.description = description;
        }
        
        public String getDescription() {
            return description;
        }
    }
}





