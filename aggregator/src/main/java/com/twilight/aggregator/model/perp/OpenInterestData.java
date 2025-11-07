package com.twilight.aggregator.model.perp;

import java.io.Serializable;
import java.math.BigDecimal;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

/**
 * 持仓量（Open Interest）数据模型
 * 从Kafka topic: binance.perp.open_interest消费
 * 
 * OI表示当前市场上所有未平仓合约的总量
 * - OI上升：新资金进入或现有仓位增加
 * - OI下降：仓位平仓或资金流出
 * 
 * 注意：Binance OI数据通常5分钟更新一次
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class OpenInterestData implements Serializable {
    private static final long serialVersionUID = 1L;

    // 交易对符号
    private String symbol;
    
    // 交易所标识
    private String exchange;
    
    // 持仓量（合约张数）
    private BigDecimal oi;
    
    // 持仓量（USD价值）
    private BigDecimal oiUsd;
    
    // 交易所时间戳（毫秒）
    private Long exchangeTs;
    
    // 数据接入时间戳（毫秒）
    private Long ingestTs;
    
    // 标记：是否为前值填充（用于处理5分钟更新间隙）
    private Boolean isCarried;
    
    // ========== 便捷方法 ==========
    
    /**
     * 判断是否为有效的新数据点
     */
    public boolean isValidNewData() {
        return isCarried == null || !isCarried;
    }
}




