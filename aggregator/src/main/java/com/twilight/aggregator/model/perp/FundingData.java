package com.twilight.aggregator.model.perp;

import java.io.Serializable;
import java.math.BigDecimal;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

/**
 * 历史资金费率数据模型
 * 从Kafka topic: binance.perp.funding_rate消费
 * 
 * 资金费率：多头/空头之间的周期性支付
 * - 正费率：多头支付空头（市场看多）
 * - 负费率：空头支付多头（市场看空）
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class FundingData implements Serializable {
    private static final long serialVersionUID = 1L;

    // 交易对符号
    private String symbol;
    
    // 交易所标识
    private String exchange;
    
    // 资金费率（例如0.0001表示0.01%）
    private BigDecimal fundingRate;
    
    // 资金费结算时间（毫秒时间戳）
    private Long fundingTime;
    
    // 资金费结算间隔（如"8h"）
    private String fundingInterval;
    
    // 交易所时间戳（毫秒）
    private Long exchangeTs;
    
    // 数据接入时间戳（毫秒）
    private Long ingestTs;
    
    // ========== 便捷方法 ==========
    
    /**
     * 转换为基点（bps）
     * 1 bps = 0.0001 = 0.01%
     */
    public BigDecimal getFundingRateBps() {
        if (fundingRate == null) {
            return null;
        }
        return fundingRate.multiply(BigDecimal.valueOf(10000));
    }
    
    /**
     * 转换为8小时费率（如果interval不是8h需要换算）
     */
    public BigDecimal getFundingRate8h() {
        if (fundingRate == null || fundingInterval == null) {
            return fundingRate;
        }
        // 目前binance永续都是8h，直接返回
        // 如果未来支持其他交易所，这里需要换算逻辑
        return fundingRate;
    }
}






