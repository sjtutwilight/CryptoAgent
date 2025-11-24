package com.twilight.aggregator.model.perp;

import java.io.Serializable;
import java.math.BigDecimal;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

/**
 * 标记价格和指数价格数据模型
 * 从Kafka topic: binance.perp.mark_index消费
 * 
 * 标记价格：用于计算未实现盈亏和清算价格
 * 指数价格：多个现货交易所的加权平均价格
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class MarkIndexData implements Serializable {
    private static final long serialVersionUID = 1L;

    // 交易对符号
    private String symbol;
    
    // 交易所标识
    private String exchange;
    
    // 标记价格
    private BigDecimal markPrice;
    
    // 指数价格
    private BigDecimal indexPrice;
    
    // 公允基差（mark - index）
    private BigDecimal fairBasis;
    
    // 上一次资金费率
    private BigDecimal lastFundingRate;
    
    // 下次资金费结算时间（毫秒时间戳）
    private Long nextFundingTime;
    
    // 交易所时间戳（毫秒）
    private Long exchangeTs;
    
    // 数据接入时间戳（毫秒）
    private Long ingestTs;
    
    // ========== 便捷方法 ==========
    
    /**
     * 计算基差（基点）
     * basis_bps = (mark - index) / index * 10000
     */
    public BigDecimal getBasisBps() {
        if (markPrice == null || indexPrice == null || indexPrice.compareTo(BigDecimal.ZERO) == 0) {
            return null;
        }
        return markPrice.subtract(indexPrice)
                .divide(indexPrice, 8, BigDecimal.ROUND_HALF_UP)
                .multiply(BigDecimal.valueOf(10000));
    }
    
    /**
     * 获取资金费率（年化百分比）
     */
    public BigDecimal getFundingRateAnnualized() {
        if (lastFundingRate == null) {
            return null;
        }
        // 假设8小时结算一次，一年3次 * 365天
        return lastFundingRate.multiply(BigDecimal.valueOf(3 * 365));
    }
}






