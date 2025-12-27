package com.twilight.aggregator.model.perp;

import java.io.Serializable;
import java.math.BigDecimal;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

/**
 * 永续合约成交数据模型
 * 从Kafka topic: binance.perp.aggtrades消费
 * 
 * 注意：使用Binance aggTrade（聚合成交）而非普通trade
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TradeData implements Serializable {
    private static final long serialVersionUID = 1L;

    // 交易对符号
    private String symbol;
    
    // 交易所标识
    private String exchange;
    
    // 成交价格
    private BigDecimal price;
    
    // 成交数量
    private BigDecimal size;
    
    // 成交方向（buy/sell - 主动方）
    private String side;
    
    // 是否买方是maker（true表示买方挂单被吃，即卖方主动）
    private Boolean buyerMaker;
    
    // 交易所时间戳（毫秒）
    private Long exchangeTs;
    
    // 数据接入时间戳（毫秒）
    private Long ingestTs;
    
    // 交易ID
    private Long tradeId;
    
    // 买方订单ID
    private Long buyerOrderId;
    
    // 卖方订单ID
    private Long sellerOrderId;
    
    // ========== 便捷方法 ==========
    
    /**
     * 计算成交金额（USD）
     */
    public BigDecimal getValueUsd() {
        if (price == null || size == null) {
            return BigDecimal.ZERO;
        }
        return price.multiply(size);
    }
    
    /**
     * 判断是否为主动买入
     * 主动买入：taker是买方，即买方主动吃单
     */
    public boolean isBuyerTaker() {
        return buyerMaker != null && !buyerMaker;
    }
    
    /**
     * 判断是否为主动卖出
     * 主动卖出：taker是卖方，即卖方主动吃单
     */
    public boolean isSellerTaker() {
        return buyerMaker != null && buyerMaker;
    }
}

