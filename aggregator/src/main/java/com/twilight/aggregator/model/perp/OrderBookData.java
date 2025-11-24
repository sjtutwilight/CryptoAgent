package com.twilight.aggregator.model.perp;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.List;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

/**
 * 订单簿数据模型
 * 从Kafka topic: binance.perp.orderbook消费
 * 
 * 数据来源：datainjector/worker 已维护完整订单簿（经过integrity+orderbook_diff处理）
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class OrderBookData implements Serializable {
    private static final long serialVersionUID = 1L;

    // 交易对符号
    private String symbol;
    
    // 交易所标识
    private String exchange;
    
    // 订单簿深度数据
    private Depth depth;
    
    // 序列号（用于去重和排序）
    private Long seq;
    
    // 是否为快照
    private Boolean snapshot;
    
    // 交易所时间戳（毫秒）
    private Long exchangeTs;
    
    // 数据接入时间戳（毫秒）
    private Long ingestTs;
    
    /**
     * 订单簿深度结构
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Depth implements Serializable {
        private static final long serialVersionUID = 1L;
        
        // 买单列表 [[price, size], ...]
        private List<PriceLevel> bids;
        
        // 卖单列表 [[price, size], ...]
        private List<PriceLevel> asks;
    }
    
    /**
     * 价格档位
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class PriceLevel implements Serializable {
        private static final long serialVersionUID = 1L;
        
        // 价格
        private BigDecimal price;
        
        // 数量
        private BigDecimal size;
        
        /**
         * 计算该档位的USD价值
         */
        public BigDecimal getValueUsd() {
            if (price == null || size == null) {
                return BigDecimal.ZERO;
            }
            return price.multiply(size);
        }
    }
    
    // ========== 便捷访问方法 ==========
    
    /**
     * 获取最优买价（L1 bid）
     */
    public BigDecimal getBestBid() {
        if (depth == null || depth.bids == null || depth.bids.isEmpty()) {
            return null;
        }
        return depth.bids.get(0).getPrice();
    }
    
    /**
     * 获取最优卖价（L1 ask）
     */
    public BigDecimal getBestAsk() {
        if (depth == null || depth.asks == null || depth.asks.isEmpty()) {
            return null;
        }
        return depth.asks.get(0).getPrice();
    }
    
    /**
     * 获取最优买量（L1 bid size）
     */
    public BigDecimal getBestBidSize() {
        if (depth == null || depth.bids == null || depth.bids.isEmpty()) {
            return null;
        }
        return depth.bids.get(0).getSize();
    }
    
    /**
     * 获取最优卖量（L1 ask size）
     */
    public BigDecimal getBestAskSize() {
        if (depth == null || depth.asks == null || depth.asks.isEmpty()) {
            return null;
        }
        return depth.asks.get(0).getSize();
    }
    
    /**
     * 计算中间价
     * mid_price = (best_bid + best_ask) / 2
     */
    public BigDecimal getMidPrice() {
        BigDecimal bid = getBestBid();
        BigDecimal ask = getBestAsk();
        if (bid == null || ask == null) {
            return null;
        }
        return bid.add(ask).divide(BigDecimal.valueOf(2), 8, BigDecimal.ROUND_HALF_UP);
    }
}






