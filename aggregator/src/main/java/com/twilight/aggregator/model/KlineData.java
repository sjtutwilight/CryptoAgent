package com.twilight.aggregator.model;

import java.io.Serializable;
import java.math.BigDecimal;

import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * K线数据模型
 * 从Kafka topic: binance.kline消费的原始K线数据
 */
@Data
@NoArgsConstructor
public class KlineData implements Serializable {
    private static final long serialVersionUID = 1L;

    // 交易所标识
    private String exchange;
    
    // 交易对符号，如BTCUSDT
    private String symbol;
    
    // K线时间间隔：1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w, 1M
    private String interval;
    
    // 事件时间（交易所推送时间戳）
    private Long eventTime;
    
    // K线详细信息
    private Kline kline;
    
    // 数据接入时间（数据进入系统的时间戳）
    private Long ingestTime;
    
    /**
     * K线详细信息
     */
    @Data
    @NoArgsConstructor
    public static class Kline implements Serializable {
        private static final long serialVersionUID = 1L;
        
        // K线开始时间
        private Long startTime;
        
        // K线结束时间
        private Long closeTime;
        
        // 开盘价
        private BigDecimal openPrice;
        
        // 收盘价
        private BigDecimal closePrice;
        
        // 最高价
        private BigDecimal highPrice;
        
        // 最低价
        private BigDecimal lowPrice;
        
        // 成交量（基础资产，如BTC）
        private BigDecimal baseVolume;
        
        // 成交额（报价资产，如USDT）
        private BigDecimal quoteVolume;
        
        // 成交笔数
        private Integer tradeCount;
        
        // K线是否已完成（true表示该K线周期已结束）
        private Boolean closed;
    }
    
    /**
     * 获取K线收盘价
     */
    public BigDecimal getClosePrice() {
        return kline != null ? kline.getClosePrice() : null;
    }
    
    /**
     * 获取K线开盘价
     */
    public BigDecimal getOpenPrice() {
        return kline != null ? kline.getOpenPrice() : null;
    }
    
    /**
     * 获取K线最高价
     */
    public BigDecimal getHighPrice() {
        return kline != null ? kline.getHighPrice() : null;
    }
    
    /**
     * 获取K线最低价
     */
    public BigDecimal getLowPrice() {
        return kline != null ? kline.getLowPrice() : null;
    }
    
    /**
     * 获取K线开始时间
     */
    public Long getStartTime() {
        return kline != null ? kline.getStartTime() : null;
    }
    
    /**
     * 获取K线结束时间
     */
    public Long getCloseTime() {
        return kline != null ? kline.getCloseTime() : null;
    }
    
    /**
     * K线是否已完成
     */
    public Boolean isClosed() {
        return kline != null && kline.getClosed() != null && kline.getClosed();
    }
}

