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
    
    // ========== 便捷访问方法 ==========
    
    /**
     * 获取开盘价
     */
    public BigDecimal getOpenPrice() {
        return kline != null ? kline.openPrice : null;
    }
    
    /**
     * 获取收盘价
     */
    public BigDecimal getClosePrice() {
        return kline != null ? kline.closePrice : null;
    }
    
    /**
     * 获取最高价
     */
    public BigDecimal getHighPrice() {
        return kline != null ? kline.highPrice : null;
    }
    
    /**
     * 获取最低价
     */
    public BigDecimal getLowPrice() {
        return kline != null ? kline.lowPrice : null;
    }
    
    /**
     * 获取成交量
     */
    public BigDecimal getBaseVolume() {
        return kline != null ? kline.baseVolume : null;
    }
    
    /**
     * 获取成交额
     */
    public BigDecimal getQuoteVolume() {
        return kline != null ? kline.quoteVolume : null;
    }
    
    /**
     * 获取K线开始时间
     */
    public Long getStartTime() {
        return kline != null ? kline.startTime : null;
    }
    
    /**
     * 获取K线结束时间
     */
    public Long getCloseTime() {
        return kline != null ? kline.closeTime : null;
    }
    
    /**
     * K线是否已完成
     */
    public Boolean isClosed() {
        return kline != null ? kline.closed : null;
    }
    
    /**
     * 计算K线振幅（百分比）
     * 振幅 = (最高价 - 最低价) / 最低价 * 100
     */
    public BigDecimal getAmplitude() {
        if (kline == null || kline.highPrice == null || kline.lowPrice == null) {
            return null;
        }
        
        BigDecimal low = kline.lowPrice;
        if (low.compareTo(BigDecimal.ZERO) == 0) {
            return BigDecimal.ZERO;
        }
        
        return kline.highPrice.subtract(low)
            .divide(low, 6, java.math.RoundingMode.HALF_UP)
            .multiply(BigDecimal.valueOf(100));
    }
    
    /**
     * 计算涨跌幅（百分比）
     * 涨跌幅 = (收盘价 - 开盘价) / 开盘价 * 100
     */
    public BigDecimal getChangePercent() {
        if (kline == null || kline.openPrice == null || kline.closePrice == null) {
            return null;
        }
        
        BigDecimal open = kline.openPrice;
        if (open.compareTo(BigDecimal.ZERO) == 0) {
            return BigDecimal.ZERO;
        }
        
        return kline.closePrice.subtract(open)
            .divide(open, 6, java.math.RoundingMode.HALF_UP)
            .multiply(BigDecimal.valueOf(100));
    }
    
    /**
     * 判断是否为阳线
     */
    public boolean isBullish() {
        if (kline == null || kline.openPrice == null || kline.closePrice == null) {
            return false;
        }
        return kline.closePrice.compareTo(kline.openPrice) > 0;
    }
    
    /**
     * 判断是否为阴线
     */
    public boolean isBearish() {
        if (kline == null || kline.openPrice == null || kline.closePrice == null) {
            return false;
        }
        return kline.closePrice.compareTo(kline.openPrice) < 0;
    }
    
    /**
     * 计算K线实体大小（收盘价 - 开盘价的绝对值）
     */
    public BigDecimal getBodySize() {
        if (kline == null || kline.openPrice == null || kline.closePrice == null) {
            return null;
        }
        return kline.closePrice.subtract(kline.openPrice).abs();
    }
    
    /**
     * 计算上影线长度
     */
    public BigDecimal getUpperShadow() {
        if (kline == null || kline.highPrice == null || 
            kline.openPrice == null || kline.closePrice == null) {
            return null;
        }
        BigDecimal bodyTop = kline.openPrice.max(kline.closePrice);
        return kline.highPrice.subtract(bodyTop);
    }
    
    /**
     * 计算下影线长度
     */
    public BigDecimal getLowerShadow() {
        if (kline == null || kline.lowPrice == null || 
            kline.openPrice == null || kline.closePrice == null) {
            return null;
        }
        BigDecimal bodyBottom = kline.openPrice.min(kline.closePrice);
        return bodyBottom.subtract(kline.lowPrice);
    }
}
