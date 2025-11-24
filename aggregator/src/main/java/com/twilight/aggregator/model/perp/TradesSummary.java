package com.twilight.aggregator.model.perp;

import java.io.Serializable;
import java.math.BigDecimal;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;
import lombok.Builder;

/**
 * 成交聚合指标（秒级窗口输出）
 * 
 * 从TradesAggregator计算得出，包含：
 * - 成交统计：trade_count, volume_usd
 * - 价格统计：vwap
 * - 方向统计：buy_volume, sell_volume
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TradesSummary implements Serializable {
    private static final long serialVersionUID = 1L;

    // 基础信息
    private String symbol;
    private String exchange;
    private Long windowEnd;  // 窗口结束时间（毫秒）
    
    // ===== 成交统计 =====
    
    // 成交笔数
    private Integer tradeCount;
    
    // 成交量（USD）
    private BigDecimal volumeUsd;
    
    // 成交均价（VWAP - Volume Weighted Average Price）
    private BigDecimal vwap;
    
    // ===== 方向统计 =====
    
    // 主动买入成交量（USD）
    private BigDecimal buyVolumeUsd;
    
    // 主动卖出成交量（USD）
    private BigDecimal sellVolumeUsd;
    
    // 买卖比例 = buy_volume / (buy_volume + sell_volume)
    private Double buyRatio;
    
    // ===== 价格范围 =====
    
    // 最高成交价
    private BigDecimal highPrice;
    
    // 最低成交价
    private BigDecimal lowPrice;
    
    // 首笔成交价
    private BigDecimal firstPrice;
    
    // 末笔成交价
    private BigDecimal lastPrice;
    
    // ===== 元数据 =====
    
    // 计算时间
    private Long processTime;
}






