package com.twilight.aggregator.model.perp;

import java.io.Serializable;
import java.math.BigDecimal;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;
import lombok.Builder;

/**
 * 订单簿聚合指标（秒级窗口输出）
 * 
 * 从OrderBookProcessor计算得出，包含：
 * - 价格指标：mid_price, spread
 * - 深度指标：depth_10k, depth_50k, depth_100k
 * - 不平衡指标：imbalance_top5, imbalance_total
 * - 冲击成本：impact_10k, impact_50k, impact_100k
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class OrderBookSummary implements Serializable {
    private static final long serialVersionUID = 1L;

    // 基础信息
    private String symbol;
    private String exchange;
    private Long windowEnd;  // 窗口结束时间（毫秒）
    
    // ===== 价格指标 =====
    
    // 中间价 = (best_bid + best_ask) / 2
    private BigDecimal midPrice;
    
    // 点差（基点）= (best_ask - best_bid) / mid * 10000
    private BigDecimal spreadBps;
    
    // 绝对点差
    private BigDecimal spreadAbs;
    
    // L1最优买价
    private BigDecimal bestBid;
    
    // L1最优卖价
    private BigDecimal bestAsk;
    
    // L1最优买量
    private BigDecimal bestBidSize;
    
    // L1最优卖量
    private BigDecimal bestAskSize;
    
    // ===== 深度指标 (USD价值) =====
    
    // ±10k USD内的深度
    private BigDecimal depth10k;
    
    // ±50k USD内的深度
    private BigDecimal depth50k;
    
    // ±100k USD内的深度
    private BigDecimal depth100k;
    
    // ===== 订单簿不平衡 =====
    
    // 前5档不平衡 = (bid_vol - ask_vol) / (bid_vol + ask_vol)
    private Double imbalanceTop5;
    
    // 总不平衡（所有档位）
    private Double imbalanceTotal;
    
    // ===== 冲击成本（基点）=====
    
    // 买入10k USD的冲击成本
    private Double impact10kBps;
    
    // 买入50k USD的冲击成本
    private Double impact50kBps;
    
    // 买入100k USD的冲击成本
    private Double impact100kBps;
    
    // ===== 元数据 =====
    
    // 序列号（最新seq）
    private Long latestSeq;
    
    // 计算时间
    private Long processTime;
}






