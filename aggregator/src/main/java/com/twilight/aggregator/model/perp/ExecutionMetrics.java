package com.twilight.aggregator.model.perp;

import java.io.Serializable;
import java.math.BigDecimal;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;
import lombok.Builder;

/**
 * 执行面秒级指标（对应ClickHouse表：dws_exec_1s）
 * 
 * 包含订单簿指标和成交指标的组合，用于评估：
 * - 流动性健康度
 * - 可执行性
 * - 滑点风险
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ExecutionMetrics implements Serializable {
    private static final long serialVersionUID = 1L;

    // ===== 基础维度 =====
    
    private String symbol;
    private String exchange;
    private Long endTime;  // 秒级窗口结束时间（毫秒）
    
    // 算法版本（用于指标口径变更时的A/B对比）
    private String algoVersion;
    
    // ===== 订单簿指标 =====
    
    // 中间价
    private BigDecimal midPrice;
    
    // 点差（基点）
    private Double spreadBps;
    
    // 绝对点差
    private BigDecimal spreadAbs;
    
    // 深度指标（USD）
    private BigDecimal depth10k;
    private BigDecimal depth50k;
    private BigDecimal depth100k;
    
    // 订单簿不平衡
    private Double imbalanceTop5;
    private Double imbalanceTotal;
    
    // 冲击成本（基点）
    private Double impact10kBps;
    private Double impact50kBps;
    private Double impact100kBps;
    
    // ===== OFI (Order Flow Imbalance) - L1版本 =====
    
    /**
     * L1版OFI计算公式：
     * OFI_t = Δq_L1_bid · I{p_L1_bid不降} - Δq_L1_ask · I{p_L1_ask不升}
     * 
     * 其中：
     * - Δq_L1_bid: L1买单数量变化
     * - Δq_L1_ask: L1卖单数量变化
     * - I{}: 指示函数，满足条件为1，否则为0
     * 
     * 含义：
     * - 正值：买盘流入 > 卖盘流入，看多信号
     * - 负值：卖盘流入 > 买盘流入，看空信号
     */
    private Double ofi;
    
    // ===== 成交指标 =====
    
    // 成交笔数
    private Integer tradeCount;
    
    // 成交量（USD）
    private BigDecimal volumeUsd;
    
    // 成交均价（VWAP）
    private BigDecimal vwap;
    
    // 主动买入成交量
    private BigDecimal buyVolumeUsd;
    
    // 主动卖出成交量
    private BigDecimal sellVolumeUsd;
    
    // ===== 流动性指标（可选）=====
    
    // Amihud流动性系数 λ
    // λ = |returns| / volume （在分钟级更稳定，秒级较噪）
    private Double illiqLambda;
    
    // ===== 元数据 =====
    
    // 计算时间
    private Long processTime;
}




