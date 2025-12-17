package com.twilight.aggregator.model.perp;

import java.io.Serializable;
import java.math.BigDecimal;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;
import lombok.Builder;

/**
 * 语境面分钟级指标（对应ClickHouse表：dws_perps_ctx_1m）
 * 
 * 包含标记价格、资金费率、持仓量等慢速变化的市场语境指标
 * 用于评估：
 * - 市场情绪
 * - 拥挤度
 * - 清算风险
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ContextMetrics implements Serializable {
    private static final long serialVersionUID = 1L;

    // ===== 基础维度 =====
    
    private String symbol;
    private String exchange;
    private Long endTime;  // 分钟级窗口结束时间（毫秒）
    
    // 算法版本
    private String algoVersion;
    
    // ===== 价格指标 =====
    
    // 标记价格
    private BigDecimal markPrice;
    
    // 指数价格
    private BigDecimal indexPrice;
    
    // 基差（基点）= (mark - index) / index * 10000
    private Double basisBps;
    
    // ===== 资金费率 =====
    
    // 当前资金费率
    private BigDecimal fundingRate;
    
    // 8小时资金费率（标准化）
    private BigDecimal fundingRate8h;
    
    /**
     * 24小时资金费率EMA（在线计算）
     * 
     * 公式：EMA_t = α · x_t + (1-α) · EMA_{t-1}
     * 其中：α = 1 - exp(-Δt / τ)
     * τ = 24小时 = 86400秒
     * Δt = 当前时间 - 上次更新时间（秒）
     * 
     * 优点：
     * - 只需单值状态，内存占用小
     * - 适应不规则更新频率
     * - 对异常值不敏感
     */
    private BigDecimal fundingEma24h;
    
    // 下次资金费结算时间
    private Long nextFundingTime;
    
    // ===== 持仓量（Open Interest）=====
    
    // 持仓量（合约张数）
    private BigDecimal oi;
    
    // 持仓量（USD价值）
    private BigDecimal oiUsd;
    
    /**
     * 1分钟OI变化量（采样差分，非逐笔）
     * 
     * 注意：Binance OI通常5分钟更新一次
     * 在非更新分钟，使用前值填充并标记isOiCarried=true
     */
    private BigDecimal oiDelta1m;
    
    // OI变化百分比
    private Double oiDeltaPct;
    
    // OI是否为前值填充（用于区分真实更新和填充值）
    private Boolean isOiCarried;
    
    // ===== 元数据 =====
    
    // 计算时间
    private Long processTime;
}







