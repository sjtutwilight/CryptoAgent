package com.twilight.backend.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * 宏观PnL指标模型
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class MacroPnLMetrics {
    /**
     * 代币ID
     */
    private Long tokenId;

    /**
     * 数据时间
     */
    private LocalDateTime endTime;

    /**
     * 市值 (USD)
     */
    private BigDecimal mcapUsd;

    /**
     * 已实现市值 (USD)
     */
    private BigDecimal realizedCapUsd;

    /**
     * 网络价值 (USD)
     */
    private BigDecimal networkValueUsd;

    /**
     * 未实现盈利 (USD)
     */
    private BigDecimal unrealizedProfitUsd;

    /**
     * 未实现亏损 (USD)
     */
    private BigDecimal unrealizedLossUsd;

    /**
     * 网络未实现盈亏 (NUPL)
     */
    private Double nupl;

    /**
     * 市值与已实现价值比 (MVRV)
     */
    private Double mvrv;

    /**
     * 网络价值与交易量比 (NVT)
     */
    private Double nvtRatio;

    /**
     * 已花费产出盈利比 (SOPR)
     */
    private Double sopr;

    /**
     * 已实现盈亏 (USD)
     */
    private BigDecimal realizedPnlUsd;

    /**
     * 数据完整性标记
     */
    private Boolean hasMcap;
    private Boolean hasRealizedCap;
    private Boolean hasUnrealizedPnl;
    private Boolean hasSopr;

    /**
     * 最后更新时间
     */
    private LocalDateTime lastUpdated;
}
