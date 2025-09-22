package com.twilight.backend.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * 代币宏观指标模型
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TokenMetrics {
    /**
     * 代币ID
     */
    private Long tokenId;

    /**
     * 数据时间
     */
    private LocalDateTime endTime;

    /**
     * 当前价格 (USD)
     */
    private BigDecimal currentPrice;

    /**
     * 完全稀释估值 (FDV)
     */
    private BigDecimal fdv;

    /**
     * 市值 (MCAP)
     */
    private BigDecimal mcap;

    /**
     * 流动性 (USD)
     */
    private BigDecimal liquidity;

    /**
     * FDV/MCAP 比值
     */
    private BigDecimal fdvMcapRatio;

    /**
     * MCAP/Liquidity 比值
     */
    private BigDecimal mcapLiquidityRatio;

    /**
     * FDV/Liquidity 比值
     */
    private BigDecimal fdvLiquidityRatio;
}
