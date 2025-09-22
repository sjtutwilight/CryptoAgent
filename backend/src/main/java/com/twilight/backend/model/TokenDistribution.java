package com.twilight.backend.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * 代币分布宏观指标模型
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TokenDistribution {
    /**
     * 代币ID
     */
    private Long tokenId;

    /**
     * 数据时间
     */
    private LocalDateTime endTime;

    /**
     * 持有人数
     */
    private Integer holdersCount;

    /**
     * Top2持仓占比 (%)
     */
    private Double top2SharePercent;

    /**
     * 中位数持有者价值 (USD)
     */
    private BigDecimal medianHolderValueUsd;

    /**
     * 平均持有者价值 (USD)
     */
    private BigDecimal avgHolderValueUsd;

    /**
     * Top2持仓价值 (USD)
     */
    private BigDecimal top2ValueUsd;

    /**
     * 总持仓价值 (USD)
     */
    private BigDecimal totalValueUsd;

    /**
     * 新钱包持有者数量占比 (%)
     */
    private Double freshHolderCountShare;

    /**
     * 新钱包持仓价值占比 (%)
     */
    private Double freshHolderValueShare;

    /**
     * 代币集中度指数（基于Top2占比计算）
     */
    private Double concentrationIndex;
}
