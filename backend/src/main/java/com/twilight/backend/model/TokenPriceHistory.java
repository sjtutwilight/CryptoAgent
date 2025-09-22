package com.twilight.backend.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * 代币历史价格模型
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TokenPriceHistory {
    /**
     * 代币ID
     */
    private Long tokenId;

    /**
     * 时间点
     */
    private LocalDateTime endTime;

    /**
     * 价格 (USD)
     */
    private BigDecimal price;

    /**
     * 价格变化量 (USD)
     */
    private BigDecimal priceChange;

    /**
     * 价格变化百分比
     */
    private BigDecimal priceChangePercent;

    /**
     * 交易量 (USD)
     */
    private BigDecimal volumeUsd;

    /**
     * 市值 (USD)
     */
    private BigDecimal mcap;
}
