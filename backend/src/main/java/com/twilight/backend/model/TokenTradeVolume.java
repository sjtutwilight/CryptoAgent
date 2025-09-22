package com.twilight.backend.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * 代币交易量统计模型
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TokenTradeVolume {
    /**
     * 代币ID
     */
    private Long tokenId;

    /**
     * 时间窗口 (1min, 5min, 1h, 1d)
     */
    private String timeWindow;

    /**
     * 结束时间
     */
    private LocalDateTime endTime;

    /**
     * 标签 (all, exchange, smart_money, whale, fresh_wallet)
     */
    private String tag;

    /**
     * 交易数量
     */
    private Integer txCount;

    /**
     * 买入交易数
     */
    private Integer buyCount;

    /**
     * 卖出交易数
     */
    private Integer sellCount;

    /**
     * 总交易量 (USD)
     */
    private BigDecimal volumeUsd;

    /**
     * 买入交易量 (USD)
     */
    private BigDecimal buyVolumeUsd;

    /**
     * 卖出交易量 (USD)
     */
    private BigDecimal sellVolumeUsd;

    /**
     * 买入压力 (USD)
     */
    private BigDecimal buyPressureUsd;

    /**
     * 代币价格 (USD)
     */
    private BigDecimal tokenPriceUsd;
}
