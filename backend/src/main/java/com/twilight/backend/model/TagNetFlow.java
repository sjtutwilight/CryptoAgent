package com.twilight.backend.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * 标签净流入数据模型
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TagNetFlow {
    /**
     * 代币ID
     */
    private Long tokenId;

    /**
     * 时间点
     */
    private LocalDateTime endTime;

    /**
     * 标签 (exchange, smart_money, whale, public_figure, fresh_wallet, top_pnl)
     */
    private String tag;

    /**
     * 标签中文名称
     */
    private String tagName;

    /**
     * 净流入金额 (USD)
     */
    private BigDecimal netFlowUsd;

    /**
     * 流入金额 (USD)
     */
    private BigDecimal inflowUsd;

    /**
     * 流出金额 (USD)
     */
    private BigDecimal outflowUsd;

    /**
     * 交易者数量
     */
    private Integer tradersCount;

    /**
     * 买入交易数
     */
    private Integer buyCount;

    /**
     * 卖出交易数
     */
    private Integer sellCount;
}
