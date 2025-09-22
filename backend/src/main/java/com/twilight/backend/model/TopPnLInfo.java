package com.twilight.backend.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

/**
 * Top PnL信息模型
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TopPnLInfo {
    /**
     * 账户ID
     */
    private Long accountId;

    /**
     * 账户地址
     */
    private String address;

    /**
     * 代币ID
     */
    private Long tokenId;

    /**
     * 总盈亏 (USD)
     */
    private BigDecimal totalPnlUsd;

    /**
     * 投资回报率 (%)
     */
    private Double roiPercent;

    /**
     * 已实现盈亏 (USD)
     */
    private BigDecimal realizedPnlUsd;

    /**
     * 未实现盈亏 (USD)
     */
    private BigDecimal unrealizedPnlUsd;

    /**
     * 仍持有百分比 (%)
     */
    private Double stillHoldingPercent;

    /**
     * 当前持仓
     */
    private BigDecimal position;

    /**
     * 平均成本
     */
    private BigDecimal avgCost;

    /**
     * 最新价格
     */
    private BigDecimal lastPrice;

    /**
     * 最近交易时间
     */
    private LocalDateTime lastTxTime;

    /**
     * 账户标签列表
     */
    private List<String> labels;

    /**
     * 标签位图
     */
    private Integer labelMask;
}
