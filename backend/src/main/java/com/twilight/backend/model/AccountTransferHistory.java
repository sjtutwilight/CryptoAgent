package com.twilight.backend.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * 账户转账历史聚合模型
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class AccountTransferHistory {
    /**
     * 账户ID
     */
    private Long accountId;

    /**
     * 代币ID
     */
    private Long tokenId;

    /**
     * 时间窗口结束时间
     */
    private LocalDateTime endTime;

    /**
     * 买入交易数
     */
    private Integer buyTxCount;

    /**
     * 卖出交易数
     */
    private Integer sellTxCount;

    /**
     * 总交易数
     */
    private Integer totalTxCount;

    /**
     * 买入交易量 (USD)
     */
    private BigDecimal buyVolumeUsd;

    /**
     * 卖出交易量 (USD)
     */
    private BigDecimal sellVolumeUsd;

    /**
     * 总交易量 (USD)
     */
    private BigDecimal totalVolumeUsd;

    /**
     * 净买入量 (USD) = 买入量 - 卖出量
     */
    private BigDecimal netBuyVolumeUsd;
}
