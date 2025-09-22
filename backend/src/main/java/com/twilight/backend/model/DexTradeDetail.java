package com.twilight.backend.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

/**
 * DEX交易明细模型
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class DexTradeDetail {
    /**
     * 交易哈希
     */
    private String txHash;

    /**
     * 区块时间
     */
    private LocalDateTime blockTime;

    /**
     * 发送地址
     */
    private String fromAddress;

    /**
     * 接收地址
     */
    private String toAddress;

    /**
     * 代币ID
     */
    private Long tokenId;

    /**
     * 代币符号
     */
    private String tokenSymbol;

    /**
     * 代币合约地址
     */
    private String tokenAddress;

    /**
     * 交易方向 (BUY/SELL)
     */
    private String side;

    /**
     * 交易数量
     */
    private BigDecimal qty;

    /**
     * 价格 (USD)
     */
    private BigDecimal priceUsd;

    /**
     * 交易金额 (USD)
     */
    private BigDecimal valueUsd;

    /**
     * 交易对ID
     */
    private Long pairId;

    /**
     * 日志索引
     */
    private Integer logIndex;

    /**
     * 账户标签列表
     */
    private List<String> labels;

    /**
     * 标签位图
     */
    private Integer labelMask;
}
