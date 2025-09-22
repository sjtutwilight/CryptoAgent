package com.twilight.backend.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * 账户资产模型
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class AccountAsset {
    /**
     * 账户ID
     */
    private Long accountId;

    /**
     * 资产类型 (native/erc20/lp)
     */
    private String assetType;

    /**
     * 业务ID (token_id 或 pair_id)
     */
    private Long bizId;

    /**
     * 业务名称 (token symbol 或 pair的 token0/token1)
     */
    private String bizName;

    /**
     * 持有数量
     */
    private BigDecimal amount;

    /**
     * 价值 (USD)
     */
    private BigDecimal valueUsd;

    /**
     * 价格 (USD)
     */
    private BigDecimal price;

    /**
     * 观察时间
     */
    private LocalDateTime observedTime;

    /**
     * 标签位图
     */
    private Integer labelMask;
}
