package com.twilight.backend.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

/**
 * Top Holder明细模型
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TopHolder {
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
     * 数据时间
     */
    private LocalDateTime endTime;

    /**
     * 账户标签列表
     */
    private List<String> labels;

    /**
     * 持有占比 (%)
     */
    private Double ownershipPercent;

    /**
     * 持有数量
     */
    private BigDecimal balance;

    /**
     * 持有价值 (USD)
     */
    private BigDecimal valueUsd;

    /**
     * 标签位图
     */
    private Integer labelMask;

    /**
     * 排名
     */
    private Integer rank;
}
