package com.twilight.backend.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * 标签维度持仓模型
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TagHolding {
    /**
     * 代币ID
     */
    private Long tokenId;

    /**
     * 数据时间
     */
    private LocalDateTime endTime;

    /**
     * 标签 (fresh_wallet, whale, smart_money, cex)
     */
    private String tag;

    /**
     * 标签中文名称
     */
    private String tagName;

    /**
     * 持仓价值 (USD)
     */
    private BigDecimal valueUsd;

    /**
     * 持有者数量
     */
    private Integer holdersCount;

    /**
     * 1分钟变化百分比 (%)
     */
    private Double changePercent1Min;

    /**
     * 与前一个时间点的变化金额 (USD)
     */
    private BigDecimal valueChangeUsd;

    /**
     * 占总持仓的比例 (%)
     */
    private Double totalSharePercent;
}
