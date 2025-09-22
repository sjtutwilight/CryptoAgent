package com.twilight.aggregator.model;

import lombok.Data;
import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * Account Balance模型，对应ClickHouse ch_account_balance_snapshot表
 */
@Data
public class AccountBalance implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private Long accountId;
    private LocalDateTime observedTime;
    private Long blockId;      // blockchain block number for snapshot
    private String assetType;  // "ERC20" 或 "LP"
    private Long bizId;        // token_id 或 pair_id
    private BigDecimal amount;
    private BigDecimal priceUsd;
    private BigDecimal valueUsd;
    private Integer labelMask; // 位图标签
    
    // 辅助字段（用于处理过程，不写入ClickHouse）
    private String accountAddress;
    private String contractAddress;
    private String bizName;     // token symbol 或 pair name
}
