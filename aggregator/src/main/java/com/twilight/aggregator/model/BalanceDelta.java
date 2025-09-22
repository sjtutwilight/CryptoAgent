package com.twilight.aggregator.model;

import lombok.Data;
import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * Balance Delta模型，表示余额变化事件
 */
@Data
public class BalanceDelta implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private Long accountId;
    private String accountAddress;
    private String assetType;     // "ERC20" 或 "LP"
    private Long bizId;           // token_id 或 pair_id
    private String contractAddress;
    private BigDecimal delta;     // 余额变化量，正数为增加，负数为减少
    private LocalDateTime eventTime;
    private Long blockId;         // blockchain block number
    private String transactionHash;
    private BigDecimal priceUsd;  // token价格（来自价格广播流）
    
    // 辅助字段
    private String fromAddress;
    private String toAddress;
    private String bizName;       // token symbol 或 pair name
}
