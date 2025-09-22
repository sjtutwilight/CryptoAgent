package com.twilight.aggregator.model;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDateTime;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

/**
 * 已实现盈亏事件
 * 记录每次卖出交易产生的已实现盈亏详情
 * 对应ClickHouse表 ch_pnl_realized_event
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class PnLRealizedEvent implements Serializable {
    private static final long serialVersionUID = 1L;

    // 基础信息
    private Long tokenId;           // Token ID
    private Long accountId;         // 账户ID
    private Long blockId;           // 区块ID
    private LocalDateTime blockTime; // 区块时间
    
    // 已实现盈亏详情
    private BigDecimal realizedQty;         // 已实现数量（卖出数量）
    private BigDecimal realizedCostUsd;     // 已实现成本（数量 * 平均成本）
    private BigDecimal realizedProceedsUsd; // 已实现收入（数量 * 卖出价格）
    private BigDecimal realizedPnLUsd;      // 已实现盈亏（收入 - 成本）
    
    /**
     * 创建已实现盈亏事件
     */
    public static PnLRealizedEvent create(Long tokenId, Long accountId, Long blockId, 
                                        LocalDateTime blockTime, BigDecimal realizedQty,
                                        BigDecimal realizedCostUsd, BigDecimal realizedProceedsUsd,
                                        BigDecimal realizedPnLUsd) {
        return new PnLRealizedEvent(tokenId, accountId, blockId, blockTime,
                                  realizedQty, realizedCostUsd, realizedProceedsUsd, realizedPnLUsd);
    }
    
    /**
     * 验证事件数据有效性
     */
    public boolean isValid() {
        return tokenId != null && tokenId > 0
            && accountId != null && accountId > 0
            && blockId != null && blockId > 0
            && blockTime != null
            && realizedQty != null && realizedQty.compareTo(BigDecimal.ZERO) > 0
            && realizedCostUsd != null
            && realizedProceedsUsd != null
            && realizedPnLUsd != null;
    }
}
