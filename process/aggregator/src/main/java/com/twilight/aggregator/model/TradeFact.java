package com.twilight.aggregator.model;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDateTime;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

/**
 * 账户交易事实表模型
 * 映射到 ch_account_trade_fact 表结构
 * 
 * 用于存储所有账户的Token交易事实数据，支持Token和Account两个维度的查询
 */
@Data
@NoArgsConstructor  
@AllArgsConstructor
public class TradeFact implements Serializable {
    private static final long serialVersionUID = 1L;

    // 维度字段
    private Integer chainId;         // 链ID (31337为测试网)
    private Long tokenId;           // Token ID
    private Long accountId;         // 账户ID
    private String accountAddress;  // 账户地址
    private String side;            // 'BUY' | 'SELL'
    private Long pairId;            // 交易对ID
    private String pairAddress;     // 交易对地址

    // 业务时间
    private LocalDateTime blockTime; // 区块时间
    private Long blockId;           // 区块号

    // 唯一定位
    private String txHash;          // 交易哈希
    private Integer logIndex;       // 日志索引

    // 度量字段
    private BigDecimal qty;         // 交易数量
    private BigDecimal priceUsd;    // USD单价
    private BigDecimal valueUsd;    // USD价值

    // 标签位图 (参见design文档中的标签定义)
    private Integer labelMask;      // 标签位图 (0=default)

    /**
     * 构造器：从AccountTrade转换
     */
    public static TradeFact fromAccountTrade(AccountTrade trade, Long pairId) {
        TradeFact fact = new TradeFact();
        
        // 基础维度字段
        fact.setChainId(31337); // 测试网链ID
        fact.setTokenId(trade.getTokenId());
        fact.setAccountId(trade.getAccountId());
        fact.setSide(trade.getSide() != null ? trade.getSide().name() : "UNKNOWN");
        fact.setPairId(pairId);
        
        // 时间字段
        if (trade.getBlockTimeMs() != null) {
            fact.setBlockTime(LocalDateTime.ofEpochSecond(
                trade.getBlockTimeMs() / 1000, 0, 
                java.time.ZoneOffset.UTC
            ));
        }
        fact.setBlockId(trade.getBlockId());
        
        // 唯一定位
        fact.setTxHash(trade.getTxHash());
        fact.setLogIndex(trade.getLogIndex());
        
        // 度量字段
        fact.setQty(trade.getQuantity());
        fact.setPriceUsd(trade.getPriceUsd());
        fact.setValueUsd(trade.getValueUsd());
        
        // 标签位图 (默认为0，将在增强器中设置)
        fact.setLabelMask(0);
        
        return fact;
    }

    /**
     * 生成用于ClickHouse插入的SQL字段值
     */
    public String toInsertValues() {
        return String.format(
            "(%d, %d, %d, '%s', %d, '%s', %d, '%s', %d, %s, %s, %s, %d)",
            chainId,
            tokenId,
            accountId, 
            side,
            pairId,
            blockTime != null ? blockTime.toString().replace("T", " ") : "1970-01-01 00:00:00",
            blockId,
            txHash != null ? txHash : "",
            logIndex,
            qty != null ? qty.toString() : "0",
            priceUsd != null ? priceUsd.toString() : "0",
            valueUsd != null ? valueUsd.toString() : "0",
            labelMask
        );
    }
}
