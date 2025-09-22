package com.twilight.aggregator.model;

import java.io.Serializable;
import java.math.BigDecimal;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

/**
 * 账户交易记录
 * 从DEX交易事件中提取的标准化账户交易数据
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class AccountTrade implements Serializable {
    private static final long serialVersionUID = 1L;

    private Long accountId;        // 账户ID
    private String accountAddress; // 账户地址
    private Long tokenId;          // Token ID
    private String tokenAddress;   // Token地址
    private Side side;             // 交易方向：BUY/SELL (相对该token)
    private BigDecimal quantity;   // 交易数量 (token原生单位)
    private BigDecimal priceUsd;   // 交易时USD单价
    private BigDecimal valueUsd;   // 交易USD价值 = quantity * priceUsd
    private Long blockId;          // 区块号
    private Long blockTimeMs;      // 区块时间戳(毫秒)
    private String txHash;         // 交易哈希
    private Integer logIndex;      // 日志索引
    private String accountTag;     // 账户标签 (smart_money, whale, fresh等)

    /**
     * 交易方向枚举
     */
    public enum Side {
        BUY,   // 买入：用户从池子获得token
        SELL   // 卖出：用户向池子提供token
    }

    /**
     * 检查是否为买入交易
     */
    public boolean isBuy() {
        return Side.BUY.equals(side);
    }

    /**
     * 检查是否为卖出交易
     */
    public boolean isSell() {
        return Side.SELL.equals(side);
    }

    /**
     * 获取交易方向的字符串表示
     */
    public String getSideString() {
        return side != null ? side.name().toLowerCase() : "unknown";
    }

    /**
     * 生成交易的唯一键
     */
    public String getKey() {
        return String.format("%s_%s", 
                           accountAddress != null ? accountAddress.toLowerCase() : "unknown",
                           tokenAddress != null ? tokenAddress.toLowerCase() : "unknown");
    }

    // 手动生成getter/setter方法以解决Lombok编译问题
    public Long getAccountId() { return accountId; }
    public void setAccountId(Long accountId) { this.accountId = accountId; }
    
    public String getAccountAddress() { return accountAddress; }
    public void setAccountAddress(String accountAddress) { this.accountAddress = accountAddress; }
    
    public Long getTokenId() { return tokenId; }
    public void setTokenId(Long tokenId) { this.tokenId = tokenId; }
    
    public String getTokenAddress() { return tokenAddress; }
    public void setTokenAddress(String tokenAddress) { this.tokenAddress = tokenAddress; }
    
    public Side getSide() { return side; }
    public void setSide(Side side) { this.side = side; }
    
    public BigDecimal getQuantity() { return quantity; }
    public void setQuantity(BigDecimal quantity) { this.quantity = quantity; }
    
    public BigDecimal getPriceUsd() { return priceUsd; }
    public void setPriceUsd(BigDecimal priceUsd) { this.priceUsd = priceUsd; }
    
    public BigDecimal getValueUsd() { return valueUsd; }
    public void setValueUsd(BigDecimal valueUsd) { this.valueUsd = valueUsd; }
    
    public Long getBlockId() { return blockId; }
    public void setBlockId(Long blockId) { this.blockId = blockId; }
    
    public Long getBlockTimeMs() { return blockTimeMs; }
    public void setBlockTimeMs(Long blockTimeMs) { this.blockTimeMs = blockTimeMs; }
    
    public String getTxHash() { return txHash; }
    public void setTxHash(String txHash) { this.txHash = txHash; }
    
    public Integer getLogIndex() { return logIndex; }
    public void setLogIndex(Integer logIndex) { this.logIndex = logIndex; }
    
    public String getAccountTag() { return accountTag; }
    public void setAccountTag(String accountTag) { this.accountTag = accountTag; }
}
