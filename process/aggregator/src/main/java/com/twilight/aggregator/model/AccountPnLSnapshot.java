package com.twilight.aggregator.model;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonFilter;

/**
 * 账户PnL快照
 * 对应ClickHouse表 ch_account_pnl_current_ma 的输出结构
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonFilter("excludeKeyValid")
public class AccountPnLSnapshot implements Serializable {
    private static final long serialVersionUID = 1L;

    @JsonProperty("account_id")
    private Long accountId;

    @JsonProperty("account_address")
    private String accountAddress;

    @JsonProperty("token_id")
    private Long tokenId;

    @JsonProperty("position")
    private BigDecimal position;           // 当前剩余仓位数量

    @JsonProperty("avg_cost")
    private BigDecimal avgCost;           // 移动加权平均成本价

    @JsonProperty("realized_cost_usd")
    private BigDecimal realizedCostUsd;   // 已卖出部分的成本总额

    @JsonProperty("realized_proceeds_usd")
    private BigDecimal realizedProceedsUsd; // 已卖出部分的收入总额

    @JsonProperty("realized_pnl_usd")
    private BigDecimal realizedPnLUsd;    // 累计已实现盈亏

    @JsonProperty("last_price_usd")
    private BigDecimal lastPriceUsd;      // 最新价格

    @JsonProperty("unrealized_pnl_usd")
    private BigDecimal unrealizedPnLUsd;  // 未实现盈亏

    @JsonProperty("total_pnl_usd")
    private BigDecimal totalPnLUsd;       // 总盈亏

    @JsonProperty("roi_pct")
    private Double roiPct;                // 投资回报率(百分比)

    @JsonProperty("holding_pct")
    private Double holdingPct;            // 仍持有的仓位占比(百分比)

    @JsonProperty("last_tx_time")
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime lastTxTime;     // 最后交易时间

    @JsonProperty("version")
    private Long version;                 // 版本号，使用区块号

    /**
     * 从毫秒时间戳创建快照对象的便捷构造函数
     */
    public static AccountPnLSnapshot fromTimestamp(Long accountId, String accountAddress, Long tokenId, 
                                                  BigDecimal position, BigDecimal avgCost,
                                                  BigDecimal realizedCostUsd, BigDecimal realizedProceedsUsd,
                                                  BigDecimal realizedPnLUsd, BigDecimal lastPriceUsd,
                                                  BigDecimal unrealizedPnLUsd, BigDecimal totalPnLUsd,
                                                  Double roiPct, Double holdingPct,
                                                  Long lastTxTimeMs, Long version) {
        AccountPnLSnapshot snapshot = new AccountPnLSnapshot();
        snapshot.setAccountId(accountId);
        snapshot.setAccountAddress(accountAddress);
        snapshot.setTokenId(tokenId);
        snapshot.setPosition(position);
        snapshot.setAvgCost(avgCost);
        snapshot.setRealizedCostUsd(realizedCostUsd);
        snapshot.setRealizedProceedsUsd(realizedProceedsUsd);
        snapshot.setRealizedPnLUsd(realizedPnLUsd);
        snapshot.setLastPriceUsd(lastPriceUsd);
        snapshot.setUnrealizedPnLUsd(unrealizedPnLUsd);
        snapshot.setTotalPnLUsd(totalPnLUsd);
        snapshot.setRoiPct(roiPct);
        snapshot.setHoldingPct(holdingPct);
        snapshot.setLastTxTime(LocalDateTime.ofInstant(Instant.ofEpochMilli(lastTxTimeMs), ZoneOffset.UTC));
        snapshot.setVersion(version);
        return snapshot;
    }

    /**
     * 获取毫秒时间戳
     */
    public Long getLastTxTimeMs() {
        return lastTxTime != null ? lastTxTime.toInstant(ZoneOffset.UTC).toEpochMilli() : null;
    }

    /**
     * 检查是否为有效的PnL快照
     */
    public boolean isValid() {
        return accountId != null && tokenId != null && 
               position != null && avgCost != null && 
               lastTxTime != null && version != null;
    }

    /**
     * 获取快照的唯一键
     */
    public String getKey() {
        return String.format("%d_%d", accountId, tokenId);
    }

    // 手动生成getter/setter方法以解决Lombok编译问题
    public Long getAccountId() { return accountId; }
    public void setAccountId(Long accountId) { this.accountId = accountId; }
    
    public String getAccountAddress() { return accountAddress; }
    public void setAccountAddress(String accountAddress) { this.accountAddress = accountAddress; }
    
    public Long getTokenId() { return tokenId; }
    public void setTokenId(Long tokenId) { this.tokenId = tokenId; }
    
    public BigDecimal getPosition() { return position; }
    public void setPosition(BigDecimal position) { this.position = position; }
    
    public BigDecimal getAvgCost() { return avgCost; }
    public void setAvgCost(BigDecimal avgCost) { this.avgCost = avgCost; }
    
    public BigDecimal getRealizedCostUsd() { return realizedCostUsd; }
    public void setRealizedCostUsd(BigDecimal realizedCostUsd) { this.realizedCostUsd = realizedCostUsd; }
    
    public BigDecimal getRealizedProceedsUsd() { return realizedProceedsUsd; }
    public void setRealizedProceedsUsd(BigDecimal realizedProceedsUsd) { this.realizedProceedsUsd = realizedProceedsUsd; }
    
    public BigDecimal getRealizedPnLUsd() { return realizedPnLUsd; }
    public void setRealizedPnLUsd(BigDecimal realizedPnLUsd) { this.realizedPnLUsd = realizedPnLUsd; }
    
    public BigDecimal getLastPriceUsd() { return lastPriceUsd; }
    public void setLastPriceUsd(BigDecimal lastPriceUsd) { this.lastPriceUsd = lastPriceUsd; }
    
    public BigDecimal getUnrealizedPnLUsd() { return unrealizedPnLUsd; }
    public void setUnrealizedPnLUsd(BigDecimal unrealizedPnLUsd) { this.unrealizedPnLUsd = unrealizedPnLUsd; }
    
    public BigDecimal getTotalPnLUsd() { return totalPnLUsd; }
    public void setTotalPnLUsd(BigDecimal totalPnLUsd) { this.totalPnLUsd = totalPnLUsd; }
    
    public Double getRoiPct() { return roiPct; }
    public void setRoiPct(Double roiPct) { this.roiPct = roiPct; }
    
    public Double getHoldingPct() { return holdingPct; }
    public void setHoldingPct(Double holdingPct) { this.holdingPct = holdingPct; }
    
    public LocalDateTime getLastTxTime() { return lastTxTime; }
    public void setLastTxTime(LocalDateTime lastTxTime) { this.lastTxTime = lastTxTime; }
    
    public Long getVersion() { return version; }
    public void setVersion(Long version) { this.version = version; }
}
