package com.twilight.aggregator.model;

import java.util.Objects;

/**
 * Token指标数据模型
 * 包含价格、市值、FDV、流动性等指标
 */
public class TokenMetrics {
    private String tokenAddress;
    private Double tokenPriceUsd;
    private Double mcap;
    private Double fdv;
    private Double liquidityUsd;
    private long timestamp;
    
    public TokenMetrics() {
        this.timestamp = System.currentTimeMillis();
    }
    
    public TokenMetrics(String tokenAddress) {
        this.tokenAddress = tokenAddress;
        this.timestamp = System.currentTimeMillis();
    }
    
    public String getTokenAddress() {
        return tokenAddress;
    }
    
    public void setTokenAddress(String tokenAddress) {
        this.tokenAddress = tokenAddress;
    }
    
    public Double getTokenPriceUsd() {
        return tokenPriceUsd;
    }
    
    public void setTokenPriceUsd(Double tokenPriceUsd) {
        this.tokenPriceUsd = tokenPriceUsd;
    }
    
    public Double getMcap() {
        return mcap;
    }
    
    public void setMcap(Double mcap) {
        this.mcap = mcap;
    }
    
    public Double getFdv() {
        return fdv;
    }
    
    public void setFdv(Double fdv) {
        this.fdv = fdv;
    }
    
    public Double getLiquidityUsd() {
        return liquidityUsd;
    }
    
    public void setLiquidityUsd(Double liquidityUsd) {
        this.liquidityUsd = liquidityUsd;
    }
    
    public long getTimestamp() {
        return timestamp;
    }
    
    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
    
    /**
     * 检查是否有所有必需的指标
     */
    public boolean hasAllMetrics() {
        return tokenPriceUsd != null && mcap != null && fdv != null && liquidityUsd != null;
    }
    
    /**
     * 检查是否至少有价格信息
     */
    public boolean hasPrice() {
        return tokenPriceUsd != null;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TokenMetrics that = (TokenMetrics) o;
        return Objects.equals(tokenAddress, that.tokenAddress);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(tokenAddress);
    }
    
    @Override
    public String toString() {
        return "TokenMetrics{" +
                "tokenAddress='" + tokenAddress + '\'' +
                ", tokenPriceUsd=" + tokenPriceUsd +
                ", mcap=" + mcap +
                ", fdv=" + fdv +
                ", liquidityUsd=" + liquidityUsd +
                ", timestamp=" + timestamp +
                '}';
    }
}
