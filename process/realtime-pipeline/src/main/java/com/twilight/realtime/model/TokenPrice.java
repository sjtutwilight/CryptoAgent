package com.twilight.realtime.model;

import java.io.Serializable;
import java.time.Instant;

public class TokenPrice implements Serializable {
    private int chainId;
    private String tokenAddress;
    private double priceUsd;
    private double mcapUsd;
    private String source;
    private Instant updatedAt;

    public TokenPrice() {
    }

    public int getChainId() {
        return chainId;
    }

    public void setChainId(int chainId) {
        this.chainId = chainId;
    }

    public String getTokenAddress() {
        return tokenAddress;
    }

    public void setTokenAddress(String tokenAddress) {
        this.tokenAddress = tokenAddress;
    }

    public double getPriceUsd() {
        return priceUsd;
    }

    public void setPriceUsd(double priceUsd) {
        this.priceUsd = priceUsd;
    }

    public double getMcapUsd() {
        return mcapUsd;
    }

    public void setMcapUsd(double mcapUsd) {
        this.mcapUsd = mcapUsd;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(Instant updatedAt) {
        this.updatedAt = updatedAt;
    }
}
