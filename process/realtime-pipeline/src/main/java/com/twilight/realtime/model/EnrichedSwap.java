package com.twilight.realtime.model;

import java.io.Serializable;

/**
 * Carrier for swap facts enriched with account tags and token prices.
 */
public class EnrichedSwap implements Serializable {
    private OdsDexSwap swap;
    private AccountTag traderTag;
    private TokenPrice token0Price;
    private TokenPrice token1Price;
    private TokenPrice nativeTokenPrice;

    public EnrichedSwap() {
    }

    public OdsDexSwap getSwap() {
        return swap;
    }

    public void setSwap(OdsDexSwap swap) {
        this.swap = swap;
    }

    public AccountTag getTraderTag() {
        return traderTag;
    }

    public void setTraderTag(AccountTag traderTag) {
        this.traderTag = traderTag;
    }

    public TokenPrice getToken0Price() {
        return token0Price;
    }

    public void setToken0Price(TokenPrice token0Price) {
        this.token0Price = token0Price;
    }

    public TokenPrice getToken1Price() {
        return token1Price;
    }

    public void setToken1Price(TokenPrice token1Price) {
        this.token1Price = token1Price;
    }

    public TokenPrice getNativeTokenPrice() {
        return nativeTokenPrice;
    }

    public void setNativeTokenPrice(TokenPrice nativeTokenPrice) {
        this.nativeTokenPrice = nativeTokenPrice;
    }
}
