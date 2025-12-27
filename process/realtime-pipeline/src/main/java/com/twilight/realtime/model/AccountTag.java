package com.twilight.realtime.model;

import java.io.Serializable;
import java.time.Instant;

public class AccountTag implements Serializable {
    private int chainId;
    private String accountAddress;
    private boolean whale;
    private boolean smart;
    private boolean bot;
    private boolean cexDeposit;
    private short vipLevel;
    private String segment;
    private Instant updatedAt;

    public AccountTag() {
    }

    public int getChainId() {
        return chainId;
    }

    public void setChainId(int chainId) {
        this.chainId = chainId;
    }

    public String getAccountAddress() {
        return accountAddress;
    }

    public void setAccountAddress(String accountAddress) {
        this.accountAddress = accountAddress;
    }

    public boolean isWhale() {
        return whale;
    }

    public void setWhale(boolean whale) {
        this.whale = whale;
    }

    public boolean isSmart() {
        return smart;
    }

    public void setSmart(boolean smart) {
        this.smart = smart;
    }

    public boolean isBot() {
        return bot;
    }

    public void setBot(boolean bot) {
        this.bot = bot;
    }

    public boolean isCexDeposit() {
        return cexDeposit;
    }

    public void setCexDeposit(boolean cexDeposit) {
        this.cexDeposit = cexDeposit;
    }

    public short getVipLevel() {
        return vipLevel;
    }

    public void setVipLevel(short vipLevel) {
        this.vipLevel = vipLevel;
    }

    public String getSegment() {
        return segment;
    }

    public void setSegment(String segment) {
        this.segment = segment;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(Instant updatedAt) {
        this.updatedAt = updatedAt;
    }
}
