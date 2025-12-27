package com.twilight.realtime.model;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Instant;

public class DwdDexSwap implements Serializable {
    private int chainId;
    private String dexName;
    private String dexVersion;
    private String txHash;
    private int logIndex;
    private long blockNumber;
    private Instant blockTimestamp;
    private String poolAddress;
    private String routerAddress;
    private String traderAddress;
    private String senderAddress;
    private String recipientAddress;
    private String token0Address;
    private String token1Address;
    private String token0Symbol;
    private String token1Symbol;
    private int token0Decimals;
    private int token1Decimals;
    private BigDecimal amountToken0InRaw;
    private BigDecimal amountToken0OutRaw;
    private BigDecimal amountToken1InRaw;
    private BigDecimal amountToken1OutRaw;
    private double amountToken0In;
    private double amountToken0Out;
    private double amountToken1In;
    private double amountToken1Out;
    private String baseTokenAddress;
    private String quoteTokenAddress;
    private Double priceBaseInQuote;
    private Double priceToken0Usd;
    private Double priceToken1Usd;
    private Double swapValueUsd;
    private Double token0McapUsd;
    private Double token1McapUsd;
    private long gasUsed;
    private long effectiveGasPriceWei;
    private BigDecimal gasCostNative;
    private Double gasCostUsd;
    private Boolean traderIsWhale;
    private Boolean traderIsSmart;
    private Boolean traderIsBot;
    private String traderSegment;
    private String priceSource;
    private String accountTagVersion;
    private Instant ingestionTime;

    public DwdDexSwap() {}

    public int getChainId() {
        return chainId;
    }

    public void setChainId(int chainId) {
        this.chainId = chainId;
    }

    public String getDexName() {
        return dexName;
    }

    public void setDexName(String dexName) {
        this.dexName = dexName;
    }

    public String getDexVersion() {
        return dexVersion;
    }

    public void setDexVersion(String dexVersion) {
        this.dexVersion = dexVersion;
    }

    public String getTxHash() {
        return txHash;
    }

    public void setTxHash(String txHash) {
        this.txHash = txHash;
    }

    public int getLogIndex() {
        return logIndex;
    }

    public void setLogIndex(int logIndex) {
        this.logIndex = logIndex;
    }

    public long getBlockNumber() {
        return blockNumber;
    }

    public void setBlockNumber(long blockNumber) {
        this.blockNumber = blockNumber;
    }

    public Instant getBlockTimestamp() {
        return blockTimestamp;
    }

    public void setBlockTimestamp(Instant blockTimestamp) {
        this.blockTimestamp = blockTimestamp;
    }

    public String getPoolAddress() {
        return poolAddress;
    }

    public void setPoolAddress(String poolAddress) {
        this.poolAddress = poolAddress;
    }

    public String getRouterAddress() {
        return routerAddress;
    }

    public void setRouterAddress(String routerAddress) {
        this.routerAddress = routerAddress;
    }

    public String getTraderAddress() {
        return traderAddress;
    }

    public void setTraderAddress(String traderAddress) {
        this.traderAddress = traderAddress;
    }

    public String getSenderAddress() {
        return senderAddress;
    }

    public void setSenderAddress(String senderAddress) {
        this.senderAddress = senderAddress;
    }

    public String getRecipientAddress() {
        return recipientAddress;
    }

    public void setRecipientAddress(String recipientAddress) {
        this.recipientAddress = recipientAddress;
    }

    public String getToken0Address() {
        return token0Address;
    }

    public void setToken0Address(String token0Address) {
        this.token0Address = token0Address;
    }

    public String getToken1Address() {
        return token1Address;
    }

    public void setToken1Address(String token1Address) {
        this.token1Address = token1Address;
    }

    public String getToken0Symbol() {
        return token0Symbol;
    }

    public void setToken0Symbol(String token0Symbol) {
        this.token0Symbol = token0Symbol;
    }

    public String getToken1Symbol() {
        return token1Symbol;
    }

    public void setToken1Symbol(String token1Symbol) {
        this.token1Symbol = token1Symbol;
    }

    public int getToken0Decimals() {
        return token0Decimals;
    }

    public void setToken0Decimals(int token0Decimals) {
        this.token0Decimals = token0Decimals;
    }

    public int getToken1Decimals() {
        return token1Decimals;
    }

    public void setToken1Decimals(int token1Decimals) {
        this.token1Decimals = token1Decimals;
    }

    public BigDecimal getAmountToken0InRaw() {
        return amountToken0InRaw;
    }

    public void setAmountToken0InRaw(BigDecimal amountToken0InRaw) {
        this.amountToken0InRaw = amountToken0InRaw;
    }

    public BigDecimal getAmountToken0OutRaw() {
        return amountToken0OutRaw;
    }

    public void setAmountToken0OutRaw(BigDecimal amountToken0OutRaw) {
        this.amountToken0OutRaw = amountToken0OutRaw;
    }

    public BigDecimal getAmountToken1InRaw() {
        return amountToken1InRaw;
    }

    public void setAmountToken1InRaw(BigDecimal amountToken1InRaw) {
        this.amountToken1InRaw = amountToken1InRaw;
    }

    public BigDecimal getAmountToken1OutRaw() {
        return amountToken1OutRaw;
    }

    public void setAmountToken1OutRaw(BigDecimal amountToken1OutRaw) {
        this.amountToken1OutRaw = amountToken1OutRaw;
    }

    public double getAmountToken0In() {
        return amountToken0In;
    }

    public void setAmountToken0In(double amountToken0In) {
        this.amountToken0In = amountToken0In;
    }

    public double getAmountToken0Out() {
        return amountToken0Out;
    }

    public void setAmountToken0Out(double amountToken0Out) {
        this.amountToken0Out = amountToken0Out;
    }

    public double getAmountToken1In() {
        return amountToken1In;
    }

    public void setAmountToken1In(double amountToken1In) {
        this.amountToken1In = amountToken1In;
    }

    public double getAmountToken1Out() {
        return amountToken1Out;
    }

    public void setAmountToken1Out(double amountToken1Out) {
        this.amountToken1Out = amountToken1Out;
    }

    public String getBaseTokenAddress() {
        return baseTokenAddress;
    }

    public void setBaseTokenAddress(String baseTokenAddress) {
        this.baseTokenAddress = baseTokenAddress;
    }

    public String getQuoteTokenAddress() {
        return quoteTokenAddress;
    }

    public void setQuoteTokenAddress(String quoteTokenAddress) {
        this.quoteTokenAddress = quoteTokenAddress;
    }

    public Double getPriceBaseInQuote() {
        return priceBaseInQuote;
    }

    public void setPriceBaseInQuote(Double priceBaseInQuote) {
        this.priceBaseInQuote = priceBaseInQuote;
    }

    public Double getPriceToken0Usd() {
        return priceToken0Usd;
    }

    public void setPriceToken0Usd(Double priceToken0Usd) {
        this.priceToken0Usd = priceToken0Usd;
    }

    public Double getPriceToken1Usd() {
        return priceToken1Usd;
    }

    public void setPriceToken1Usd(Double priceToken1Usd) {
        this.priceToken1Usd = priceToken1Usd;
    }

    public Double getSwapValueUsd() {
        return swapValueUsd;
    }

    public void setSwapValueUsd(Double swapValueUsd) {
        this.swapValueUsd = swapValueUsd;
    }

    public Double getToken0McapUsd() {
        return token0McapUsd;
    }

    public void setToken0McapUsd(Double token0McapUsd) {
        this.token0McapUsd = token0McapUsd;
    }

    public Double getToken1McapUsd() {
        return token1McapUsd;
    }

    public void setToken1McapUsd(Double token1McapUsd) {
        this.token1McapUsd = token1McapUsd;
    }

    public long getGasUsed() {
        return gasUsed;
    }

    public void setGasUsed(long gasUsed) {
        this.gasUsed = gasUsed;
    }

    public long getEffectiveGasPriceWei() {
        return effectiveGasPriceWei;
    }

    public void setEffectiveGasPriceWei(long effectiveGasPriceWei) {
        this.effectiveGasPriceWei = effectiveGasPriceWei;
    }

    public BigDecimal getGasCostNative() {
        return gasCostNative;
    }

    public void setGasCostNative(BigDecimal gasCostNative) {
        this.gasCostNative = gasCostNative;
    }

    public Double getGasCostUsd() {
        return gasCostUsd;
    }

    public void setGasCostUsd(Double gasCostUsd) {
        this.gasCostUsd = gasCostUsd;
    }

    public Boolean getTraderIsWhale() {
        return traderIsWhale;
    }

    public void setTraderIsWhale(Boolean traderIsWhale) {
        this.traderIsWhale = traderIsWhale;
    }

    public Boolean getTraderIsSmart() {
        return traderIsSmart;
    }

    public void setTraderIsSmart(Boolean traderIsSmart) {
        this.traderIsSmart = traderIsSmart;
    }

    public Boolean getTraderIsBot() {
        return traderIsBot;
    }

    public void setTraderIsBot(Boolean traderIsBot) {
        this.traderIsBot = traderIsBot;
    }

    public String getTraderSegment() {
        return traderSegment;
    }

    public void setTraderSegment(String traderSegment) {
        this.traderSegment = traderSegment;
    }

    public String getPriceSource() {
        return priceSource;
    }

    public void setPriceSource(String priceSource) {
        this.priceSource = priceSource;
    }

    public String getAccountTagVersion() {
        return accountTagVersion;
    }

    public void setAccountTagVersion(String accountTagVersion) {
        this.accountTagVersion = accountTagVersion;
    }

    public Instant getIngestionTime() {
        return ingestionTime;
    }

    public void setIngestionTime(Instant ingestionTime) {
        this.ingestionTime = ingestionTime;
    }
}
