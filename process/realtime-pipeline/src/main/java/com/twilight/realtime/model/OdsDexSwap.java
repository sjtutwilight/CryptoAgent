package com.twilight.realtime.model;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Instant;

/**
 * Representation of ods_dex_swap_full_* schema emitted by the ingestion layer.
 */
public class OdsDexSwap implements Serializable {
    private int chainId;
    private String dexName;
    private String dexVersion;
    private String txHash;
    private int logIndex;
    private long blockNumber;
    private Instant blockTimestamp;
    private String traderAddress;
    private String routerAddress;
    private Integer status;
    private long gasUsed;
    private long effectiveGasPriceWei;
    private BigDecimal gasCostNative;
    private String poolAddress;
    private String senderAddress;
    private String recipientAddress;
    private String token0Address;
    private String token1Address;
    private String token0Symbol;
    private String token1Symbol;
    private int token0Decimals;
    private int token1Decimals;
    private BigDecimal amount0Raw;
    private BigDecimal amount1Raw;
    private String amount0Direction;
    private String amount1Direction;
    private BigDecimal sqrtPriceX96;
    private BigDecimal liquidity;
    private Integer tick;
    private Instant ingestionTime;

    public OdsDexSwap() {
    }

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

    public String getTraderAddress() {
        return traderAddress;
    }

    public void setTraderAddress(String traderAddress) {
        this.traderAddress = traderAddress;
    }

    public String getRouterAddress() {
        return routerAddress;
    }

    public void setRouterAddress(String routerAddress) {
        this.routerAddress = routerAddress;
    }

    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
        this.status = status;
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

    public String getPoolAddress() {
        return poolAddress;
    }

    public void setPoolAddress(String poolAddress) {
        this.poolAddress = poolAddress;
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

    public BigDecimal getAmount0Raw() {
        return amount0Raw;
    }

    public void setAmount0Raw(BigDecimal amount0Raw) {
        this.amount0Raw = amount0Raw;
    }

    public BigDecimal getAmount1Raw() {
        return amount1Raw;
    }

    public void setAmount1Raw(BigDecimal amount1Raw) {
        this.amount1Raw = amount1Raw;
    }

    public String getAmount0Direction() {
        return amount0Direction;
    }

    public void setAmount0Direction(String amount0Direction) {
        this.amount0Direction = amount0Direction;
    }

    public String getAmount1Direction() {
        return amount1Direction;
    }

    public void setAmount1Direction(String amount1Direction) {
        this.amount1Direction = amount1Direction;
    }

    public BigDecimal getSqrtPriceX96() {
        return sqrtPriceX96;
    }

    public void setSqrtPriceX96(BigDecimal sqrtPriceX96) {
        this.sqrtPriceX96 = sqrtPriceX96;
    }

    public BigDecimal getLiquidity() {
        return liquidity;
    }

    public void setLiquidity(BigDecimal liquidity) {
        this.liquidity = liquidity;
    }

    public Integer getTick() {
        return tick;
    }

    public void setTick(Integer tick) {
        this.tick = tick;
    }

    public Instant getIngestionTime() {
        return ingestionTime;
    }

    public void setIngestionTime(Instant ingestionTime) {
        this.ingestionTime = ingestionTime;
    }
}
