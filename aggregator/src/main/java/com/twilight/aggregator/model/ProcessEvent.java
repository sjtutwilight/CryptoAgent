package com.twilight.aggregator.model;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Map;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class 
ProcessEvent implements Serializable {
    private static final long serialVersionUID = 1L;

    // 基础字段（所有事件共有）
    private String eventName;
    private String contractAddress;

    private String transactionHash;
    private Long blockId;
    private String fromAddress;
    private Integer chainId;
    private Long timestamp;
    private Integer logIndex;

    private String contractType;     // "erc20" 或 "dex"
    private Long bizId;           // tokenId（ERC20）或 pairId（dex）
        
    // 强类型事件数据（只有一个非null）
    private ERC20TransferData erc20Data;  // 包含ERC20和LP Token的Transfer事件
    private DexSwapData dexSwapData;
    private LPMintData lpMintData;
    private LPBurnData lpBurnData;

    // 元数据（由前置异步增强填充）
    private AccountMetadata accountMetadata; // 账户元数据（含真实 account_id 等）
    //assetType为ERC20时
    private TokenMetadata tokenMetadata;     // Token 元数据（symbol、decimals 等）
    //assetType为LP时
    private PairMetadata pairMetadata;       // 交易对元数据（token0/1、pairId 等）
    
    /**
     * ERC20 Transfer事件数据（包含普通ERC20和LP Token）
     */
    @Data
    @NoArgsConstructor
    public static class ERC20TransferData implements Serializable {
        private static final long serialVersionUID = 1L;
        private String toAddress;
        private BigDecimal amount;
    }
    
    /**
     * DEX Swap事件数据
     */
    @Data
    @NoArgsConstructor
    public static class DexSwapData implements Serializable {
        private static final long serialVersionUID = 1L;      
        // Swap特定字段
        private BigDecimal amount0In;
        private BigDecimal amount0Out;
        private BigDecimal amount1In;
        private BigDecimal amount1Out;
        private String to;
    }
    
    /**
     * LP Mint事件数据
     */
    @Data
    @NoArgsConstructor
    public static class LPMintData implements Serializable {
        private static final long serialVersionUID = 1L;
        // Mint特定字段
        private BigDecimal amount0;
        private BigDecimal amount1;
        private String sender;
        private String to;
    }
    
    /**
     * LP Burn事件数据
     */
    @Data
    @NoArgsConstructor
    public static class LPBurnData implements Serializable {
        private static final long serialVersionUID = 1L;
        // Burn特定字段
        private BigDecimal amount0;
        private BigDecimal amount1;
        private String sender;
        private String to;

    }
    
    /**
     * 事件类型枚举
     */
    public enum EventType {
        ERC20_TRANSFER, DEX_SWAP, LP_MINT, LP_BURN, UNKNOWN
    }
    
    /**
     * 获取事件类型
     */
    public EventType getEventType() {
        if (erc20Data != null) return EventType.ERC20_TRANSFER;
        if (dexSwapData != null) return EventType.DEX_SWAP;
        if (lpMintData != null) return EventType.LP_MINT;
        if (lpBurnData != null) return EventType.LP_BURN;
        return EventType.UNKNOWN;
    }
    
    /**
     * 是否为DEX相关事件
     */
    public boolean isDexEvent() {
        return dexSwapData != null || lpMintData != null || lpBurnData != null;
    }
    /**
     * 是否为ERC20事件
     */
    public boolean isErc20Event() {
        return erc20Data != null;
    }
    
    /**
     * 是否为LP相关事件
     */
    public boolean isLPEvent() {
        return lpMintData != null || lpBurnData != null;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("ProcessEvent{");
        boolean firstField = true;

        if (eventName != null && !eventName.isEmpty()) {
            if (!firstField) sb.append(", ");
            sb.append("eventName='").append(eventName).append('\'');
            firstField = false;
        }
        if (contractAddress != null && !contractAddress.isEmpty()) {
            if (!firstField) sb.append(", ");
            sb.append("contractAddress='").append(contractAddress).append('\'');
            firstField = false;
        }
        if (transactionHash != null && !transactionHash.isEmpty()) {
            if (!firstField) sb.append(", ");
            sb.append("transactionHash='").append(transactionHash).append('\'');
            firstField = false;
        }
        if (blockId != null) {
            if (!firstField) sb.append(", ");
            sb.append("blockId=").append(blockId);
            firstField = false;
        }
        if (fromAddress != null && !fromAddress.isEmpty()) {
            if (!firstField) sb.append(", ");
            sb.append("fromAddress='").append(fromAddress).append('\'');
            firstField = false;
        }
        if (timestamp != null) {
            if (!firstField) sb.append(", ");
            sb.append("timestamp=").append(timestamp);
            firstField = false;
        }
        if (contractType != null && !contractType.isEmpty()) {
            if (!firstField) sb.append(", ");
            sb.append("contractType='").append(contractType).append('\'');
            firstField = false;
        }
        if (bizId != null) {
            if (!firstField) sb.append(", ");
            sb.append("bizId=").append(bizId);
            firstField = false;
        }
        if (erc20Data != null) {
            if (!firstField) sb.append(", ");
            sb.append("erc20Data=").append(erc20Data);
            firstField = false;
        }
        if (dexSwapData != null) {
            if (!firstField) sb.append(", ");
            sb.append("dexSwapData=").append(dexSwapData);
            firstField = false;
        }
        if (lpMintData != null) {
            if (!firstField) sb.append(", ");
            sb.append("lpMintData=").append(lpMintData);
            firstField = false;
        }
        if (lpBurnData != null) {
            if (!firstField) sb.append(", ");
            sb.append("lpBurnData=").append(lpBurnData);
            firstField = false;
        }
        if (accountMetadata != null) {
            if (!firstField) sb.append(", ");
            sb.append("accountMetadata=").append(accountMetadata);
            firstField = false;
        }
        if (tokenMetadata != null) {
            if (!firstField) sb.append(", ");
            sb.append("tokenMetadata=").append(tokenMetadata);
            firstField = false;
        }
        if (pairMetadata != null) {
            if (!firstField) sb.append(", ");
            sb.append("pairMetadata=").append(pairMetadata);
            firstField = false;
        }

        sb.append('}');
        return sb.toString();
    }
}
