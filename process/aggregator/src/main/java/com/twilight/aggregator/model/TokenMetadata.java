package com.twilight.aggregator.model;

import lombok.Data;
import java.io.Serializable;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Token元数据模型，对应Redis中的tokenMetadata
 */
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class TokenMetadata implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private Long id;           // tokenId
    private String address;      // token合约地址
    private String symbol;       // token符号，如USDC
    private String name;         // token名称，如USDC
    private Integer decimals;     // token精度，通常为18
    private String chainId;      // 链ID
    private String chainName;    // 链名称
    private TokenMetrics tokenMetrics;
    /**
     * 获取token地址（小写）
     */
    public String getTokenAddress() {
        return address != null ? address.toLowerCase() : null;
    }
    
    /**
     * 检查tokenMetadata是否有效
     */
    public boolean isValid() {
        return id != null && address != null && symbol != null;
    }
}
