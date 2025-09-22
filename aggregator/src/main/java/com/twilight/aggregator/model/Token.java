package com.twilight.aggregator.model;

import java.io.Serializable;

import lombok.Data;

@Data
public class Token implements Serializable {
    private static final long serialVersionUID = 1L;

    private String tokenAddress;
    private Long tokenId;
    private double tokenPriceUsd;
    private boolean buyOrSell;
    private double amount;
    private String fromAddress;
    private String fromAddressTag;
    private Long timestamp;
    
    // Token指标字段，用于窗口聚合时获取最新指标
    private Double mcapUsd;
    private Double fdvUsd;
    private Double liquidityUsd;
}