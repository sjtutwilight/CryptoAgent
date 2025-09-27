package com.twilight.aggregator.model;

import java.io.Serializable;
import java.util.Map;
import java.util.HashMap;

import lombok.Data;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class PairMetadata implements Serializable {
    private static final long serialVersionUID = 1L;
    private Long pairId;       // 与Redis字段pairId匹配
    private String pairAddress; // 与Redis字段pairAddress匹配
    private String pairName;
    private String chainId;
    private String chainName;

    private TokenMetadata token0;
    private TokenMetadata token1;
    
}