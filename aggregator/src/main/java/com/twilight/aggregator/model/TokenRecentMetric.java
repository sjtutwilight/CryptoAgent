package com.twilight.aggregator.model;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class TokenRecentMetric implements Serializable {
    private static final long serialVersionUID = 1L;

    private Long tokenId;
    private String timeWindow;
    private long endTime;
    private String tag; // 指标类型 tag: all, cex, dex, smart_money, fresh_wallet
    private double volumeUsd = 0.0;
    private Integer txCnt = 0;
    private double tokenPriceUsd = 0.0;
    private double buyPressureUsd = 0.0;
    private double buyVolumeUsd = 0.0;
    private double sellVolumeUsd = 0.0;
    private Integer buyCount = 0;
    private Integer sellCount = 0;
    
    // 新增的Token指标字段
    private Double mcapUsd = 0.0;
    private Double fdvUsd = 0.0;
    private Double liquidityUsd = 0.0;

    public TokenRecentMetric(Long tokenId, String timeWindow, long endTimeMillis, String tag) {
        this.tokenId = tokenId;
        this.timeWindow = timeWindow;
        this.endTime = endTimeMillis;
        this.tag = tag;
    }

}