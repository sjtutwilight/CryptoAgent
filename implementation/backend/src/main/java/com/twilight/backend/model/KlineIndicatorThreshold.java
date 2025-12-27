package com.twilight.backend.model;

import lombok.Data;

/**
 * 指标的阈值描述，例如 RSI 超买/超卖线
 */
@Data
public class KlineIndicatorThreshold {
    private String name;
    private Double value;
}
