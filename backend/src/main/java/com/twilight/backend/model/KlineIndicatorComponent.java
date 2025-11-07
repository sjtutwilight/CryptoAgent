package com.twilight.backend.model;

import lombok.Data;

/**
 * 指标输出的单个组件，如 MACD 的 DIF/DEA
 */
@Data
public class KlineIndicatorComponent {
    private String name;
    private Double value;
}
