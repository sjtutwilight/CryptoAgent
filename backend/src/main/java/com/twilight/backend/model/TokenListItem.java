package com.twilight.backend.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * 代币列表项模型 - 用于前端TokenList显示
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TokenListItem {
    /**
     * 代币ID
     */
    private Long tokenId;

    /**
     * 代币符号
     */
    private String symbol;

    /**
     * 代币名称
     */
    private String name;

    /**
     * 链名称
     */
    private String chainName;

    /**
     * 当前价格 (USD)
     */
    private String price;

    /**
     * 1分钟涨跌幅 (%)
     */
    private String change1m;

    /**
     * 市值 (USD)
     */
    private String mcap;

    /**
     * DEX交易量 (USD)
     */
    private String dexVolume;

    /**
     * 流动性 (USD)
     */
    private String liquidity;

    /**
     * DEX买入量 (USD)
     */
    private String dexBuys;

    /**
     * DEX卖出量 (USD)
     */
    private String dexSells;

    /**
     * 买入压力比例 (0-1)
     */
    private Double buyPressure;
}






