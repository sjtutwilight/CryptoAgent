package com.twilight.backend.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 代币基础信息模型
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TokenInfo {
    /**
     * 代币ID
     */
    private Long tokenId;

    /**
     * 链名称
     */
    private String chainName;

    /**
     * 代币符号
     */
    private String symbol;

    /**
     * 代币名称（使用symbol）
     */
    private String name;

    /**
     * 代币年龄（天数，随机值）
     */
    private Integer age;

    /**
     * 代币类别
     */
    private String tokenCategory;

    /**
     * 安全评分（随机值）
     */
    private Integer securityScore;

    /**
     * 发行商
     */
    private String issuer;

    /**
     * 代币合约地址
     */
    private String address;

    /**
     * 代币精度
     */
    private Integer decimals;

    /**
     * 介绍信息（随机生成）
     */
    private String description;
}
