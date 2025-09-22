package com.twilight.backend.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * 账户基础信息模型
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class AccountInfo {
    /**
     * 账户ID
     */
    private Long accountId;

    /**
     * 链名称
     */
    private String chainName;

    /**
     * 账户地址
     */
    private String address;

    /**
     * 实体名称
     */
    private String entity;

    /**
     * 账户标签列表
     */
    private List<String> labels;

    /**
     * 标签位图
     */
    private Integer tagBitmap;
}
