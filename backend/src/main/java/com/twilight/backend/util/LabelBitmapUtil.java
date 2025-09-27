package com.twilight.backend.util;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 标签位图工具类
 * 用于解析账户标签位图
 */
@Component
public class LabelBitmapUtil {

    /**
     * 标签类型枚举
     * 位图定义：
     * 1<<0 EX（Exchange）
     * 1<<1 SM（SmartMoney）
     * 1<<2 WH（Whale）
     * 1<<3 PF（PublicFigure）
     * 1<<4 FR（Fresh）
     * 1<<5 TP（TopPnL）
     */
    @Getter
    @AllArgsConstructor
    public enum LabelType {
        EXCHANGE(0, "交易所", "EX"),
        SMART_MONEY(1, "聪明钱", "SM"),
        WHALE(2, "巨鲸", "WH"),
        PUBLIC_FIGURE(3, "公众人物", "PF"),
        FRESH_WALLET(4, "新钱包", "FR"),
        TOP_PNL(5, "Top PnL", "TP");

        private final int bit;
        private final String description;
        private final String code;
    }

    /**
     * 解析位图为标签列表
     * 
     * @param labelMask 标签位图
     * @return 标签描述列表
     */
    public List<String> parseLabels(Integer labelMask) {
        if (labelMask == null || labelMask == 0) {
            return Collections.emptyList();
        }

        List<String> labels = new ArrayList<>();
        for (LabelType type : LabelType.values()) {
            if (hasLabel(labelMask, type)) {
                labels.add(type.getDescription());
            }
        }
        return labels;
    }

    /**
     * 解析位图为标签代码列表
     * 
     * @param labelMask 标签位图
     * @return 标签代码列表
     */
    public List<String> parseLabelCodes(Integer labelMask) {
        if (labelMask == null || labelMask == 0) {
            return Collections.emptyList();
        }

        List<String> codes = new ArrayList<>();
        for (LabelType type : LabelType.values()) {
            if (hasLabel(labelMask, type)) {
                codes.add(type.getCode());
            }
        }
        return codes;
    }

    /**
     * 检查是否包含特定标签
     * 
     * @param labelMask 标签位图
     * @param labelType 标签类型
     * @return 是否包含该标签
     */
    public boolean hasLabel(Integer labelMask, LabelType labelType) {
        if (labelMask == null) {
            return false;
        }
        return (labelMask & (1 << labelType.getBit())) != 0;
    }

    /**
     * 根据标签代码获取位图
     * 
     * @param tagCode 标签代码
     * @return 对应的位图值
     */
    public Integer getLabelMask(String tagCode) {
        if (tagCode == null || tagCode.isEmpty()) {
            return 0;
        }

        for (LabelType type : LabelType.values()) {
            if (type.getCode().equalsIgnoreCase(tagCode)) {
                return 1 << type.getBit();
            }
        }
        return 0;
    }

    /**
     * 合并多个标签为位图
     * 
     * @param labelTypes 标签类型数组
     * @return 合并后的位图
     */
    public Integer combineLabels(LabelType... labelTypes) {
        int result = 0;
        for (LabelType type : labelTypes) {
            result |= (1 << type.getBit());
        }
        return result;
    }

    /**
     * 获取所有可用的标签描述
     * 
     * @return 所有标签描述列表
     */
    public List<String> getAllLabelDescriptions() {
        List<String> descriptions = new ArrayList<>();
        for (LabelType type : LabelType.values()) {
            descriptions.add(type.getDescription());
        }
        return descriptions;
    }
}


