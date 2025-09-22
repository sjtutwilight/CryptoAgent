package com.twilight.aggregator.model;

import lombok.Data;
import java.io.Serializable;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * AccountMetadata模型，包含地址和对应的位图标签
 */
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class AccountMetadata implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private Long id;
    private String address;
    private Integer tagBitmap;  // 新增位图标签字段
    private String tag;
    
    // 位图标签常量定义
    public static final int TAG_EXCHANGE = 1 << 0;     // 1<<0 EX（Exchange）
    public static final int TAG_SMART_MONEY = 1 << 1;  // 1<<1 SM（SmartMoney）
    public static final int TAG_WHALE = 1 << 2;        // 1<<2 WH（Whale）
    public static final int TAG_PUBLIC_FIGURE = 1 << 3; // 1<<3 PF（PublicFigure）
    public static final int TAG_FRESH = 1 << 4;        // 1<<4 FR（Fresh）
    public static final int TAG_TOP_PNL = 1 << 5;      // 1<<5 TP（TopPnL）
    
    /**
     * 检查是否包含特定标签
     * @param tagBit 标签位
     * @return 是否包含该标签
     */
    public boolean hasTag(int tagBit) {
        return tagBitmap != null && (tagBitmap & tagBit) != 0;
    }
    
    /**
     * 检查是否为CEX标签
     * @return 是否为CEX
     */
    public boolean isCex() {
        return hasTag(TAG_EXCHANGE);
    }
    
    /**
     * 检查是否为SmartMoney标签
     * @return 是否为SmartMoney
     */
    public boolean isSmartMoney() {
        return hasTag(TAG_SMART_MONEY);
    }
    
    /**
     * 检查是否为Whale标签
     * @return 是否为Whale
     */
    public boolean isWhale() {
        return hasTag(TAG_WHALE);
    }
    
    /**
     * 检查是否为Fresh标签
     * @return 是否为Fresh
     */
    public boolean isFresh() {
        return hasTag(TAG_FRESH);
    }
    
    /**
     * 根据字符串标签转换为位图值
     * @param tagString 标签字符串
     * @return 位图值
     */
    public static int tagStringToBitmap(String tagString) {
        if (tagString == null || tagString.trim().isEmpty()) {
            return 0;
        }
        
        switch (tagString.toLowerCase().trim()) {
            case "cex":
            case "exchange":
                return TAG_EXCHANGE;
            case "smart_money":
                return TAG_SMART_MONEY;
            case "whale":
            case "big_whale":
                return TAG_WHALE;
            case "fresh_wallet":
            case "fresh":
                return TAG_FRESH;
            case "public_figure":
                return TAG_PUBLIC_FIGURE;
            case "top_pnl":
                return TAG_TOP_PNL;
            case "normal":
            default:
                return 0;
        }
    }
    
    /**
     * 根据位图值转换为主要标签字符串
     * @param bitmap 位图值
     * @return 标签字符串
     */
    public static String bitmapToTagString(int bitmap) {
        if (bitmap == 0) {
            return "normal";
        }
        
        // 按优先级返回第一个匹配的标签
        if ((bitmap & TAG_EXCHANGE) != 0) return "cex";
        if ((bitmap & TAG_SMART_MONEY) != 0) return "smart_money";
        if ((bitmap & TAG_WHALE) != 0) return "whale";
        if ((bitmap & TAG_FRESH) != 0) return "fresh";
        if ((bitmap & TAG_PUBLIC_FIGURE) != 0) return "public_figure";
        if ((bitmap & TAG_TOP_PNL) != 0) return "top_pnl";
        
        return "normal";
    }
}
