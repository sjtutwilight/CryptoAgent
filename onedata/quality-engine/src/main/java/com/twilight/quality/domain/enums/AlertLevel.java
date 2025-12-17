package com.twilight.quality.domain.enums;

/**
 * 告警级别枚举
 */
public enum AlertLevel {
    
    /**
     * 信息级别：观察性告警，仅记录到存储
     */
    INFO(1, "info", "信息"),
    
    /**
     * 警告级别：需要关注，发送Kafka事件
     */
    WARNING(2, "warning", "警告"),
    
    /**
     * 严重级别：严重异常，Kafka + Webhook通知
     */
    CRITICAL(3, "critical", "严重");
    
    private final int priority;
    private final String code;
    private final String description;
    
    AlertLevel(int priority, String code, String description) {
        this.priority = priority;
        this.code = code;
        this.description = description;
    }
    
    public int getPriority() {
        return priority;
    }
    
    public String getCode() {
        return code;
    }
    
    public String getDescription() {
        return description;
    }
    
    /**
     * 判断当前级别是否大于等于指定级别
     */
    public boolean isAtLeast(AlertLevel other) {
        return this.priority >= other.priority;
    }
    
    /**
     * 从字符串解析告警级别
     */
    public static AlertLevel fromString(String value) {
        if (value == null) {
            return INFO;
        }
        for (AlertLevel level : values()) {
            if (level.code.equalsIgnoreCase(value) || level.name().equalsIgnoreCase(value)) {
                return level;
            }
        }
        return INFO;
    }
}

