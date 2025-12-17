package com.twilight.quality.domain.enums;

/**
 * 数据质量维度枚举
 * 定义数据质量检测的五个核心维度
 */
public enum QualityDimension {
    
    /**
     * 完整性：检测必填字段是否缺失
     */
    COMPLETENESS("completeness", "完整性"),
    
    /**
     * 时效性：检测数据延迟、断流等时间相关问题
     */
    TIMELINESS("timeliness", "时效性"),
    
    /**
     * 准确性：检测数值范围、格式等准确性问题
     */
    ACCURACY("accuracy", "准确性"),
    
    /**
     * 一致性：检测跨源对比、时序连续性等问题
     */
    CONSISTENCY("consistency", "一致性"),
    
    /**
     * 模式合规：检测字段类型变更、结构异常等问题
     */
    SCHEMA("schema", "模式合规");
    
    private final String code;
    private final String description;
    
    QualityDimension(String code, String description) {
        this.code = code;
        this.description = description;
    }
    
    public String getCode() {
        return code;
    }
    
    public String getDescription() {
        return description;
    }
}

