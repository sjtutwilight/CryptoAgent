package com.twilight.quality.rule.base;

import com.fasterxml.jackson.databind.JsonNode;
import com.twilight.quality.domain.enums.DataDomain;
import com.twilight.quality.domain.enums.QualityDimension;
import com.twilight.quality.domain.rule.RuleContext;
import com.twilight.quality.domain.rule.RuleResult;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * 质量规则接口
 * 所有质量检测规则的基础接口
 * 
 * @param <T> 消息类型（通常为JsonNode）
 */
public interface QualityRule<T> {
    
    /**
     * 获取规则名称
     */
    String getRuleName();
    
    /**
     * 获取规则描述
     */
    default String getDescription() {
        return getRuleName();
    }
    
    /**
     * 获取支持的业务域
     */
    Set<DataDomain> getSupportedDomains();
    
    /**
     * 获取质量维度
     */
    QualityDimension getDimension();
    
    /**
     * 检测消息是否支持该规则
     */
    default boolean supports(DataDomain domain) {
        return getSupportedDomains().contains(domain);
    }
    
    /**
     * 执行规则检测
     * 
     * @param message 消息内容
     * @param context 规则上下文
     * @return 检测结果，如果规则不适用返回empty
     */
    Optional<RuleResult> evaluate(T message, RuleContext context);
    
    /**
     * 是否为聚合规则（需要窗口聚合）
     */
    default boolean isAggregateRule() {
        return false;
    }
    
    /**
     * 规则是否启用
     */
    default boolean isEnabled() {
        return true;
    }
    
    /**
     * 初始化规则配置
     */
    default void configure(Map<String, Object> config) {
        // 默认空实现
    }
}

