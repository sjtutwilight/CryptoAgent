package com.twilight.quality.rule;

import com.fasterxml.jackson.databind.JsonNode;
import com.twilight.quality.aggregator.WindowManager;
import com.twilight.quality.aggregator.WindowState;
import com.twilight.quality.domain.enums.DataDomain;
import com.twilight.quality.domain.rule.RuleContext;
import com.twilight.quality.domain.rule.RuleResult;
import com.twilight.quality.rule.base.AggregateRule;
import com.twilight.quality.rule.base.QualityRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * 规则引擎
 * 负责执行质量规则检测
 */
@Component
public class RuleEngine {
    
    private static final Logger log = LoggerFactory.getLogger(RuleEngine.class);
    
    private final RuleRegistry ruleRegistry;
    private final WindowManager windowManager;
    
    public RuleEngine(RuleRegistry ruleRegistry, WindowManager windowManager) {
        this.ruleRegistry = ruleRegistry;
        this.windowManager = windowManager;
    }
    
    /**
     * 执行实时规则检测
     * 
     * @param message 消息内容
     * @param context 规则上下文
     * @return 检测结果列表
     */
    @SuppressWarnings("unchecked")
    public List<RuleResult> evaluateRealtime(JsonNode message, RuleContext context) {
        List<RuleResult> results = new ArrayList<>();
        DataDomain domain = context.getDomain();
        
        // 获取该域的所有实时规则
        List<QualityRule<?>> rules = ruleRegistry.getRealtimeRulesForDomain(domain);
        
        for (QualityRule<?> rule : rules) {
            try {
                QualityRule<JsonNode> jsonRule = (QualityRule<JsonNode>) rule;
                Optional<RuleResult> result = jsonRule.evaluate(message, context);
                result.ifPresent(results::add);
            } catch (Exception e) {
                log.error("规则 {} 执行异常: {}", rule.getRuleName(), e.getMessage(), e);
            }
        }
        
        return results;
    }
    
    /**
     * 累加消息到聚合规则窗口
     * 
     * @param message 消息内容
     * @param context 规则上下文
     */
    public void accumulateToWindow(JsonNode message, RuleContext context) {
        DataDomain domain = context.getDomain();
        
        // 获取该域的所有聚合规则
        List<AggregateRule> aggregateRules = ruleRegistry.getAggregateRulesForDomain(domain);
        
        for (AggregateRule rule : aggregateRules) {
            try {
                // 获取或创建窗口状态
                WindowState state = windowManager.getOrCreateWindow(
                        domain, 
                        context.getStreamKey(), 
                        rule.getRuleName(),
                        rule.getWindowSizeMs()
                );
                
                // 累加消息
                rule.accumulate(message, context, state);
                
            } catch (Exception e) {
                log.error("聚合规则 {} 累加异常: {}", rule.getRuleName(), e.getMessage(), e);
            }
        }
    }
    
    /**
     * 评估指定的窗口状态
     * 
     * @param state 窗口状态
     * @return 检测结果
     */
    public Optional<RuleResult> evaluateWindow(WindowState state) {
        Optional<QualityRule<?>> ruleOpt = ruleRegistry.getRule(state.getRuleName());
        
        if (ruleOpt.isEmpty()) {
            log.warn("未找到规则: {}", state.getRuleName());
            return Optional.empty();
        }
        
        QualityRule<?> rule = ruleOpt.get();
        if (!(rule instanceof AggregateRule)) {
            log.warn("规则 {} 不是聚合规则", state.getRuleName());
            return Optional.empty();
        }
        
        try {
            AggregateRule aggregateRule = (AggregateRule) rule;
            return aggregateRule.evaluateWindow(state);
        } catch (Exception e) {
            log.error("窗口评估异常 规则={} 窗口={}: {}", 
                    state.getRuleName(), state.getWindowKey(), e.getMessage(), e);
            return Optional.empty();
        }
    }
    
    /**
     * 处理消息（实时检测 + 窗口累加）
     * 
     * @param message 消息内容
     * @param context 规则上下文
     * @return 实时检测结果
     */
    public List<RuleResult> processMessage(JsonNode message, RuleContext context) {
        // 1. 执行实时规则
        List<RuleResult> results = evaluateRealtime(message, context);
        
        // 2. 累加到聚合窗口
        accumulateToWindow(message, context);
        
        return results;
    }
    
    /**
     * 获取规则注册中心
     */
    public RuleRegistry getRegistry() {
        return ruleRegistry;
    }
}

