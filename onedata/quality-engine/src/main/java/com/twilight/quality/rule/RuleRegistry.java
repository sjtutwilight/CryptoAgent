package com.twilight.quality.rule;

import com.twilight.quality.domain.enums.DataDomain;
import com.twilight.quality.rule.base.AggregateRule;
import com.twilight.quality.rule.base.QualityRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * 规则注册中心
 * 管理所有质量检测规则的注册和查询
 */
@Component
public class RuleRegistry {
    
    private static final Logger log = LoggerFactory.getLogger(RuleRegistry.class);
    
    /**
     * 按业务域分组的规则映射
     */
    private final Map<DataDomain, List<QualityRule<?>>> rulesByDomain = new ConcurrentHashMap<>();
    
    /**
     * 按规则名称索引
     */
    private final Map<String, QualityRule<?>> rulesByName = new ConcurrentHashMap<>();
    
    /**
     * 所有规则列表
     */
    private final List<QualityRule<?>> allRules = new ArrayList<>();
    
    /**
     * 注册单个规则
     */
    public void register(QualityRule<?> rule) {
        if (rule == null) {
            return;
        }
        
        String ruleName = rule.getRuleName();
        if (rulesByName.containsKey(ruleName)) {
            log.warn("规则 {} 已存在，将被覆盖", ruleName);
        }
        
        rulesByName.put(ruleName, rule);
        allRules.add(rule);
        
        // 按业务域分组
        for (DataDomain domain : rule.getSupportedDomains()) {
            rulesByDomain.computeIfAbsent(domain, k -> new ArrayList<>()).add(rule);
        }
        
        log.info("注册规则: {} [{}] 支持域: {}", 
                ruleName, rule.getDimension(), rule.getSupportedDomains());
    }
    
    /**
     * 批量注册规则
     */
    public void register(QualityRule<?>... rules) {
        for (QualityRule<?> rule : rules) {
            register(rule);
        }
    }
    
    /**
     * 获取指定业务域的所有规则
     */
    public List<QualityRule<?>> getRulesForDomain(DataDomain domain) {
        return rulesByDomain.getOrDefault(domain, Collections.emptyList())
                .stream()
                .filter(QualityRule::isEnabled)
                .collect(Collectors.toList());
    }
    
    /**
     * 获取指定业务域的实时规则（非聚合规则）
     */
    public List<QualityRule<?>> getRealtimeRulesForDomain(DataDomain domain) {
        return getRulesForDomain(domain).stream()
                .filter(r -> !r.isAggregateRule())
                .collect(Collectors.toList());
    }
    
    /**
     * 获取指定业务域的聚合规则
     */
    public List<AggregateRule> getAggregateRulesForDomain(DataDomain domain) {
        return getRulesForDomain(domain).stream()
                .filter(QualityRule::isAggregateRule)
                .map(r -> (AggregateRule) r)
                .collect(Collectors.toList());
    }
    
    /**
     * 根据规则名称获取规则
     */
    public Optional<QualityRule<?>> getRule(String ruleName) {
        return Optional.ofNullable(rulesByName.get(ruleName));
    }
    
    /**
     * 获取所有规则
     */
    public List<QualityRule<?>> getAllRules() {
        return Collections.unmodifiableList(allRules);
    }
    
    /**
     * 获取所有启用的规则
     */
    public List<QualityRule<?>> getEnabledRules() {
        return allRules.stream()
                .filter(QualityRule::isEnabled)
                .collect(Collectors.toList());
    }
    
    /**
     * 配置规则
     */
    public void configureRule(String ruleName, Map<String, Object> config) {
        QualityRule<?> rule = rulesByName.get(ruleName);
        if (rule != null) {
            rule.configure(config);
            log.info("配置规则: {} 配置项: {}", ruleName, config.keySet());
        } else {
            log.warn("规则 {} 不存在，无法配置", ruleName);
        }
    }
    
    /**
     * 获取规则统计信息
     */
    public Map<String, Object> getStats() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("totalRules", allRules.size());
        stats.put("enabledRules", getEnabledRules().size());
        stats.put("realtimeRules", allRules.stream().filter(r -> !r.isAggregateRule()).count());
        stats.put("aggregateRules", allRules.stream().filter(QualityRule::isAggregateRule).count());
        
        Map<String, Integer> byDomain = new HashMap<>();
        for (DataDomain domain : DataDomain.values()) {
            int count = getRulesForDomain(domain).size();
            if (count > 0) {
                byDomain.put(domain.getDomainId(), count);
            }
        }
        stats.put("rulesByDomain", byDomain);
        
        return stats;
    }
}

