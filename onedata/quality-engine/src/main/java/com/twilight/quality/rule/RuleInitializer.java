package com.twilight.quality.rule;

import com.twilight.quality.aggregator.WindowManager;
import com.twilight.quality.domain.rule.RuleResult;
import com.twilight.quality.rule.aggregate.FreshnessRule;
import com.twilight.quality.rule.aggregate.ThroughputRule;
import com.twilight.quality.rule.realtime.CompletenessRule;
import com.twilight.quality.rule.realtime.RangeCheckRule;
import com.twilight.quality.rule.realtime.SchemaValidationRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.function.Consumer;

/**
 * 规则初始化器
 * 负责注册所有质量规则
 */
@Component
public class RuleInitializer {
    
    private static final Logger log = LoggerFactory.getLogger(RuleInitializer.class);
    
    private final RuleRegistry ruleRegistry;
    private final WindowManager windowManager;
    private final Consumer<RuleResult> resultHandler;
    
    public RuleInitializer(RuleRegistry ruleRegistry, 
                           WindowManager windowManager,
                           RuleResultHandler resultHandler) {
        this.ruleRegistry = ruleRegistry;
        this.windowManager = windowManager;
        this.resultHandler = resultHandler::handleResult;
    }
    
    @PostConstruct
    public void init() {
        log.info("开始初始化质量规则...");
        
        // 注册实时规则
        registerRealtimeRules();
        
        // 注册聚合规则
        registerAggregateRules();
        
        // 设置窗口管理器的结果回调
        windowManager.setResultCallback(resultHandler);
        
        log.info("规则初始化完成，统计: {}", ruleRegistry.getStats());
    }
    
    /**
     * 注册实时规则
     */
    private void registerRealtimeRules() {
        // 完整性规则
        ruleRegistry.register(new CompletenessRule.DexCompletenessRule());
        ruleRegistry.register(new CompletenessRule.KlineCompletenessRule());
        ruleRegistry.register(new CompletenessRule.OrderbookCompletenessRule());
        ruleRegistry.register(new CompletenessRule.TradesCompletenessRule());
        
        // 数值范围规则
        ruleRegistry.register(new RangeCheckRule.DexAmountRangeRule());
        ruleRegistry.register(new RangeCheckRule.KlinePriceRangeRule());
        ruleRegistry.register(new RangeCheckRule.OrderbookPriceRangeRule());
        
        // Schema校验规则
        ruleRegistry.register(new SchemaValidationRule.DexSchemaRule());
        ruleRegistry.register(new SchemaValidationRule.KlineSchemaRule());
        ruleRegistry.register(new SchemaValidationRule.OrderbookSchemaRule());
        
        log.info("已注册实时规则");
    }
    
    /**
     * 注册聚合规则
     */
    private void registerAggregateRules() {
        // 时效性规则
        ruleRegistry.register(new FreshnessRule.DexFreshnessRule());
        ruleRegistry.register(new FreshnessRule.KlineFreshnessRule());
        ruleRegistry.register(new FreshnessRule.OrderbookFreshnessRule());
        
        // 吞吐量规则
        ruleRegistry.register(new ThroughputRule.DexThroughputRule());
        ruleRegistry.register(new ThroughputRule.KlineThroughputRule());
        ruleRegistry.register(new ThroughputRule.OrderbookThroughputRule());
        
        log.info("已注册聚合规则");
    }
}

