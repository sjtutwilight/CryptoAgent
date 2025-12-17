package com.twilight.quality.rule.base;

import com.fasterxml.jackson.databind.JsonNode;
import com.twilight.quality.domain.enums.DataDomain;
import com.twilight.quality.domain.enums.QualityDimension;
import com.twilight.quality.domain.rule.RuleContext;
import com.twilight.quality.domain.rule.RuleResult;
import com.twilight.quality.aggregator.WindowState;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;

/**
 * 聚合规则基类
 * 用于需要窗口聚合后再评估的规则（如时效性、吞吐量检测）
 */
public abstract class AggregateRule extends BaseRule {
    
    /**
     * 默认窗口大小：1分钟
     */
    protected long windowSizeMs = 60_000L;
    
    protected AggregateRule(String ruleName, QualityDimension dimension, DataDomain... domains) {
        super(ruleName, dimension, domains);
    }
    
    @Override
    public boolean isAggregateRule() {
        return true;
    }
    
    /**
     * 获取窗口大小（毫秒）
     */
    public long getWindowSizeMs() {
        return windowSizeMs;
    }
    
    /**
     * 实时检测（聚合规则通常返回empty，在窗口结束时评估）
     */
    @Override
    public Optional<RuleResult> evaluate(JsonNode message, RuleContext context) {
        // 聚合规则的实时评估返回空，实际评估在窗口结束时进行
        return Optional.empty();
    }
    
    /**
     * 累加消息到窗口状态
     * 
     * @param message 消息内容
     * @param context 规则上下文
     * @param state 窗口状态
     */
    public abstract void accumulate(JsonNode message, RuleContext context, WindowState state);
    
    /**
     * 窗口结束时评估
     * 
     * @param state 窗口状态
     * @return 检测结果
     */
    public abstract Optional<RuleResult> evaluateWindow(WindowState state);
    
    @Override
    protected void parseConfig(Map<String, Object> config) {
        super.parseConfig(config);
        if (config.containsKey("window_ms")) {
            this.windowSizeMs = Long.parseLong(config.get("window_ms").toString());
        }
    }
    
    /**
     * 创建窗口结果
     */
    protected RuleResult createWindowResult(WindowState state, boolean passed, 
                                             String message, Double metricValue, Double threshold) {
        return RuleResult.builder()
                .ruleName(ruleName)
                .domain(state.getDomain())
                .dimension(dimension)
                .streamKey(state.getStreamKey())
                .passed(passed)
                .alertLevel(passed ? null : defaultAlertLevel)
                .message(message)
                .metricValue(metricValue)
                .threshold(threshold)
                .windowStart(state.getWindowStart())
                .windowEnd(state.getWindowEnd())
                .messageCount(state.getMessageCount())
                .evaluatedAt(Instant.now())
                .build();
    }
}

