package com.twilight.quality.rule.aggregate;

import com.fasterxml.jackson.databind.JsonNode;
import com.twilight.quality.aggregator.WindowState;
import com.twilight.quality.domain.enums.AlertLevel;
import com.twilight.quality.domain.enums.DataDomain;
import com.twilight.quality.domain.enums.QualityDimension;
import com.twilight.quality.domain.rule.RuleContext;
import com.twilight.quality.domain.rule.RuleResult;
import com.twilight.quality.rule.base.AggregateRule;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * 时效性规则
 * 检测数据延迟（event_time vs process_time）
 */
public abstract class FreshnessRule extends AggregateRule {
    
    /**
     * 最大允许延迟（毫秒）- WARNING级别
     */
    protected long maxDelayMs = 5000;
    
    /**
     * 严重延迟阈值（毫秒）- CRITICAL级别
     */
    protected long criticalDelayMs = 30000;
    
    protected FreshnessRule(String ruleName, DataDomain... domains) {
        super(ruleName, QualityDimension.TIMELINESS, domains);
        this.defaultAlertLevel = AlertLevel.WARNING;
    }
    
    @Override
    public void accumulate(JsonNode message, RuleContext context, WindowState state) {
        state.incrementCount();
        state.setLastMessageTime(System.currentTimeMillis());
        
        // 计算延迟
        Long eventTime = context.getEventTime();
        if (eventTime != null) {
            long processTime = context.getReceiveTime().toEpochMilli();
            long delay = processTime - eventTime;
            
            // 排除异常值（负延迟或超大延迟可能是时钟问题）
            if (delay >= 0 && delay < 3600_000) { // 1小时内
                state.updateDelay(delay);
            }
        }
    }
    
    @Override
    public Optional<RuleResult> evaluateWindow(WindowState state) {
        long messageCount = state.getMessageCount();
        
        // 如果窗口内没有消息，跳过评估
        if (messageCount == 0) {
            return Optional.empty();
        }
        
        double avgDelay = state.getAvgDelayMs();
        Long maxDelay = state.getMaxDelayMs();
        
        Map<String, Object> ctx = new HashMap<>();
        ctx.put("message_count", messageCount);
        ctx.put("avg_delay_ms", avgDelay);
        ctx.put("max_delay_ms", maxDelay);
        ctx.put("max_allowed_ms", maxDelayMs);
        ctx.put("critical_threshold_ms", criticalDelayMs);
        
        // 判断告警级别
        AlertLevel level = null;
        String message = null;
        
        if (maxDelay != null && maxDelay > criticalDelayMs) {
            level = AlertLevel.CRITICAL;
            message = String.format("数据严重延迟: 最大延迟=%dms (阈值=%dms), 平均延迟=%.0fms",
                    maxDelay, criticalDelayMs, avgDelay);
        } else if (avgDelay > maxDelayMs) {
            level = AlertLevel.WARNING;
            message = String.format("数据延迟告警: 平均延迟=%.0fms (阈值=%dms), 最大延迟=%dms",
                    avgDelay, maxDelayMs, maxDelay);
        }
        
        if (level != null) {
            RuleResult result = createWindowResult(state, false, message, avgDelay, (double) maxDelayMs);
            result.setAlertLevel(level);
            result.setContext(ctx);
            return Optional.of(result);
        }
        
        return Optional.of(createWindowResult(state, true, 
                String.format("时效性正常: 平均延迟=%.0fms", avgDelay), avgDelay, (double) maxDelayMs));
    }
    
    @Override
    protected void parseConfig(Map<String, Object> config) {
        super.parseConfig(config);
        if (config.containsKey("max_delay_ms")) {
            this.maxDelayMs = Long.parseLong(config.get("max_delay_ms").toString());
        }
        if (config.containsKey("critical_delay_ms")) {
            this.criticalDelayMs = Long.parseLong(config.get("critical_delay_ms").toString());
        }
    }
    
    // ===== 具体规则实现 =====
    
    /**
     * DEX时效性规则
     */
    public static class DexFreshnessRule extends FreshnessRule {
        
        public DexFreshnessRule() {
            super("dex.timeliness.freshness", 
                    DataDomain.DEX_UNISWAP, DataDomain.DEX_HYPERLIQUID);
            
            // DEX数据允许较大延迟（区块确认需要时间）
            this.maxDelayMs = 30000;      // 30秒
            this.criticalDelayMs = 120000; // 2分钟
            this.windowSizeMs = 60000;     // 1分钟窗口
        }
    }
    
    /**
     * K线时效性规则
     */
    public static class KlineFreshnessRule extends FreshnessRule {
        
        public KlineFreshnessRule() {
            super("kline.timeliness.freshness", DataDomain.CEX_KLINE);
            
            // K线数据要求较高时效性
            this.maxDelayMs = 5000;       // 5秒
            this.criticalDelayMs = 30000;  // 30秒
            this.windowSizeMs = 60000;     // 1分钟窗口
        }
    }
    
    /**
     * 订单簿时效性规则
     */
    public static class OrderbookFreshnessRule extends FreshnessRule {
        
        public OrderbookFreshnessRule() {
            super("perp.orderbook.freshness", DataDomain.CEX_PERP_ORDERBOOK);
            
            // 订单簿数据要求最高时效性
            this.maxDelayMs = 1000;       // 1秒
            this.criticalDelayMs = 5000;   // 5秒
            this.windowSizeMs = 60000;     // 1分钟窗口
        }
    }
}

