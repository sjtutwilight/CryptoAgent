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
import java.util.concurrent.ConcurrentHashMap;

/**
 * 吞吐量规则
 * 检测消息吞吐量是否正常
 */
public abstract class ThroughputRule extends AggregateRule {
    
    /**
     * 最小消息数量（低于此值告警）
     */
    protected long minMessages = 1;
    
    /**
     * 历史基线（用于检测骤降）
     * Key: streamKey, Value: 历史平均吞吐量
     */
    protected final Map<String, Double> throughputBaseline = new ConcurrentHashMap<>();
    
    /**
     * 骤降阈值（低于基线的百分比）
     */
    protected double dropThresholdPct = 0.5; // 50%
    
    protected ThroughputRule(String ruleName, DataDomain... domains) {
        super(ruleName, QualityDimension.TIMELINESS, domains);
        this.defaultAlertLevel = AlertLevel.WARNING;
    }
    
    @Override
    public void accumulate(JsonNode message, RuleContext context, WindowState state) {
        state.incrementCount();
        state.setLastMessageTime(System.currentTimeMillis());
    }
    
    @Override
    public Optional<RuleResult> evaluateWindow(WindowState state) {
        long messageCount = state.getMessageCount();
        String streamKey = state.getStreamKey();
        
        Map<String, Object> ctx = new HashMap<>();
        ctx.put("message_count", messageCount);
        ctx.put("min_messages", minMessages);
        ctx.put("window_ms", windowSizeMs);
        
        // 计算吞吐量（消息/分钟）
        double throughputPerMin = messageCount * 60000.0 / windowSizeMs;
        ctx.put("throughput_per_min", throughputPerMin);
        
        // 获取历史基线
        Double baseline = throughputBaseline.get(streamKey);
        if (baseline != null) {
            ctx.put("baseline_throughput", baseline);
        }
        
        // 判断告警条件
        AlertLevel level = null;
        String message = null;
        
        // 1. 检查是否低于最小阈值
        if (messageCount < minMessages) {
            level = AlertLevel.WARNING;
            message = String.format("吞吐量过低: 窗口内消息数=%d (最小要求=%d)", 
                    messageCount, minMessages);
        }
        
        // 2. 检查是否相比基线骤降
        if (level == null && baseline != null && throughputPerMin < baseline * dropThresholdPct) {
            level = AlertLevel.WARNING;
            message = String.format("吞吐量骤降: 当前=%.1f/min, 基线=%.1f/min (降幅=%.1f%%)",
                    throughputPerMin, baseline, (1 - throughputPerMin / baseline) * 100);
            ctx.put("drop_pct", (1 - throughputPerMin / baseline) * 100);
        }
        
        // 3. 更新基线（使用指数移动平均）
        updateBaseline(streamKey, throughputPerMin);
        
        if (level != null) {
            RuleResult result = createWindowResult(state, false, message, 
                    (double) messageCount, (double) minMessages);
            result.setAlertLevel(level);
            result.setContext(ctx);
            return Optional.of(result);
        }
        
        return Optional.of(createWindowResult(state, true,
                String.format("吞吐量正常: %d条/窗口, %.1f条/分钟", messageCount, throughputPerMin),
                (double) messageCount, (double) minMessages));
    }
    
    /**
     * 更新吞吐量基线（指数移动平均）
     */
    protected void updateBaseline(String streamKey, double currentThroughput) {
        double alpha = 0.1; // 平滑因子
        throughputBaseline.compute(streamKey, (k, v) -> {
            if (v == null) {
                return currentThroughput;
            }
            return v * (1 - alpha) + currentThroughput * alpha;
        });
    }
    
    @Override
    protected void parseConfig(Map<String, Object> config) {
        super.parseConfig(config);
        if (config.containsKey("min_messages")) {
            this.minMessages = Long.parseLong(config.get("min_messages").toString());
        }
        if (config.containsKey("drop_threshold_pct")) {
            this.dropThresholdPct = Double.parseDouble(config.get("drop_threshold_pct").toString());
        }
    }
    
    // ===== 具体规则实现 =====
    
    /**
     * DEX吞吐量规则
     */
    public static class DexThroughputRule extends ThroughputRule {
        
        public DexThroughputRule() {
            super("dex.timeliness.throughput", 
                    DataDomain.DEX_UNISWAP, DataDomain.DEX_HYPERLIQUID);
            
            // DEX交易量波动较大，阈值设置较低
            this.minMessages = 1;
            this.windowSizeMs = 60000;
            this.dropThresholdPct = 0.3; // 70%降幅才告警
        }
    }
    
    /**
     * K线吞吐量规则
     */
    public static class KlineThroughputRule extends ThroughputRule {
        
        public KlineThroughputRule() {
            super("kline.timeliness.throughput", DataDomain.CEX_KLINE);
            
            // K线数据应该稳定
            this.minMessages = 10;
            this.windowSizeMs = 60000;
            this.dropThresholdPct = 0.5;
        }
    }
    
    /**
     * 订单簿吞吐量规则
     */
    public static class OrderbookThroughputRule extends ThroughputRule {
        
        public OrderbookThroughputRule() {
            super("perp.orderbook.throughput", DataDomain.CEX_PERP_ORDERBOOK);
            
            // 订单簿更新频繁
            this.minMessages = 100;
            this.windowSizeMs = 60000;
            this.dropThresholdPct = 0.5;
        }
    }
}

