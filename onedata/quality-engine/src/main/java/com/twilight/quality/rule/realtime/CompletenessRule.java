package com.twilight.quality.rule.realtime;

import com.fasterxml.jackson.databind.JsonNode;
import com.twilight.quality.domain.enums.AlertLevel;
import com.twilight.quality.domain.enums.DataDomain;
import com.twilight.quality.domain.enums.QualityDimension;
import com.twilight.quality.domain.rule.RuleContext;
import com.twilight.quality.domain.rule.RuleResult;
import com.twilight.quality.rule.base.BaseRule;

import java.util.*;

/**
 * 完整性规则
 * 检测消息必填字段是否缺失
 */
public abstract class CompletenessRule extends BaseRule {
    
    protected List<String> requiredFields = new ArrayList<>();
    
    protected CompletenessRule(String ruleName, DataDomain... domains) {
        super(ruleName, QualityDimension.COMPLETENESS, domains);
        this.defaultAlertLevel = AlertLevel.CRITICAL;
    }
    
    @Override
    public Optional<RuleResult> evaluate(JsonNode message, RuleContext context) {
        List<String> missingFields = new ArrayList<>();
        
        for (String field : requiredFields) {
            if (!checkFieldExists(message, field)) {
                missingFields.add(field);
            }
        }
        
        if (missingFields.isEmpty()) {
            return Optional.of(pass(context));
        }
        
        String msg = String.format("缺失必填字段: %s", missingFields);
        Map<String, Object> ctx = new HashMap<>();
        ctx.put("missing_fields", missingFields);
        ctx.put("total_required", requiredFields.size());
        ctx.put("missing_count", missingFields.size());
        
        double completeness = 1.0 - (double) missingFields.size() / requiredFields.size();
        
        return Optional.of(fail(context, msg, completeness, 1.0, ctx));
    }
    
    /**
     * 检查字段是否存在（支持嵌套路径，如 "transaction.hash"）
     */
    protected boolean checkFieldExists(JsonNode node, String fieldPath) {
        String[] parts = fieldPath.split("\\.");
        JsonNode current = node;
        
        for (String part : parts) {
            if (current == null || current.isNull()) {
                return false;
            }
            current = current.get(part);
        }
        
        return current != null && !current.isNull();
    }
    
    @Override
    protected void parseConfig(Map<String, Object> config) {
        super.parseConfig(config);
        if (config.containsKey("required_fields")) {
            Object fields = config.get("required_fields");
            if (fields instanceof List) {
                this.requiredFields = new ArrayList<>((List<String>) fields);
            }
        }
    }
    
    // ===== 具体规则实现 =====
    
    /**
     * DEX交易完整性规则
     */
    public static class DexCompletenessRule extends CompletenessRule {
        
        public DexCompletenessRule() {
            super("dex.completeness.required_fields", 
                    DataDomain.DEX_UNISWAP, DataDomain.DEX_HYPERLIQUID);
            
            // 默认必填字段
            this.requiredFields = Arrays.asList(
                    "transaction",
                    "events"
            );
        }
        
        @Override
        public Optional<RuleResult> evaluate(JsonNode message, RuleContext context) {
            List<String> missingFields = new ArrayList<>();
            
            // 检查顶层字段
            if (!hasField(message, "transaction")) {
                missingFields.add("transaction");
            } else {
                // 检查transaction内部字段
                JsonNode tx = message.get("transaction");
                checkTransactionFields(tx, missingFields);
            }
            
            if (!hasField(message, "events")) {
                missingFields.add("events");
            } else {
                JsonNode events = message.get("events");
                if (!events.isArray() || events.size() == 0) {
                    missingFields.add("events (empty)");
                }
            }
            
            if (missingFields.isEmpty()) {
                return Optional.of(pass(context));
            }
            
            String msg = String.format("DEX交易缺失必填字段: %s", missingFields);
            Map<String, Object> ctx = new HashMap<>();
            ctx.put("missing_fields", missingFields);
            
            return Optional.of(fail(context, msg, (double) missingFields.size(), 0.0, ctx));
        }
        
        private void checkTransactionFields(JsonNode tx, List<String> missingFields) {
            String[] txFields = {"transaction_hash", "block_number", "from_address", "timestamp"};
            for (String field : txFields) {
                if (!hasField(tx, field)) {
                    missingFields.add("transaction." + field);
                }
            }
        }
    }
    
    /**
     * K线完整性规则
     */
    public static class KlineCompletenessRule extends CompletenessRule {
        
        public KlineCompletenessRule() {
            super("kline.completeness.required_fields", DataDomain.CEX_KLINE);
            
            this.requiredFields = Arrays.asList(
                    "symbol",
                    "interval",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume"
            );
        }
    }
    
    /**
     * 订单簿完整性规则
     */
    public static class OrderbookCompletenessRule extends CompletenessRule {
        
        private int minDepth = 5;
        
        public OrderbookCompletenessRule() {
            super("perp.orderbook.completeness", DataDomain.CEX_PERP_ORDERBOOK);
            
            this.requiredFields = Arrays.asList(
                    "symbol",
                    "bids",
                    "asks"
            );
        }
        
        @Override
        public Optional<RuleResult> evaluate(JsonNode message, RuleContext context) {
            // 先检查基础字段
            Optional<RuleResult> baseResult = super.evaluate(message, context);
            if (baseResult.isPresent() && !baseResult.get().isPassed()) {
                return baseResult;
            }
            
            // 检查深度
            JsonNode bids = message.get("bids");
            JsonNode asks = message.get("asks");
            
            int bidDepth = bids != null && bids.isArray() ? bids.size() : 0;
            int askDepth = asks != null && asks.isArray() ? asks.size() : 0;
            int actualDepth = Math.min(bidDepth, askDepth);
            
            if (actualDepth < minDepth) {
                String msg = String.format("订单簿深度不足: 实际=%d, 要求>=%d", actualDepth, minDepth);
                Map<String, Object> ctx = new HashMap<>();
                ctx.put("bid_depth", bidDepth);
                ctx.put("ask_depth", askDepth);
                ctx.put("min_depth", minDepth);
                
                return Optional.of(fail(context, msg, (double) actualDepth, (double) minDepth, ctx));
            }
            
            return Optional.of(pass(context));
        }
        
        @Override
        protected void parseConfig(Map<String, Object> config) {
            super.parseConfig(config);
            if (config.containsKey("min_depth")) {
                this.minDepth = Integer.parseInt(config.get("min_depth").toString());
            }
        }
    }
    
    /**
     * 成交数据完整性规则
     */
    public static class TradesCompletenessRule extends CompletenessRule {
        
        public TradesCompletenessRule() {
            super("perp.trades.completeness", DataDomain.CEX_PERP_TRADES);
            
            this.requiredFields = Arrays.asList(
                    "symbol",
                    "price",
                    "quantity",
                    "time"
            );
        }
    }
}

