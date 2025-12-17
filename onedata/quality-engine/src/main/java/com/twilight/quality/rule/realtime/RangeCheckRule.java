package com.twilight.quality.rule.realtime;

import com.fasterxml.jackson.databind.JsonNode;
import com.twilight.quality.domain.enums.AlertLevel;
import com.twilight.quality.domain.enums.DataDomain;
import com.twilight.quality.domain.enums.QualityDimension;
import com.twilight.quality.domain.rule.RuleContext;
import com.twilight.quality.domain.rule.RuleResult;
import com.twilight.quality.rule.base.BaseRule;

import java.math.BigDecimal;
import java.util.*;

/**
 * 数值范围检测规则
 * 检测数值字段是否在合理范围内
 */
public abstract class RangeCheckRule extends BaseRule {
    
    protected double minValue = 0;
    protected double maxValue = Double.MAX_VALUE;
    protected List<String> valueFields = new ArrayList<>();
    
    protected RangeCheckRule(String ruleName, DataDomain... domains) {
        super(ruleName, QualityDimension.ACCURACY, domains);
        this.defaultAlertLevel = AlertLevel.WARNING;
    }
    
    @Override
    public Optional<RuleResult> evaluate(JsonNode message, RuleContext context) {
        List<String> outOfRangeFields = new ArrayList<>();
        Map<String, Double> fieldValues = new HashMap<>();
        
        for (String field : valueFields) {
            Double value = extractValue(message, field);
            if (value != null) {
                fieldValues.put(field, value);
                if (value < minValue || value > maxValue) {
                    outOfRangeFields.add(field);
                }
            }
        }
        
        if (outOfRangeFields.isEmpty()) {
            return Optional.of(pass(context));
        }
        
        String msg = String.format("数值超出范围 [%s, %s]: %s", 
                formatNumber(minValue), formatNumber(maxValue), outOfRangeFields);
        
        Map<String, Object> ctx = new HashMap<>();
        ctx.put("out_of_range_fields", outOfRangeFields);
        ctx.put("field_values", fieldValues);
        ctx.put("min_value", minValue);
        ctx.put("max_value", maxValue);
        
        // 返回第一个超范围的值作为指标
        Double firstValue = fieldValues.get(outOfRangeFields.get(0));
        
        return Optional.of(fail(context, msg, firstValue, maxValue, ctx));
    }
    
    /**
     * 从消息中提取数值（支持嵌套路径）
     */
    protected Double extractValue(JsonNode node, String fieldPath) {
        String[] parts = fieldPath.split("\\.");
        JsonNode current = node;
        
        for (String part : parts) {
            if (current == null || current.isNull()) {
                return null;
            }
            current = current.get(part);
        }
        
        if (current == null || current.isNull()) {
            return null;
        }
        
        if (current.isNumber()) {
            return current.asDouble();
        }
        
        // 尝试解析字符串数值
        if (current.isTextual()) {
            try {
                return new BigDecimal(current.asText()).doubleValue();
            } catch (NumberFormatException e) {
                return null;
            }
        }
        
        return null;
    }
    
    protected String formatNumber(double value) {
        if (value == Double.MAX_VALUE) {
            return "MAX";
        }
        if (value >= 1_000_000_000) {
            return String.format("%.2fB", value / 1_000_000_000);
        }
        if (value >= 1_000_000) {
            return String.format("%.2fM", value / 1_000_000);
        }
        if (value >= 1_000) {
            return String.format("%.2fK", value / 1_000);
        }
        return String.format("%.2f", value);
    }
    
    @Override
    protected void parseConfig(Map<String, Object> config) {
        super.parseConfig(config);
        if (config.containsKey("min_value") || config.containsKey("min_amount") || config.containsKey("min_price")) {
            Object min = config.getOrDefault("min_value", 
                    config.getOrDefault("min_amount", config.get("min_price")));
            if (min != null) {
                this.minValue = Double.parseDouble(min.toString());
            }
        }
        if (config.containsKey("max_value") || config.containsKey("max_amount") || config.containsKey("max_price")) {
            Object max = config.getOrDefault("max_value", 
                    config.getOrDefault("max_amount", config.get("max_price")));
            if (max != null) {
                this.maxValue = Double.parseDouble(max.toString());
            }
        }
        if (config.containsKey("value_fields")) {
            Object fields = config.get("value_fields");
            if (fields instanceof List) {
                this.valueFields = new ArrayList<>((List<String>) fields);
            }
        }
    }
    
    // ===== 具体规则实现 =====
    
    /**
     * DEX金额范围规则
     */
    public static class DexAmountRangeRule extends RangeCheckRule {
        
        public DexAmountRangeRule() {
            super("dex.accuracy.amount_range", 
                    DataDomain.DEX_UNISWAP, DataDomain.DEX_HYPERLIQUID);
            
            this.minValue = 0;
            this.maxValue = 1_000_000_000_000.0; // 1万亿
        }
        
        @Override
        public Optional<RuleResult> evaluate(JsonNode message, RuleContext context) {
            // DEX交易需要检查events中的金额
            if (!message.has("events") || !message.get("events").isArray()) {
                return Optional.of(pass(context)); // 没有events，跳过检查
            }
            
            List<String> outOfRangeFields = new ArrayList<>();
            Map<String, Double> fieldValues = new HashMap<>();
            
            JsonNode events = message.get("events");
            for (int i = 0; i < events.size(); i++) {
                JsonNode event = events.get(i);
                JsonNode args = event.get("decoded_args");
                if (args == null) continue;
                
                // 检查常见的金额字段
                String[] amountFields = {"amount0In", "amount0Out", "amount1In", "amount1Out", 
                        "amount0", "amount1", "value", "amount"};
                
                for (String field : amountFields) {
                    Double value = extractValue(args, field);
                    if (value != null) {
                        String fieldKey = String.format("events[%d].%s", i, field);
                        fieldValues.put(fieldKey, value);
                        
                        if (value < minValue || value > maxValue) {
                            outOfRangeFields.add(fieldKey);
                        }
                    }
                }
            }
            
            if (outOfRangeFields.isEmpty()) {
                return Optional.of(pass(context));
            }
            
            String msg = String.format("DEX金额超出范围 [%s, %s]: %s", 
                    formatNumber(minValue), formatNumber(maxValue), outOfRangeFields);
            
            Map<String, Object> ctx = new HashMap<>();
            ctx.put("out_of_range_fields", outOfRangeFields);
            ctx.put("field_values", fieldValues);
            
            Double firstValue = fieldValues.get(outOfRangeFields.get(0));
            return Optional.of(fail(context, msg, firstValue, maxValue, ctx));
        }
    }
    
    /**
     * K线价格范围规则
     */
    public static class KlinePriceRangeRule extends RangeCheckRule {
        
        public KlinePriceRangeRule() {
            super("kline.accuracy.price_range", DataDomain.CEX_KLINE);
            
            this.minValue = 0;
            this.maxValue = 10_000_000.0; // 1000万
            this.valueFields = Arrays.asList("open", "high", "low", "close");
        }
        
        @Override
        public Optional<RuleResult> evaluate(JsonNode message, RuleContext context) {
            // 额外检查：high >= low, high >= open, high >= close
            Double high = extractValue(message, "high");
            Double low = extractValue(message, "low");
            Double open = extractValue(message, "open");
            Double close = extractValue(message, "close");
            
            // 先检查基础范围
            Optional<RuleResult> baseResult = super.evaluate(message, context);
            if (baseResult.isPresent() && !baseResult.get().isPassed()) {
                return baseResult;
            }
            
            // 检查价格逻辑关系
            List<String> logicErrors = new ArrayList<>();
            
            if (high != null && low != null && high < low) {
                logicErrors.add(String.format("high(%.2f) < low(%.2f)", high, low));
            }
            if (high != null && open != null && high < open) {
                logicErrors.add(String.format("high(%.2f) < open(%.2f)", high, open));
            }
            if (high != null && close != null && high < close) {
                logicErrors.add(String.format("high(%.2f) < close(%.2f)", high, close));
            }
            if (low != null && open != null && low > open) {
                logicErrors.add(String.format("low(%.2f) > open(%.2f)", low, open));
            }
            if (low != null && close != null && low > close) {
                logicErrors.add(String.format("low(%.2f) > close(%.2f)", low, close));
            }
            
            if (!logicErrors.isEmpty()) {
                String msg = "K线价格逻辑错误: " + logicErrors;
                Map<String, Object> ctx = new HashMap<>();
                ctx.put("logic_errors", logicErrors);
                ctx.put("open", open);
                ctx.put("high", high);
                ctx.put("low", low);
                ctx.put("close", close);
                
                return Optional.of(fail(context, msg, null, null, ctx));
            }
            
            return Optional.of(pass(context));
        }
    }
    
    /**
     * 订单簿价格范围规则
     */
    public static class OrderbookPriceRangeRule extends RangeCheckRule {
        
        public OrderbookPriceRangeRule() {
            super("perp.orderbook.price_range", DataDomain.CEX_PERP_ORDERBOOK);
            
            this.minValue = 0;
            this.maxValue = 10_000_000.0;
        }
        
        @Override
        public Optional<RuleResult> evaluate(JsonNode message, RuleContext context) {
            List<String> outOfRangeItems = new ArrayList<>();
            
            // 检查bids
            JsonNode bids = message.get("bids");
            if (bids != null && bids.isArray()) {
                for (int i = 0; i < Math.min(bids.size(), 10); i++) { // 只检查前10档
                    JsonNode bid = bids.get(i);
                    Double price = extractPriceFromLevel(bid);
                    if (price != null && (price < minValue || price > maxValue)) {
                        outOfRangeItems.add(String.format("bid[%d]=%.2f", i, price));
                    }
                }
            }
            
            // 检查asks
            JsonNode asks = message.get("asks");
            if (asks != null && asks.isArray()) {
                for (int i = 0; i < Math.min(asks.size(), 10); i++) {
                    JsonNode ask = asks.get(i);
                    Double price = extractPriceFromLevel(ask);
                    if (price != null && (price < minValue || price > maxValue)) {
                        outOfRangeItems.add(String.format("ask[%d]=%.2f", i, price));
                    }
                }
            }
            
            if (outOfRangeItems.isEmpty()) {
                return Optional.of(pass(context));
            }
            
            String msg = String.format("订单簿价格超出范围 [%s, %s]: %s", 
                    formatNumber(minValue), formatNumber(maxValue), outOfRangeItems);
            
            Map<String, Object> ctx = new HashMap<>();
            ctx.put("out_of_range_items", outOfRangeItems);
            
            return Optional.of(fail(context, msg, null, maxValue, ctx));
        }
        
        private Double extractPriceFromLevel(JsonNode level) {
            if (level == null) return null;
            
            // 支持数组格式 [price, qty] 和对象格式 {price: x, qty: y}
            if (level.isArray() && level.size() >= 1) {
                return level.get(0).asDouble();
            }
            if (level.has("price")) {
                return level.get("price").asDouble();
            }
            if (level.has("p")) {
                return level.get("p").asDouble();
            }
            
            return null;
        }
    }
}

