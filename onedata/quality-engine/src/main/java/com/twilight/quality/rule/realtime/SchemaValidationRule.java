package com.twilight.quality.rule.realtime;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.twilight.quality.domain.enums.AlertLevel;
import com.twilight.quality.domain.enums.DataDomain;
import com.twilight.quality.domain.enums.QualityDimension;
import com.twilight.quality.domain.rule.RuleContext;
import com.twilight.quality.domain.rule.RuleResult;
import com.twilight.quality.rule.base.BaseRule;

import java.util.*;

/**
 * Schema校验规则
 * 检测字段类型是否符合预期
 */
public abstract class SchemaValidationRule extends BaseRule {
    
    /**
     * 字段类型定义
     * Key: 字段路径, Value: 期望类型
     */
    protected Map<String, ExpectedType> fieldTypes = new HashMap<>();
    
    protected SchemaValidationRule(String ruleName, DataDomain... domains) {
        super(ruleName, QualityDimension.SCHEMA, domains);
        this.defaultAlertLevel = AlertLevel.WARNING;
    }
    
    @Override
    public Optional<RuleResult> evaluate(JsonNode message, RuleContext context) {
        List<String> typeErrors = new ArrayList<>();
        
        for (Map.Entry<String, ExpectedType> entry : fieldTypes.entrySet()) {
            String fieldPath = entry.getKey();
            ExpectedType expectedType = entry.getValue();
            
            JsonNode fieldNode = getFieldNode(message, fieldPath);
            
            if (fieldNode == null || fieldNode.isNull()) {
                // 字段不存在，由完整性规则检查，这里跳过
                continue;
            }
            
            if (!matchesType(fieldNode, expectedType)) {
                typeErrors.add(String.format("%s: 期望%s, 实际%s", 
                        fieldPath, expectedType, getActualType(fieldNode)));
            }
        }
        
        if (typeErrors.isEmpty()) {
            return Optional.of(pass(context));
        }
        
        String msg = "Schema校验失败: " + typeErrors;
        Map<String, Object> ctx = new HashMap<>();
        ctx.put("type_errors", typeErrors);
        ctx.put("error_count", typeErrors.size());
        
        return Optional.of(fail(context, msg, (double) typeErrors.size(), 0.0, ctx));
    }
    
    /**
     * 获取嵌套字段节点
     */
    protected JsonNode getFieldNode(JsonNode node, String fieldPath) {
        String[] parts = fieldPath.split("\\.");
        JsonNode current = node;
        
        for (String part : parts) {
            if (current == null || current.isNull()) {
                return null;
            }
            current = current.get(part);
        }
        
        return current;
    }
    
    /**
     * 检查节点类型是否匹配
     */
    protected boolean matchesType(JsonNode node, ExpectedType expectedType) {
        switch (expectedType) {
            case STRING:
                return node.isTextual();
            case NUMBER:
                return node.isNumber() || (node.isTextual() && isNumericString(node.asText()));
            case INTEGER:
                return node.isInt() || node.isLong() || 
                       (node.isTextual() && isIntegerString(node.asText()));
            case BOOLEAN:
                return node.isBoolean();
            case ARRAY:
                return node.isArray();
            case OBJECT:
                return node.isObject();
            case ANY:
                return true;
            default:
                return false;
        }
    }
    
    protected boolean isNumericString(String s) {
        try {
            Double.parseDouble(s);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }
    
    protected boolean isIntegerString(String s) {
        try {
            Long.parseLong(s);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }
    
    protected String getActualType(JsonNode node) {
        if (node == null || node.isNull()) return "NULL";
        JsonNodeType type = node.getNodeType();
        return type.name();
    }
    
    /**
     * 期望类型枚举
     */
    public enum ExpectedType {
        STRING,
        NUMBER,
        INTEGER,
        BOOLEAN,
        ARRAY,
        OBJECT,
        ANY
    }
    
    // ===== 具体规则实现 =====
    
    /**
     * DEX Schema规则
     */
    public static class DexSchemaRule extends SchemaValidationRule {
        
        public DexSchemaRule() {
            super("dex.schema.validation", 
                    DataDomain.DEX_UNISWAP, DataDomain.DEX_HYPERLIQUID);
            
            // 定义字段类型
            fieldTypes.put("transaction", ExpectedType.OBJECT);
            fieldTypes.put("transaction.transaction_hash", ExpectedType.STRING);
            fieldTypes.put("transaction.block_number", ExpectedType.INTEGER);
            fieldTypes.put("transaction.from_address", ExpectedType.STRING);
            fieldTypes.put("transaction.timestamp", ExpectedType.INTEGER);
            fieldTypes.put("events", ExpectedType.ARRAY);
        }
    }
    
    /**
     * K线Schema规则
     */
    public static class KlineSchemaRule extends SchemaValidationRule {
        
        public KlineSchemaRule() {
            super("kline.schema.validation", DataDomain.CEX_KLINE);
            
            fieldTypes.put("symbol", ExpectedType.STRING);
            fieldTypes.put("interval", ExpectedType.STRING);
            fieldTypes.put("open", ExpectedType.NUMBER);
            fieldTypes.put("high", ExpectedType.NUMBER);
            fieldTypes.put("low", ExpectedType.NUMBER);
            fieldTypes.put("close", ExpectedType.NUMBER);
            fieldTypes.put("volume", ExpectedType.NUMBER);
        }
    }
    
    /**
     * 订单簿Schema规则
     */
    public static class OrderbookSchemaRule extends SchemaValidationRule {
        
        public OrderbookSchemaRule() {
            super("perp.orderbook.schema.validation", DataDomain.CEX_PERP_ORDERBOOK);
            
            fieldTypes.put("symbol", ExpectedType.STRING);
            fieldTypes.put("bids", ExpectedType.ARRAY);
            fieldTypes.put("asks", ExpectedType.ARRAY);
        }
        
        @Override
        public Optional<RuleResult> evaluate(JsonNode message, RuleContext context) {
            // 先执行基础Schema检查
            Optional<RuleResult> baseResult = super.evaluate(message, context);
            if (baseResult.isPresent() && !baseResult.get().isPassed()) {
                return baseResult;
            }
            
            // 额外检查：验证bids和asks的元素格式
            List<String> formatErrors = new ArrayList<>();
            
            JsonNode bids = message.get("bids");
            if (bids != null && bids.isArray() && bids.size() > 0) {
                if (!isValidPriceLevel(bids.get(0))) {
                    formatErrors.add("bids元素格式错误，期望[price, qty]或{price, qty}");
                }
            }
            
            JsonNode asks = message.get("asks");
            if (asks != null && asks.isArray() && asks.size() > 0) {
                if (!isValidPriceLevel(asks.get(0))) {
                    formatErrors.add("asks元素格式错误，期望[price, qty]或{price, qty}");
                }
            }
            
            if (!formatErrors.isEmpty()) {
                String msg = "订单簿格式错误: " + formatErrors;
                Map<String, Object> ctx = new HashMap<>();
                ctx.put("format_errors", formatErrors);
                return Optional.of(fail(context, msg, (double) formatErrors.size(), 0.0, ctx));
            }
            
            return Optional.of(pass(context));
        }
        
        private boolean isValidPriceLevel(JsonNode level) {
            if (level == null) return false;
            
            // 数组格式 [price, qty]
            if (level.isArray() && level.size() >= 2) {
                return true;
            }
            
            // 对象格式
            if (level.isObject()) {
                return (level.has("price") || level.has("p")) && 
                       (level.has("qty") || level.has("quantity") || level.has("q"));
            }
            
            return false;
        }
    }
}

