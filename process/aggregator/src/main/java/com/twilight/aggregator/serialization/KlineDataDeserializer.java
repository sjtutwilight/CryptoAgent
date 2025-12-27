package com.twilight.aggregator.serialization;

import java.io.IOException;
import java.math.BigDecimal;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.twilight.aggregator.model.KlineData;

/**
 * K线数据反序列化器
 * 从Kafka topic: binance.kline消费JSON格式的K线数据
 */
public class KlineDataDeserializer implements DeserializationSchema<KlineData> {
    private static final long serialVersionUID = 1L;
    private static final Logger log = LoggerFactory.getLogger(KlineDataDeserializer.class);
    
    private transient ObjectMapper objectMapper;
    
    @Override
    public void open(InitializationContext context) throws Exception {
        this.objectMapper = new ObjectMapper();
    }
    
    @Override
    public KlineData deserialize(byte[] message) throws IOException {
        if (message == null || message.length == 0) {
            log.warn("Received null or empty message");
            return null;
        }
        
        try {
            JsonNode root = objectMapper.readTree(message);
            if (root == null || root.isNull()) {
                return null;
            }
            
            // Binance combined stream会包含 data 字段
            JsonNode payload = root.hasNonNull("data") ? root.get("data") : root;
            if (payload == null || payload.isNull()) {
                return null;
            }
            
            // 订阅确认等非数据消息只包含 result / id
            if (!payload.has("k") && !payload.has("kline")) {
                return null;
            }
            
            JsonNode klineNode = payload.hasNonNull("kline") ? payload.get("kline") : payload.get("k");
            if (klineNode == null || klineNode.isNull()) {
                return null;
            }
            
            KlineData klineData = new KlineData();
            
            // 交易所标识：优先取payload中的exchange/datasource_id，其次使用默认值
            String exchange = getStringValue(payload, "exchange", "datasource_id");
            if (exchange == null) {
                exchange = getStringValue(root, "exchange", "datasource_id");
            }
            if (exchange == null) {
                exchange = "binance";
            }
            klineData.setExchange(exchange);
            
            // 交易对符号
            String symbol = getStringValue(payload, "symbol", "s");
            if (symbol == null) {
                symbol = getStringValue(klineNode, "s", "symbol");
            }
            klineData.setSymbol(symbol);
            
            // K线间隔
            String interval = getStringValue(payload, "interval");
            if (interval == null) {
                interval = getStringValue(klineNode, "i", "interval");
            }
            klineData.setInterval(interval);
            
            // 事件时间
            Long eventTime = getLongValue(payload, "eventTime", "E");
            if (eventTime == null) {
                eventTime = getLongValue(root, "eventTime", "E");
            }
            klineData.setEventTime(eventTime);
            
            // 数据入库时间（缺失时使用当前时间）
            Long ingestTime = getLongValue(payload, "ingestTime");
            if (ingestTime == null) {
                ingestTime = getLongValue(root, "ingestTime");
            }
            if (ingestTime == null) {
                ingestTime = System.currentTimeMillis();
            }
            klineData.setIngestTime(ingestTime);
            
            // 解析K线字段
            KlineData.Kline kline = new KlineData.Kline();
            kline.setStartTime(getLongValue(klineNode, "startTime", "t"));
            kline.setCloseTime(getLongValue(klineNode, "closeTime", "T"));
            kline.setOpenPrice(getBigDecimalValue(klineNode, "openPrice", "o"));
            kline.setClosePrice(getBigDecimalValue(klineNode, "closePrice", "c"));
            kline.setHighPrice(getBigDecimalValue(klineNode, "highPrice", "h"));
            kline.setLowPrice(getBigDecimalValue(klineNode, "lowPrice", "l"));
            kline.setBaseVolume(getBigDecimalValue(klineNode, "baseVolume", "v"));
            kline.setQuoteVolume(getBigDecimalValue(klineNode, "quoteVolume", "q", "Q"));
            kline.setTradeCount(getIntegerValue(klineNode, "tradeCount", "n"));
            kline.setClosed(getBooleanValue(klineNode, "closed", "x"));
            
            klineData.setKline(kline);
            return klineData;
            
        } catch (Exception e) {
            log.error("Failed to deserialize KlineData: {}", e.getMessage(), e);
            return null;
        }
    }
    
    @Override
    public boolean isEndOfStream(KlineData nextElement) {
        return false;
    }
    
    @Override
    public TypeInformation<KlineData> getProducedType() {
        return TypeInformation.of(KlineData.class);
    }
    
    // 辅助方法：安全获取字符串值
    private JsonNode getFirstNonNull(JsonNode node, String... fieldNames) {
        if (node == null || fieldNames == null) {
            return null;
        }
        for (String fieldName : fieldNames) {
            if (fieldName == null) {
                continue;
            }
            JsonNode fieldNode = node.get(fieldName);
            if (fieldNode != null && !fieldNode.isNull()) {
                return fieldNode;
            }
        }
        return null;
    }
    
    private String getStringValue(JsonNode node, String... fieldNames) {
        JsonNode fieldNode = getFirstNonNull(node, fieldNames);
        return (fieldNode != null) ? fieldNode.asText() : null;
    }
    
    private Long getLongValue(JsonNode node, String... fieldNames) {
        JsonNode fieldNode = getFirstNonNull(node, fieldNames);
        if (fieldNode != null) {
            try {
                if (fieldNode.isNumber()) {
                    return fieldNode.longValue();
                }
                if (fieldNode.isTextual()) {
                    return Long.parseLong(fieldNode.asText());
                }
            } catch (NumberFormatException e) {
                log.warn("Failed to parse long for field {}: {}", String.join(",", fieldNames), fieldNode.asText());
            }
        }
        return null;
    }
    
    private Integer getIntegerValue(JsonNode node, String... fieldNames) {
        JsonNode fieldNode = getFirstNonNull(node, fieldNames);
        if (fieldNode != null) {
            if (fieldNode.isNumber()) {
                return fieldNode.intValue();
            }
            if (fieldNode.isTextual()) {
                try {
                    return Integer.parseInt(fieldNode.asText());
                } catch (NumberFormatException e) {
                    log.warn("Failed to parse integer for field {}: {}", String.join(",", fieldNames), fieldNode.asText());
                }
            }
        }
        return null;
    }
    
    private BigDecimal getBigDecimalValue(JsonNode node, String... fieldNames) {
        JsonNode fieldNode = getFirstNonNull(node, fieldNames);
        if (fieldNode != null) {
            try {
                if (fieldNode.isNumber()) {
                    return new BigDecimal(fieldNode.asText());
                }
                if (fieldNode.isTextual()) {
                    return new BigDecimal(fieldNode.asText());
                }
            } catch (NumberFormatException e) {
                log.warn("Failed to parse BigDecimal for field {}: {}", String.join(",", fieldNames), fieldNode.asText());
            }
        }
        return null;
    }
    
    private Boolean getBooleanValue(JsonNode node, String... fieldNames) {
        JsonNode fieldNode = getFirstNonNull(node, fieldNames);
        if (fieldNode != null) {
            if (fieldNode.isBoolean()) {
                return fieldNode.asBoolean();
            }
            if (fieldNode.isTextual()) {
                return Boolean.parseBoolean(fieldNode.asText());
            }
            if (fieldNode.isInt() || fieldNode.isLong()) {
                return fieldNode.asInt() != 0;
            }
        }
        return null;
    }
}







