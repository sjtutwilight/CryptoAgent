package com.twilight.aggregator.serialization.perp;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.twilight.aggregator.model.perp.ContextMetrics;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;

/**
 * ContextMetrics反序列化器 - 从Kafka读取Job2输出的ContextMetrics JSON
 * 
 * 输入格式（来自perp.ctx.1m topic）:
 * {
 *   "symbol": "BTCUSDT",
 *   "exchange": "binance",
 *   "end_time": 1762320120000,
 *   "mark_price": "27010.50",
 *   "index_price": "27009.00",
 *   "basis_bps": 5.55,
 *   ...
 * }
 */
public class ContextMetricsDeserializer implements DeserializationSchema<ContextMetrics> {
    
    private static final Logger LOG = LoggerFactory.getLogger(ContextMetricsDeserializer.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    
    @Override
    public ContextMetrics deserialize(byte[] message) throws IOException {
        JsonNode json = MAPPER.readTree(message);
        
        ContextMetrics metrics = new ContextMetrics();
        
        // 基础标识
        metrics.setSymbol(json.get("symbol").asText());
        metrics.setExchange(json.get("exchange").asText());
        metrics.setEndTime(json.get("end_time").asLong());
        
        // 价格指标
        metrics.setMarkPrice(getDecimal(json, "mark_price"));
        metrics.setIndexPrice(getDecimal(json, "index_price"));
        metrics.setBasisBps(getDouble(json, "basis_bps"));
        
        // 资金费率
        metrics.setFundingRate(getDecimal(json, "funding_rate"));
        metrics.setFundingRate8h(getDecimal(json, "funding_rate_8h"));
        metrics.setFundingEma24h(getDecimal(json, "funding_ema_24h"));
        metrics.setNextFundingTime(getLong(json, "next_funding_time"));
        
        // 持仓量
        metrics.setOi(getDecimal(json, "oi"));
        metrics.setOiUsd(getDecimal(json, "oi_usd"));
        metrics.setOiDelta1m(getDecimal(json, "oi_delta_1m"));
        metrics.setOiDeltaPct(getDouble(json, "oi_delta_pct"));
        metrics.setIsOiCarried(getBoolean(json, "is_oi_carried"));
        
        return metrics;
    }
    
    @Override
    public boolean isEndOfStream(ContextMetrics nextElement) {
        return false;
    }
    
    @Override
    public TypeInformation<ContextMetrics> getProducedType() {
        return TypeInformation.of(ContextMetrics.class);
    }
    
    // ===== 辅助方法 =====
    
    private BigDecimal getDecimal(JsonNode json, String field) {
        JsonNode node = json.get(field);
        if (node == null || node.isNull()) {
            return null;
        }
        return new BigDecimal(node.asText());
    }
    
    private Double getDouble(JsonNode json, String field) {
        JsonNode node = json.get(field);
        if (node == null || node.isNull()) {
            return null;
        }
        return node.asDouble();
    }
    
    private Long getLong(JsonNode json, String field) {
        JsonNode node = json.get(field);
        if (node == null || node.isNull()) {
            return null;
        }
        return node.asLong();
    }
    
    private Boolean getBoolean(JsonNode json, String field) {
        JsonNode node = json.get(field);
        if (node == null || node.isNull()) {
            return false;
        }
        return node.asBoolean();
    }
}





