package com.twilight.aggregator.serialization.perp;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.twilight.aggregator.model.perp.ExecutionMetrics;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;

/**
 * ExecutionMetrics反序列化器 - 从Kafka读取Job1输出的ExecutionMetrics JSON
 * 
 * 输入格式（来自perp.exec.1s topic）:
 * {
 *   "symbol": "BTCUSDT",
 *   "exchange": "binance",
 *   "end_time": 1762320100000,
 *   "mid_price": "27010.25",
 *   "spread_bps": 1.85,
 *   ...
 * }
 */
public class ExecutionMetricsDeserializer implements DeserializationSchema<ExecutionMetrics> {
    
    private static final Logger LOG = LoggerFactory.getLogger(ExecutionMetricsDeserializer.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    
    @Override
    public ExecutionMetrics deserialize(byte[] message) throws IOException {
        JsonNode json = MAPPER.readTree(message);
        
        ExecutionMetrics metrics = new ExecutionMetrics();
        
        // 基础标识
        metrics.setSymbol(json.get("symbol").asText());
        metrics.setExchange(json.get("exchange").asText());
        metrics.setEndTime(json.get("end_time").asLong());
        
        // 订单簿指标
        metrics.setMidPrice(getDecimal(json, "mid_price"));
        metrics.setSpreadBps(getDouble(json, "spread_bps"));
        metrics.setSpreadAbs(getDecimal(json, "spread_abs"));
        
        // 深度指标
        metrics.setDepth10k(getDecimal(json, "depth_10k"));
        metrics.setDepth50k(getDecimal(json, "depth_50k"));
        metrics.setDepth100k(getDecimal(json, "depth_100k"));
        
        // 不平衡指标
        metrics.setImbalanceTop5(getDouble(json, "imbalance_top5"));
        metrics.setImbalanceTotal(getDouble(json, "imbalance_total"));
        
        // 冲击成本
        metrics.setImpact10kBps(getDouble(json, "impact_10k_bps"));
        metrics.setImpact50kBps(getDouble(json, "impact_50k_bps"));
        metrics.setImpact100kBps(getDouble(json, "impact_100k_bps"));
        
        // OFI
        metrics.setOfi(getDouble(json, "ofi"));
        
        // 成交指标
        metrics.setTradeCount(getInt(json, "trade_count"));
        metrics.setVolumeUsd(getDecimal(json, "volume_usd"));
        metrics.setVwap(getDecimal(json, "vwap"));
        metrics.setBuyVolumeUsd(getDecimal(json, "buy_volume_usd"));
        metrics.setSellVolumeUsd(getDecimal(json, "sell_volume_usd"));
        
        // 流动性指标
        metrics.setIlliqLambda(getDouble(json, "illiq_lambda"));
        
        return metrics;
    }
    
    @Override
    public boolean isEndOfStream(ExecutionMetrics nextElement) {
        return false;
    }
    
    @Override
    public TypeInformation<ExecutionMetrics> getProducedType() {
        return TypeInformation.of(ExecutionMetrics.class);
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
    
    private Integer getInt(JsonNode json, String field) {
        JsonNode node = json.get(field);
        if (node == null || node.isNull()) {
            return null;
        }
        return node.asInt();
    }
}



