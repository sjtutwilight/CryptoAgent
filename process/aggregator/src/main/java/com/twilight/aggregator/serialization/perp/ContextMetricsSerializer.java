package com.twilight.aggregator.serialization.perp;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.twilight.aggregator.model.perp.ContextMetrics;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;

/**
 * ContextMetrics序列化器 - 将ContextMetrics对象序列化为JSON格式写入Kafka
 * 
 * 输出格式：
 * {
 *   "symbol": "BTCUSDT",
 *   "exchange": "binance",  // 支持多交易所：binance, hyperliquid
 *   "end_time": 1762320120000,
 *   "mark_price": "27010.50",
 *   "index_price": "27009.00",
 *   "basis_bps": 5.55,
 *   "funding_rate": "0.00010",
 *   "funding_rate_8h": "0.00080",
 *   "funding_ema_24h": "0.00012",
 *   "next_funding_time": 1762356000000,
 *   "oi": "12345.67",
 *   "oi_usd": "334567890.12",
 *   "oi_delta_1m": "1234.56",
 *   "oi_delta_pct": 0.5,
 *   "is_oi_carried": false
 * }
 */
public class ContextMetricsSerializer implements SerializationSchema<ContextMetrics> {
    
    private static final Logger LOG = LoggerFactory.getLogger(ContextMetricsSerializer.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    
    @Override
    public byte[] serialize(ContextMetrics metrics) {
        try {
            ObjectNode json = MAPPER.createObjectNode();
            
            // 基础标识
            json.put("symbol", metrics.getSymbol());
            json.put("exchange", metrics.getExchange());
            json.put("end_time", metrics.getEndTime());
            
            // 价格指标
            putDecimal(json, "mark_price", metrics.getMarkPrice());
            putDecimal(json, "index_price", metrics.getIndexPrice());
            json.put("basis_bps", metrics.getBasisBps());
            
            // 资金费率
            putDecimal(json, "funding_rate", metrics.getFundingRate());
            putDecimal(json, "funding_rate_8h", metrics.getFundingRate8h());
            putDecimal(json, "funding_ema_24h", metrics.getFundingEma24h());
            json.put("next_funding_time", metrics.getNextFundingTime());
            
            // 持仓量
            putDecimal(json, "oi", metrics.getOi());
            putDecimal(json, "oi_usd", metrics.getOiUsd());
            putDecimal(json, "oi_delta_1m", metrics.getOiDelta1m());
            json.put("oi_delta_pct", metrics.getOiDeltaPct());
            json.put("is_oi_carried", metrics.getIsOiCarried());
            
            return json.toString().getBytes(StandardCharsets.UTF_8);
            
        } catch (Exception e) {
            LOG.error("Failed to serialize ContextMetrics: symbol={}, exchange={}, endTime={}", 
                    metrics.getSymbol(), metrics.getExchange(), metrics.getEndTime(), e);
            return new byte[0];
        }
    }
    
    /**
     * 辅助方法：将BigDecimal安全地写入JSON
     */
    private void putDecimal(ObjectNode json, String field, BigDecimal value) {
        if (value != null) {
            json.put(field, value.toPlainString());
        } else {
            json.putNull(field);
        }
    }
}

