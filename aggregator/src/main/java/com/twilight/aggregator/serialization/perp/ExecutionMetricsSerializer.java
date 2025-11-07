package com.twilight.aggregator.serialization.perp;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.twilight.aggregator.model.perp.ExecutionMetrics;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;

/**
 * ExecutionMetrics序列化器 - 将ExecutionMetrics对象序列化为JSON格式写入Kafka
 * 
 * 输出格式：
 * {
 *   "symbol": "BTCUSDT",
 *   "exchange": "binance",  // 支持多交易所：binance, hyperliquid
 *   "end_time": 1762320100000,
 *   "mid_price": "27010.25",
 *   "spread_bps": 1.85,
 *   "spread_abs": "5.00",
 *   "depth_10k": "12500.50",
 *   "depth_50k": "62300.75",
 *   "depth_100k": "120000.00",
 *   "imbalance_top5": 0.12,
 *   "imbalance_total": 0.08,
 *   "impact_10k_bps": 0.5,
 *   "impact_50k_bps": 2.3,
 *   "impact_100k_bps": 4.8,
 *   "ofi": 125.5,
 *   "trade_count": 45,
 *   "volume_usd": "123456.78",
 *   "vwap": "27012.50",
 *   "buy_volume_usd": "65000.00",
 *   "sell_volume_usd": "58456.78",
 *   "illiq_lambda": 0.0002
 * }
 */
public class ExecutionMetricsSerializer implements SerializationSchema<ExecutionMetrics> {
    
    private static final Logger LOG = LoggerFactory.getLogger(ExecutionMetricsSerializer.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    
    @Override
    public byte[] serialize(ExecutionMetrics metrics) {
        try {
            ObjectNode json = MAPPER.createObjectNode();
            
            // 基础标识
            json.put("symbol", metrics.getSymbol());
            json.put("exchange", metrics.getExchange());
            json.put("end_time", metrics.getEndTime());
            
            // 订单簿指标
            putDecimal(json, "mid_price", metrics.getMidPrice());
            json.put("spread_bps", metrics.getSpreadBps());
            putDecimal(json, "spread_abs", metrics.getSpreadAbs());
            
            // 深度指标
            putDecimal(json, "depth_10k", metrics.getDepth10k());
            putDecimal(json, "depth_50k", metrics.getDepth50k());
            putDecimal(json, "depth_100k", metrics.getDepth100k());
            
            // 不平衡指标
            json.put("imbalance_top5", metrics.getImbalanceTop5());
            json.put("imbalance_total", metrics.getImbalanceTotal());
            
            // 冲击成本
            json.put("impact_10k_bps", metrics.getImpact10kBps());
            json.put("impact_50k_bps", metrics.getImpact50kBps());
            json.put("impact_100k_bps", metrics.getImpact100kBps());
            
            // OFI
            json.put("ofi", metrics.getOfi());
            
            // 成交指标
            json.put("trade_count", metrics.getTradeCount());
            putDecimal(json, "volume_usd", metrics.getVolumeUsd());
            putDecimal(json, "vwap", metrics.getVwap());
            putDecimal(json, "buy_volume_usd", metrics.getBuyVolumeUsd());
            putDecimal(json, "sell_volume_usd", metrics.getSellVolumeUsd());
            
            // 流动性指标
            json.put("illiq_lambda", metrics.getIlliqLambda());
            
            return json.toString().getBytes(StandardCharsets.UTF_8);
            
        } catch (Exception e) {
            LOG.error("Failed to serialize ExecutionMetrics: symbol={}, exchange={}, endTime={}", 
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



