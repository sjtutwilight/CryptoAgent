package com.twilight.aggregator.serialization.perp;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.twilight.aggregator.model.perp.PanelMetrics;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;

/**
 * PanelMetrics序列化器 - 将PanelMetrics对象序列化为JSON格式写入Kafka
 * 
 * 输出格式：
 * {
 *   "symbol": "BTCUSDT",
 *   "exchange": "hyperliquid",
 *   "end_time": 1762320120000,
 *   
 *   // 执行面聚合（1s rollup）
 *   "avg_spread_bps": 2.3,
 *   "max_spread_bps": 5.8,
 *   "avg_depth_50k": "125000.00",
 *   "avg_impact_50k_bps": 3.2,
 *   "avg_imbalance": 0.05,
 *   "sum_ofi": -12.5,
 *   "volume_usd": "1234567.89",
 *   "trade_count": 234,
 *   
 *   // 语境面
 *   "mark_price": "27010.50",
 *   "basis_bps": 5.55,
 *   "funding_rate": "0.00010",
 *   "funding_ema_24h": "0.00012",
 *   "oi_usd": "334567890.12",
 *   "oi_delta_1m": "1234.56",
 *   
 *   // 衍生指标
 *   "liquidity_regime": "NORMAL",
 *   "crowding_score": 1.23
 * }
 */
public class PanelMetricsSerializer implements SerializationSchema<PanelMetrics> {
    
    private static final Logger LOG = LoggerFactory.getLogger(PanelMetricsSerializer.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    
    @Override
    public byte[] serialize(PanelMetrics panel) {
        try {
            ObjectNode json = MAPPER.createObjectNode();
            
            // 基础标识
            json.put("symbol", panel.getSymbol());
            json.put("exchange", panel.getExchange());
            json.put("end_time", panel.getEndTime());
            
            // 执行面聚合指标（从1s rollup）
            json.put("avg_spread_bps", panel.getAvgSpreadBps());
            json.put("max_spread_bps", panel.getMaxSpreadBps());
            putDecimal(json, "avg_depth_50k", panel.getAvgDepth50k());
            json.put("avg_impact_50k_bps", panel.getAvgImpact50kBps());
            json.put("avg_imbalance", panel.getAvgImbalance());
            json.put("sum_ofi", panel.getSumOfi());
            putDecimal(json, "volume_usd", panel.getVolumeUsd());
            json.put("trade_count", panel.getTradeCount());
            
            // 语境面指标
            putDecimal(json, "mark_price", panel.getMarkPrice());
            json.put("basis_bps", panel.getBasisBps());
            putDecimal(json, "funding_rate", panel.getFundingRate());
            putDecimal(json, "funding_ema_24h", panel.getFundingEma24h());
            putDecimal(json, "oi_usd", panel.getOiUsd());
            putDecimal(json, "oi_delta_1m", panel.getOiDelta1m());
            
            // 衍生指标
            json.put("liquidity_regime", panel.getLiquidityRegime());
            json.put("crowding_score", panel.getCrowdingScore());
            
            return json.toString().getBytes(StandardCharsets.UTF_8);
            
        } catch (Exception e) {
            LOG.error("Failed to serialize PanelMetrics: symbol={}, exchange={}, endTime={}", 
                    panel.getSymbol(), panel.getExchange(), panel.getEndTime(), e);
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





