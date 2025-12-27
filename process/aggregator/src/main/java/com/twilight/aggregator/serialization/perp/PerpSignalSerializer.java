package com.twilight.aggregator.serialization.perp;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.twilight.aggregator.model.perp.PerpSignal;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

/**
 * PerpSignal序列化器 - 将PerpSignal对象序列化为JSON格式写入Kafka
 * 
 * 输出格式：
 * {
 *   "symbol": "BTCUSDT",
 *   "exchange": "binance",  // 支持多交易所：binance, hyperliquid
 *   "signal_time": 1762320100000,
 *   "signal_type": "EXEC_HEALTH",
 *   "signal_level": "WARNING",
 *   "metric_name": "spread_anomaly",
 *   "metric_value": 15.5,
 *   "threshold": 10.0,
 *   "context_json": "{...}"
 * }
 */
public class PerpSignalSerializer implements SerializationSchema<PerpSignal> {
    
    private static final Logger LOG = LoggerFactory.getLogger(PerpSignalSerializer.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    
    @Override
    public byte[] serialize(PerpSignal signal) {
        try {
            ObjectNode json = MAPPER.createObjectNode();
            
            // 基础标识
            json.put("symbol", signal.getSymbol());
            json.put("exchange", signal.getExchange());
            json.put("signal_time", signal.getSignalTime());
            
            // 信号类型（枚举转字符串）
            json.put("signal_type", signal.getSignalType() != null ? signal.getSignalType().name() : null);
            json.put("signal_level", signal.getSignalLevel() != null ? signal.getSignalLevel().name() : null);
            
            // 信号内容
            json.put("metric_name", signal.getMetricName());
            json.put("metric_value", signal.getMetricValue());
            json.put("threshold", signal.getThreshold());
            
            // 上下文
            if (signal.getContextJson() != null) {
                json.put("context_json", signal.getContextJson());
            } else {
                json.putNull("context_json");
            }
            
            return json.toString().getBytes(StandardCharsets.UTF_8);
            
        } catch (Exception e) {
            LOG.error("Failed to serialize PerpSignal: symbol={}, exchange={}, signalTime={}, type={}", 
                    signal.getSymbol(), signal.getExchange(), signal.getSignalTime(), signal.getSignalType(), e);
            return new byte[0];
        }
    }
}

