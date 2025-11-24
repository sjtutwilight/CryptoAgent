package com.twilight.aggregator.serialization.perp;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.twilight.aggregator.model.perp.OrderBookData;
import com.twilight.aggregator.model.perp.OrderBookData.Depth;
import com.twilight.aggregator.model.perp.OrderBookData.PriceLevel;

/**
 * 订单簿数据反序列化器
 * 从Kafka topic: binance.perp.orderbook 消费数据
 * 
 * 输入JSON格式示例：
 * {
 *   "symbol": "BTCUSDT",
 *   "exchange": "binance",
 *   "depth": {
 *     "bids": [["64231.1", "0.521"], ["64230.0", "1.234"], ...],
 *     "asks": [["64232.5", "0.432"], ["64233.0", "0.876"], ...]
 *   },
 *   "seq": 1234567890,
 *   "snapshot": false,
 *   "exchange_ts": 1712209123456,
 *   "ingest_ts": 1712209123470
 * }
 */
public class OrderBookDeserializer implements DeserializationSchema<OrderBookData> {
    private static final long serialVersionUID = 1L;
    private static final Logger log = LoggerFactory.getLogger(OrderBookDeserializer.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public OrderBookData deserialize(byte[] message) throws IOException {
        try {
            JsonNode root = objectMapper.readTree(message);
            
            OrderBookData data = new OrderBookData();
            data.setSymbol(getStringValue(root, "symbol"));
            data.setExchange(getStringValue(root, "exchange"));
            data.setSeq(getLongValue(root, "seq"));
            data.setSnapshot(getBooleanValue(root, "snapshot"));
            data.setExchangeTs(getLongValue(root, "exchange_ts"));
            data.setIngestTs(getLongValue(root, "ingest_ts"));
            
            // 解析depth对象
            if (root.has("depth")) {
                JsonNode depthNode = root.get("depth");
                Depth depth = new Depth();
                
                // 解析bids
                if (depthNode.has("bids")) {
                    depth.setBids(parsePriceLevels(depthNode.get("bids")));
                }
                
                // 解析asks
                if (depthNode.has("asks")) {
                    depth.setAsks(parsePriceLevels(depthNode.get("asks")));
                }
                
                data.setDepth(depth);
            }
            
            return data;
            
        } catch (Exception e) {
            log.error("Failed to deserialize OrderBook data: {}", e.getMessage(), e);
            // 返回null会被Flink过滤掉，不会导致任务失败
            return null;
        }
    }
    
    /**
     * 解析价格档位列表
     * [[price, size], ...] -> List<PriceLevel>
     */
    private List<PriceLevel> parsePriceLevels(JsonNode arrayNode) {
        List<PriceLevel> levels = new ArrayList<>();
        if (arrayNode != null && arrayNode.isArray()) {
            for (JsonNode levelNode : arrayNode) {
                if (levelNode.isArray() && levelNode.size() >= 2) {
                    BigDecimal price = new BigDecimal(levelNode.get(0).asText());
                    BigDecimal size = new BigDecimal(levelNode.get(1).asText());
                    levels.add(new PriceLevel(price, size));
                }
            }
        }
        return levels;
    }
    
    private String getStringValue(JsonNode node, String fieldName) {
        return node.has(fieldName) ? node.get(fieldName).asText() : null;
    }
    
    private Long getLongValue(JsonNode node, String fieldName) {
        return node.has(fieldName) ? node.get(fieldName).asLong() : null;
    }
    
    private Boolean getBooleanValue(JsonNode node, String fieldName) {
        return node.has(fieldName) ? node.get(fieldName).asBoolean() : null;
    }

    @Override
    public boolean isEndOfStream(OrderBookData nextElement) {
        return false;
    }

    @Override
    public TypeInformation<OrderBookData> getProducedType() {
        return TypeInformation.of(OrderBookData.class);
    }
}






