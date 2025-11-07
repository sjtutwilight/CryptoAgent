package com.twilight.aggregator.serialization.perp;

import java.io.IOException;
import java.math.BigDecimal;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.twilight.aggregator.model.perp.MarkIndexData;

/**
 * 标记价格/指数价格反序列化器
 * 从Kafka topic: binance.perp.mark_index 消费数据
 * 
 * 输入JSON格式示例：
 * {
 *   "symbol": "BTCUSDT",
 *   "exchange": "binance",
 *   "mark_price": "64225.5",
 *   "index_price": "64220.8",
 *   "fair_basis": "0.0007",
 *   "last_funding_rate": "0.00010",
 *   "next_funding_time": 1712227200000,
 *   "exchange_ts": 1712209123000,
 *   "ingest_ts": 1712209123015
 * }
 */
public class MarkIndexDeserializer implements DeserializationSchema<MarkIndexData> {
    private static final long serialVersionUID = 1L;
    private static final Logger log = LoggerFactory.getLogger(MarkIndexDeserializer.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public MarkIndexData deserialize(byte[] message) throws IOException {
        try {
            JsonNode root = objectMapper.readTree(message);
            
            MarkIndexData data = new MarkIndexData();
            data.setSymbol(getStringValue(root, "symbol"));
            data.setExchange(getStringValue(root, "exchange"));
            data.setMarkPrice(getBigDecimalValue(root, "mark_price"));
            data.setIndexPrice(getBigDecimalValue(root, "index_price"));
            data.setFairBasis(getBigDecimalValue(root, "fair_basis"));
            data.setLastFundingRate(getBigDecimalValue(root, "last_funding_rate"));
            data.setNextFundingTime(getLongValue(root, "next_funding_time"));
            data.setExchangeTs(getLongValue(root, "exchange_ts"));
            data.setIngestTs(getLongValue(root, "ingest_ts"));
            
            return data;
            
        } catch (Exception e) {
            log.error("Failed to deserialize MarkIndex data: {}", e.getMessage(), e);
            return null;
        }
    }
    
    private String getStringValue(JsonNode node, String fieldName) {
        return node.has(fieldName) ? node.get(fieldName).asText() : null;
    }
    
    private Long getLongValue(JsonNode node, String fieldName) {
        return node.has(fieldName) ? node.get(fieldName).asLong() : null;
    }
    
    private BigDecimal getBigDecimalValue(JsonNode node, String fieldName) {
        if (node.has(fieldName) && !node.get(fieldName).isNull()) {
            return new BigDecimal(node.get(fieldName).asText());
        }
        return null;
    }

    @Override
    public boolean isEndOfStream(MarkIndexData nextElement) {
        return false;
    }

    @Override
    public TypeInformation<MarkIndexData> getProducedType() {
        return TypeInformation.of(MarkIndexData.class);
    }
}




