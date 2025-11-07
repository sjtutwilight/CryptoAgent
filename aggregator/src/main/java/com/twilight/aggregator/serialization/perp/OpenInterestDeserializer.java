package com.twilight.aggregator.serialization.perp;

import java.io.IOException;
import java.math.BigDecimal;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.twilight.aggregator.model.perp.OpenInterestData;

/**
 * 持仓量反序列化器
 * 从Kafka topic: binance.perp.open_interest 消费数据
 * 
 * 输入JSON格式示例：
 * {
 *   "symbol": "BTCUSDT",
 *   "exchange": "binance",
 *   "oi": "98765.432",
 *   "oi_usd": "634210000.12",
 *   "exchange_ts": 1712209080000,
 *   "ingest_ts": 1712209081000
 * }
 */
public class OpenInterestDeserializer implements DeserializationSchema<OpenInterestData> {
    private static final long serialVersionUID = 1L;
    private static final Logger log = LoggerFactory.getLogger(OpenInterestDeserializer.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public OpenInterestData deserialize(byte[] message) throws IOException {
        try {
            JsonNode root = objectMapper.readTree(message);
            
            OpenInterestData data = new OpenInterestData();
            data.setSymbol(getStringValue(root, "symbol"));
            data.setExchange(getStringValue(root, "exchange"));
            data.setOi(getBigDecimalValue(root, "oi"));
            data.setOiUsd(getBigDecimalValue(root, "oi_usd"));
            data.setExchangeTs(getLongValue(root, "exchange_ts"));
            data.setIngestTs(getLongValue(root, "ingest_ts"));
            data.setIsCarried(false); // 默认为真实数据
            
            return data;
            
        } catch (Exception e) {
            log.error("Failed to deserialize OpenInterest data: {}", e.getMessage(), e);
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
    public boolean isEndOfStream(OpenInterestData nextElement) {
        return false;
    }

    @Override
    public TypeInformation<OpenInterestData> getProducedType() {
        return TypeInformation.of(OpenInterestData.class);
    }
}




