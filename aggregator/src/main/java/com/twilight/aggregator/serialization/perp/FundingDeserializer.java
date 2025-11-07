package com.twilight.aggregator.serialization.perp;

import java.io.IOException;
import java.math.BigDecimal;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.twilight.aggregator.model.perp.FundingData;

/**
 * 资金费率反序列化器
 * 从Kafka topic: binance.perp.funding_rate 消费数据
 * 
 * 输入JSON格式示例：
 * {
 *   "symbol": "BTCUSDT",
 *   "exchange": "binance",
 *   "funding_rate": "0.00010",
 *   "funding_time": 1712208000000,
 *   "funding_interval": "8h",
 *   "exchange_ts": 1712208000000,
 *   "ingest_ts": 1712209125000
 * }
 */
public class FundingDeserializer implements DeserializationSchema<FundingData> {
    private static final long serialVersionUID = 1L;
    private static final Logger log = LoggerFactory.getLogger(FundingDeserializer.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public FundingData deserialize(byte[] message) throws IOException {
        try {
            JsonNode root = objectMapper.readTree(message);
            
            FundingData data = new FundingData();
            data.setSymbol(getStringValue(root, "symbol"));
            data.setExchange(getStringValue(root, "exchange"));
            data.setFundingRate(getBigDecimalValue(root, "funding_rate"));
            data.setFundingTime(getLongValue(root, "funding_time"));
            data.setFundingInterval(getStringValue(root, "funding_interval"));
            data.setExchangeTs(getLongValue(root, "exchange_ts"));
            data.setIngestTs(getLongValue(root, "ingest_ts"));
            
            return data;
            
        } catch (Exception e) {
            log.error("Failed to deserialize Funding data: {}", e.getMessage(), e);
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
    public boolean isEndOfStream(FundingData nextElement) {
        return false;
    }

    @Override
    public TypeInformation<FundingData> getProducedType() {
        return TypeInformation.of(FundingData.class);
    }
}




