package com.twilight.aggregator.serialization.perp;

import java.io.IOException;
import java.math.BigDecimal;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.twilight.aggregator.model.perp.TradeData;

/**
 * 成交数据反序列化器
 * 从Kafka topic: binance.perp.aggtrades 消费数据
 * 
 * 注意：使用Binance aggTrade而非普通trade，因为永续合约API不提供trade stream
 * 
 * 输入JSON格式示例：
 * {
 *   "symbol": "BTCUSDT",
 *   "exchange": "binance",
 *   "price": "27069.88",       // 价格精度：2位小数（0.01 USD tick）
 *   "size": "0.050",           // 数量精度：3位小数（0.001 BTC tick）
 *   "side": "buy",             // 主动方向：buy/sell
 *   "buyer_maker": false,      // 买方是否为挂单方
 *   "exchange_ts": 1762156245087,
 *   "ingest_ts": 1762156245702,
 *   "trade_id": 5933595,       // 聚合成交ID（aggTradeID）
 *   "buyer_order_id": 0,       // aggTrade中无订单ID，固定为0
 *   "seller_order_id": 0
 * }
 */
public class TradeDeserializer implements DeserializationSchema<TradeData> {
    private static final long serialVersionUID = 1L;
    private static final Logger log = LoggerFactory.getLogger(TradeDeserializer.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public TradeData deserialize(byte[] message) throws IOException {
        try {
            JsonNode root = objectMapper.readTree(message);
            
            TradeData data = new TradeData();
            data.setSymbol(getStringValue(root, "symbol"));
            data.setExchange(getStringValue(root, "exchange"));
            data.setPrice(getBigDecimalValue(root, "price"));
            data.setSize(getBigDecimalValue(root, "size"));
            data.setSide(getStringValue(root, "side"));
            data.setBuyerMaker(getBooleanValue(root, "buyer_maker"));
            data.setExchangeTs(getLongValue(root, "exchange_ts"));
            data.setIngestTs(getLongValue(root, "ingest_ts"));
            data.setTradeId(getLongValue(root, "trade_id"));
            data.setBuyerOrderId(getLongValue(root, "buyer_order_id"));
            data.setSellerOrderId(getLongValue(root, "seller_order_id"));
            
            return data;
            
        } catch (Exception e) {
            log.error("Failed to deserialize Trade data: {}", e.getMessage(), e);
            return null;
        }
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
    
    private BigDecimal getBigDecimalValue(JsonNode node, String fieldName) {
        if (node.has(fieldName) && !node.get(fieldName).isNull()) {
            return new BigDecimal(node.get(fieldName).asText());
        }
        return null;
    }

    @Override
    public boolean isEndOfStream(TradeData nextElement) {
        return false;
    }

    @Override
    public TypeInformation<TradeData> getProducedType() {
        return TypeInformation.of(TradeData.class);
    }
}

