package com.twilight.aggregator.serialization;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.twilight.aggregator.model.KlineSignal;

/**
 * K线信号序列化器
 * 将KlineSignal对象序列化为JSON格式，输出到Kafka topic: kline.signal
 */
public class KlineSignalSerializer implements SerializationSchema<KlineSignal> {
    private static final long serialVersionUID = 1L;
    private static final Logger log = LoggerFactory.getLogger(KlineSignalSerializer.class);
    
    private transient ObjectMapper objectMapper;
    
    @Override
    public void open(InitializationContext context) throws Exception {
        this.objectMapper = new ObjectMapper();
    }
    
    @Override
    public byte[] serialize(KlineSignal signal) {
        if (signal == null) {
            return null;
        }
        
        try {
            return objectMapper.writeValueAsBytes(signal);
        } catch (Exception e) {
            log.error("Failed to serialize KlineSignal for {}: {}", 
                     signal.getSymbol(), e.getMessage(), e);
            return null;
        }
    }
}

