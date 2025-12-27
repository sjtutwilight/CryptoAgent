package com.twilight.aggregator.serialization;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.twilight.aggregator.model.dexTransaction.KafkaMessage;

import java.io.IOException;

public class KafkaMessageDeserializer implements DeserializationSchema<KafkaMessage> {

    private static final long serialVersionUID = 1L;
    private final ObjectMapper objectMapper;

    public KafkaMessageDeserializer() {
        this.objectMapper = new ObjectMapper();
        // 如果需要注册模块, 如 JavaTimeModule, 可以:
        // this.objectMapper.findAndAddModules();
    }

    @Override
    public KafkaMessage deserialize(byte[] message) throws IOException {
        try {
            // 用 TypeReference 显式告诉 Jackson 目标类型，避免泛型信息丢失
            return objectMapper.readValue(message, new TypeReference<KafkaMessage>() {
            });
        } catch (IOException e) {
            throw new IOException("Error deserializing KafkaMessage", e);
        }
    }

    @Override
    public boolean isEndOfStream(KafkaMessage nextElement) {
        return false;
    }

    @Override
    public TypeInformation<KafkaMessage> getProducedType() {
        // 显式使用 TypeHint，确保 Flink 获取到具体类型信息
        return TypeInformation.of(new TypeHint<KafkaMessage>() {
        });
    }
}