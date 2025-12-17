package com.twilight.realtime.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.SerializationSchema;

/**
 * Simple Jackson based serialization schema.
 */
public class JsonSerializationSchema<T> implements SerializationSchema<T> {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public byte[] serialize(T element) {
        try {
            return MAPPER.writeValueAsBytes(element);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize object", e);
        }
    }
}
