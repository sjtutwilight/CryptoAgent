package com.twilight.realtime.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

/**
 * Generic Jackson based deserializer for Kafka sources.
 * 显式提供TypeInformation以避免类型擦除问题
 */
public class JsonDeserializationSchema<T> extends AbstractDeserializationSchema<T> {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final Class<T> clazz;

    public JsonDeserializationSchema(Class<T> clazz) {
        super(TypeInformation.of(clazz));
        this.clazz = clazz;
    }

    @Override
    public T deserialize(byte[] message) throws IOException {
        if (message == null) {
            return null;
        }
        return MAPPER.readValue(message, clazz);
    }
}
