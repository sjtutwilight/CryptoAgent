package com.twilight.aggregator.serialization;

import java.io.IOException;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.twilight.aggregator.model.dexTransaction.Event;

public class EventDeserializer implements DeserializationSchema<Event> {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public Event deserialize(byte[] bytes) throws IOException {
        return MAPPER.readValue(bytes, Event.class);
    }

    @Override
    public boolean isEndOfStream(Event event) {
        return false;
    }

    @Override
    public TypeInformation<Event> getProducedType() {
        return Types.POJO(Event.class);
    }
} 