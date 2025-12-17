package com.twilight.metadatacore.service.stream;

import java.time.Instant;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

@Component
public class MetadataUpdatePublisher {

    private static final Logger log = LoggerFactory.getLogger(MetadataUpdatePublisher.class);

    private final Sinks.Many<MetadataUpdateEvent> sink = Sinks.many().multicast().onBackpressureBuffer();

    public void publishUpdate(UUID entityId, String changeType, Instant occurredAt) {
        MetadataUpdateEvent event = new MetadataUpdateEvent(entityId, changeType, occurredAt);
        Sinks.EmitResult result = sink.tryEmitNext(event);
        if (result.isFailure()) {
            log.warn("Failed to emit metadata update event: {}", result);
        }
    }

    public Flux<MetadataUpdateEvent> stream() {
        return sink.asFlux();
    }
}
