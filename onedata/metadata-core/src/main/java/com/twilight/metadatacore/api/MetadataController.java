package com.twilight.metadatacore.api;

import com.twilight.metadatacore.domain.dto.DomainStats;
import com.twilight.metadatacore.domain.dto.MetadataEntityDetail;
import com.twilight.metadatacore.domain.dto.MetadataEntitySummary;
import com.twilight.metadatacore.domain.dto.MetadataLineageNode;
import com.twilight.metadatacore.domain.dto.MetadataQualityView;
import com.twilight.metadatacore.domain.dto.MetadataSearchRequest;
import com.twilight.metadatacore.service.query.DomainStatsService;
import com.twilight.metadatacore.service.query.MetadataDetailService;
import com.twilight.metadatacore.service.query.MetadataLineageService;
import com.twilight.metadatacore.service.query.MetadataLineageService.Direction;
import com.twilight.metadatacore.service.query.MetadataSearchService;
import com.twilight.metadatacore.service.stream.MetadataUpdateEvent;
import com.twilight.metadatacore.service.stream.MetadataUpdatePublisher;
import java.io.IOException;
import java.time.Duration;
import java.util.UUID;
import org.springframework.data.domain.Page;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import reactor.core.Disposable;

@RestController
@RequestMapping("/v1/metadata")
public class MetadataController {

    private final MetadataSearchService searchService;
    private final MetadataDetailService detailService;
    private final MetadataLineageService lineageService;
    private final DomainStatsService domainStatsService;
    private final MetadataUpdatePublisher updatePublisher;

    public MetadataController(MetadataSearchService searchService,
                              MetadataDetailService detailService,
                              MetadataLineageService lineageService,
                              DomainStatsService domainStatsService,
                              MetadataUpdatePublisher updatePublisher) {
        this.searchService = searchService;
        this.detailService = detailService;
        this.lineageService = lineageService;
        this.domainStatsService = domainStatsService;
        this.updatePublisher = updatePublisher;
    }

    @GetMapping("/entities")
    public Page<MetadataEntitySummary> search(@ModelAttribute MetadataSearchRequest request) {
        return searchService.search(request);
    }

    @GetMapping("/entities/{entityId}")
    public MetadataEntityDetail detail(@PathVariable UUID entityId) {
        return detailService.getDetail(entityId);
    }

    @GetMapping("/entities/{entityId}/lineage")
    public MetadataLineageNode lineage(@PathVariable UUID entityId,
                                       @RequestParam(defaultValue = "down") String direction) {
        Direction dir = "up".equalsIgnoreCase(direction) ? Direction.UPSTREAM : Direction.DOWNSTREAM;
        return lineageService.traverse(entityId, dir);
    }

    @GetMapping("/entities/{entityId}/quality")
    public MetadataQualityView quality(@PathVariable UUID entityId) {
        MetadataEntityDetail detail = detailService.getDetail(entityId);
        return detail.getQuality();
    }

    @GetMapping("/domains/{domain}/stats")
    public DomainStats domainStats(@PathVariable String domain) {
        return domainStatsService.getStats(domain);
    }

    @GetMapping(path = "/updates/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter updates() {
        SseEmitter emitter = new SseEmitter(Duration.ofMinutes(15).toMillis());
        Disposable subscription = updatePublisher.stream().subscribe(event -> sendEvent(emitter, event),
                emitter::completeWithError);

        emitter.onTimeout(() -> {
            subscription.dispose();
            emitter.complete();
        });
        emitter.onCompletion(subscription::dispose);
        emitter.onError(ex -> subscription.dispose());
        return emitter;
    }

    private void sendEvent(SseEmitter emitter, MetadataUpdateEvent event) {
        try {
            emitter.send(SseEmitter.event()
                    .name("metadata-update")
                    .id(event.getEntityId().toString())
                    .data(event));
        } catch (IOException e) {
            emitter.completeWithError(e);
        }
    }
}
