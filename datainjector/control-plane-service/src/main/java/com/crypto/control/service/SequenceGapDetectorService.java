package com.crypto.control.service;

import com.crypto.control.config.SequenceDetectorProperties;
import com.crypto.control.dto.SequenceEvent;
import com.crypto.control.dto.TaskCreateRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * SequenceGapDetectorService
 *
 * 消费 data.sequence（或配置的序列事件 topic），根据连续性检测缺失区间，
 * 并通过控制面任务系统触发回补。
 */
@Slf4j
@Service
public class SequenceGapDetectorService {

    private static final String STATE_KEY_PREFIX = "sequence:state:";

    @Autowired
    private SequenceDetectorProperties detectorProperties;

    @Autowired
    private MainProcessorService mainProcessorService;

    @Autowired
    private RedisTemplate<String, String> redisTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    /**
     * 监听序列事件。
     */
    @KafkaListener(topics = "${app.kafka.topics.sequence-events:data.sequence}",
                   groupId = "${sequence-detector.consumer-group:control-plane-sequence-detector}",
                   autoStartup = "${sequence-detector.detector.enabled:true}")
    public void handleSequenceEvent(@Payload String message,
                                    @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                                    @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                                    @Header(KafkaHeaders.OFFSET) long offset,
                                    Acknowledgment acknowledgment) {
        if (!detectorProperties.getDetector().isEnabled()) {
            acknowledgment.acknowledge();
            return;
        }
        try {
            SequenceEvent event = objectMapper.readValue(message, SequenceEvent.class);
            if (event.getType() == null || event.getChainId() == null) {
                log.warn("忽略非法序列消息: topic={}, partition={}, offset={}, payload={}", topic, partition, offset, message);
                acknowledgment.acknowledge();
                return;
            }
            SequenceDetectorProperties.SourceConfig sourceConfig = detectorProperties.getSourceConfig(event.getType());
            if (sourceConfig == null) {
                log.debug("未找到序列类型的缺失检测配置: type={}, chain={}", event.getType(), event.getChainId());
            } else if (!sourceConfig.isEnabled()) {
                log.debug("序列类型被禁用: type={}, chain={}", event.getType(), event.getChainId());
            } else {
                processSequenceEvent(event, sourceConfig);
            }
        } catch (Exception e) {
            log.error("处理序列事件失败: topic={}, partition={}, offset={}, error={}", topic, partition, offset, e.getMessage(), e);
        } finally {
            acknowledgment.acknowledge();
        }
    }

    private void processSequenceEvent(SequenceEvent event, SequenceDetectorProperties.SourceConfig sourceConfig) {
        long sequenceNumber = event.sequenceNumberAsLong();
        if (sequenceNumber < 0) {
            log.warn("序列号解析失败: event={}", event);
            return;
        }

        SequenceState state = loadState(event, sourceConfig);
        if (state == null) {
            state = SequenceState.initial(sequenceNumber + 1);
        }

        if (sequenceNumber > state.getExpectedNext()) {
            long start = state.getExpectedNext();
            long end = sequenceNumber - 1;
            log.info("检测到缺失区间: type={}, chain={}, start={}, end={}, current={}",
                    event.getType(), event.getChainId(), start, end, sequenceNumber);
            scheduleBackfill(event, sourceConfig, start, end);
            state.setExpectedNext(sequenceNumber + 1);
            state.setLastScheduledEnd(end);
        } else if (sequenceNumber == state.getExpectedNext()) {
            state.setExpectedNext(sequenceNumber + 1);
        } else {
            log.debug("收到历史序列: type={}, chain={}, seq={}, expectedNext={}",
                    event.getType(), event.getChainId(), sequenceNumber, state.getExpectedNext());
        }

        state.setLastSeen(sequenceNumber);
        state.setLastUpdated(nowEpochMillis());
        saveState(event, sourceConfig, state);
    }

    private SequenceState loadState(SequenceEvent event, SequenceDetectorProperties.SourceConfig sourceConfig) {
        String key = stateKey(event, sourceConfig);
        String json = redisTemplate.opsForValue().get(key);
        if (json == null) {
            return null;
        }
        try {
            return objectMapper.readValue(json, SequenceState.class);
        } catch (Exception e) {
            log.warn("解析序列状态失败，将重新初始化: key={}, error={}", key, e.getMessage());
            redisTemplate.delete(key);
            return null;
        }
    }

    private void saveState(SequenceEvent event, SequenceDetectorProperties.SourceConfig sourceConfig, SequenceState state) {
        String key = stateKey(event, sourceConfig);
        try {
            String val = objectMapper.writeValueAsString(state);
            redisTemplate.opsForValue().set(key, val,
                    detectorProperties.getDetector().getStateTtlSeconds(), TimeUnit.SECONDS);
        } catch (Exception e) {
            log.error("写入序列状态失败: key={}, error={}", key, e.getMessage(), e);
        }
    }

    private void scheduleBackfill(SequenceEvent event,
                                  SequenceDetectorProperties.SourceConfig sourceConfig,
                                  long startInclusive,
                                  long endInclusive) {
        int maxRange = Math.max(1, resolveMaxRange(sourceConfig));
        int batchSize = Math.max(1, resolveBatchSize(sourceConfig));

        long currentStart = startInclusive;
        while (currentStart <= endInclusive) {
            long currentEnd = Math.min(currentStart + maxRange - 1, endInclusive);
            createBackfillTasks(event, sourceConfig, currentStart, currentEnd, batchSize);
            currentStart = currentEnd + 1;
        }
    }

    private void createBackfillTasks(SequenceEvent event,
                                     SequenceDetectorProperties.SourceConfig sourceConfig,
                                     long rangeStart,
                                     long rangeEnd,
                                     int batchSize) {
        List<long[]> slices = sliceRange(rangeStart, rangeEnd, batchSize);
        for (long[] slice : slices) {
            long sliceStart = slice[0];
            long sliceEnd = slice[1];
            Map<String, Object> payload = buildPayload(event, sourceConfig, sliceStart, sliceEnd);
            Map<String, Object> metadata = buildMetadata(event, sliceStart, sliceEnd);

            TaskCreateRequest request = TaskCreateRequest.builder()
                    .dataSourceId(sourceConfig.getDataSourceId())
                    .taskType(sourceConfig.getTaskType())
                    .payload(payload)
                    .metadata(metadata)
                    .priority(sourceConfig.getPriority())
                    .cost(sourceConfig.getCost())
                    .scheduledTime(LocalDateTime.now())
                    .build();

            MainProcessorService.TaskProcessResult result = mainProcessorService.processTask(request);
            if (result.isSuccess()) {
                log.info("已调度回补任务: type={}, chain={}, sliceStart={}, sliceEnd={}, taskId={}",
                        event.getType(), event.getChainId(), sliceStart, sliceEnd,
                        result.getTaskResponse().getTaskId());
            } else {
                log.error("调度回补任务失败: type={}, chain={}, sliceStart={}, sliceEnd={}, error={}",
                        event.getType(), event.getChainId(), sliceStart, sliceEnd, result.getErrorMessage());
            }
        }
    }

    private Map<String, Object> buildPayload(SequenceEvent event,
                                             SequenceDetectorProperties.SourceConfig sourceConfig,
                                             long start,
                                             long end) {
        Map<String, Object> payload = new HashMap<>();
        if (sourceConfig.getUrl() != null) {
            payload.put("url", sourceConfig.getUrl());
        }
        if (sourceConfig.getMethod() != null) {
            payload.put("method", sourceConfig.getMethod());
        }
        payload.put("params", List.of(toHex(start), toHex(end)));
        payload.put("chain_id", event.getChainId());
        payload.put("source", "sequence-gap-detector");
        payload.put("range_start", start);
        payload.put("range_end", end);
        payload.put("event_type", event.getType());
        payload.put("dataSourceId", sourceConfig.getDataSourceId());
        payload.put("task_id", event.getType() + "-" + start + "-" + end);
        return payload;
    }

    private Map<String, Object> buildMetadata(SequenceEvent event, long start, long end) {
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("gapStart", start);
        metadata.put("gapEnd", end);
        metadata.put("sequenceType", event.getType());
        metadata.put("chainId", event.getChainId());
        metadata.put("detectedAt", nowEpochMillis());
        metadata.put("sequenceHash", event.getSequenceHash());
        metadata.put("sequenceTimestamp", event.getSequenceTimestamp());
        metadata.put("processTime", event.getProcessTime());
        return metadata;
    }

    private List<long[]> sliceRange(long start, long end, int batchSize) {
        List<long[]> slices = new ArrayList<>();
        long cursor = start;
        while (cursor <= end) {
            long sliceEnd = Math.min(cursor + batchSize - 1, end);
            slices.add(new long[]{cursor, sliceEnd});
            cursor = sliceEnd + 1;
        }
        return slices;
    }

    private int resolveMaxRange(SequenceDetectorProperties.SourceConfig sourceConfig) {
        return sourceConfig.getMaxGapRange() != null
                ? sourceConfig.getMaxGapRange()
                : detectorProperties.getDetector().getMaxGapRange();
    }

    private int resolveBatchSize(SequenceDetectorProperties.SourceConfig sourceConfig) {
        return sourceConfig.getBatchSize() != null
                ? sourceConfig.getBatchSize()
                : detectorProperties.getDetector().getBatchSize();
    }

    private String stateKey(SequenceEvent event, SequenceDetectorProperties.SourceConfig sourceConfig) {
        return STATE_KEY_PREFIX + event.getType() + ":" + event.getChainId();
    }

    private long nowEpochMillis() {
        return Instant.now().toEpochMilli();
    }

    private String toHex(long value) {
        return "0x" + Long.toHexString(value);
    }

    @Data
    private static class SequenceState {
        private long expectedNext;
        private long lastSeen;
        private long lastScheduledEnd;
        private long lastUpdated;

        static SequenceState initial(long expectedNext) {
            SequenceState state = new SequenceState();
            state.expectedNext = expectedNext;
            state.lastSeen = expectedNext - 1;
            state.lastUpdated = Instant.now().toEpochMilli();
            return state;
        }
    }
}
