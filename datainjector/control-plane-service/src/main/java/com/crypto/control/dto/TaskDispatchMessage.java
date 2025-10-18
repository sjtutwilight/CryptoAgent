package com.crypto.control.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.Map;

/**
 * 通用任务调度消息结构，发送给统一 Worker。
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class TaskDispatchMessage {

    @JsonProperty("taskId")
    private String taskId;

    @JsonProperty("taskType")
    private String taskType;

    @JsonProperty("payload")
    private Map<String, Object> payload;

    @JsonProperty("metadata")
    private Map<String, Object> metadata;

    public static TaskDispatchMessage fromTask(com.crypto.control.model.Task task) {
        Map<String, Object> payload = task.getPayload() != null
                ? new HashMap<>(task.getPayload())
                : new HashMap<>();
        payload.putIfAbsent("dataSourceId", task.getDataSourceId());

        String taskType = task.getTaskType();
        if (taskType == null || taskType.isBlank()) {
            taskType = "http_jsonrpc";
        }

        Map<String, Object> metadata = task.getMetadata() != null
                ? new HashMap<>(task.getMetadata())
                : new HashMap<>();
        metadata.putIfAbsent("datasourceId", task.getDataSourceId());
        metadata.putIfAbsent("priority", task.getPriority());
        metadata.putIfAbsent("cost", task.getCost());
        if (task.getScheduledTime() != null) {
            metadata.putIfAbsent("scheduledTime", task.getScheduledTime().toString());
        }
        metadata.putIfAbsent("taskType", taskType);
        metadata.putIfAbsent("enqueuedAt", OffsetDateTime.now());

        return TaskDispatchMessage.builder()
                .taskId(task.getTaskId())
                .taskType(taskType)
                .payload(payload)
                .metadata(metadata)
                .build();
    }

    /**
     * 构造发送给 worker 的扁平化参数。
     */
    public Map<String, Object> toCommandPayload() {
        Map<String, Object> command = new HashMap<>(payload);
        command.put("taskId", taskId);
        command.put("taskType", taskType);
        if (metadata != null && !metadata.isEmpty()) {
            command.put("metadata", metadata);
        }
        return command;
    }
}
