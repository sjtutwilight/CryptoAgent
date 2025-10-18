package com.crypto.control.dto;

import com.crypto.control.model.Task;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;
import lombok.Builder;

import java.time.LocalDateTime;
import java.util.Map;

/**
 * 任务响应DTO
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TaskResponse {
    
    @JsonProperty("taskId")
    private String taskId;
    
    @JsonProperty("dataSourceId")
    private String dataSourceId;
    
    @JsonProperty("scheduledTime")
    private LocalDateTime scheduledTime;    
    @JsonProperty("status")
    private Task.TaskStatus status;
    
    @JsonProperty("retryCount")
    private Integer retryCount;
    
    @JsonProperty("maxRetryCount")
    private Integer maxRetryCount;
    
    @JsonProperty("statusCode")
    private Integer statusCode;
    
    @JsonProperty("message")
    private String message;
    
    @JsonProperty("createdAt")
    private LocalDateTime createdAt;
    
    @JsonProperty("cost")
    private Integer cost;
    
    @JsonProperty("priority")
    private Integer priority;

    @JsonProperty("taskType")
    private String taskType;

    @JsonProperty("payload")
    private Map<String, Object> payload;

    @JsonProperty("metadata")
    private Map<String, Object> metadata;
    
    /**
     * 从Task实体创建TaskResponse
     */
    public static TaskResponse fromTask(Task task) {
        return TaskResponse.builder()
                .taskId(task.getTaskId())
                .dataSourceId(task.getDataSourceId())
                .scheduledTime(task.getScheduledTime())
                .status(task.getStatus())
                .retryCount(task.getRetryCount())
                .maxRetryCount(task.getMaxRetryCount())
                .statusCode(task.getStatusCode())
                .message(task.getMessage())
                .createdAt(task.getCreatedAt())
                .cost(task.getCost())
                .priority(task.getPriority())
                .taskType(task.getTaskType())
                .payload(task.getPayload())
                .metadata(task.getMetadata())
                .build();
    }
}
