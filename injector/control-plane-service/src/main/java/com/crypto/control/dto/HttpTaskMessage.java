package com.crypto.control.dto;

import com.crypto.control.model.Task;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;
import lombok.Builder;

import java.util.Map;

/**
 * HTTP任务消息DTO
 * 发送到http.tasks topic的消息格式
 * 与HTTP Worker期望的格式完全兼容
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class HttpTaskMessage {
    
    @JsonProperty("taskId")
    private String taskId;
    
    @JsonProperty("payload")
    private TaskPayload payload;
    
    /**
     * 任务载荷
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class TaskPayload {
        
        @JsonProperty("dataSourceUrl")
        private String dataSourceUrl;
        
        @JsonProperty("method")
        private String method;
        
        @JsonProperty("params")
        private Map<String, Object> params;
        
        @JsonProperty("apikey")
        private String apikey;
        
        @JsonProperty("headers")
        private Map<String, String> headers;
        @JsonProperty("dataSourceId")
        private String dataSourceId;
    }
    
    /**
     * 从Task实体创建HttpTaskMessage
     */
    public static HttpTaskMessage fromTask(Task task) {
        TaskPayload payload = TaskPayload.builder()
                .dataSourceUrl(task.getBaseUrl())
                .method(task.getMethod())  // 保持原始大小写
                .params(task.getParams())
                .apikey(task.getApiKey())
                .headers(task.getHeaders())
                .dataSourceId(task.getDataSourceId())
                .build();
                
        return HttpTaskMessage.builder()
                .taskId(task.getTaskId())
                .payload(payload)
                .build();
    }
}