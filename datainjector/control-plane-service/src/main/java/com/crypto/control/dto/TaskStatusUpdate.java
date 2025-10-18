package com.crypto.control.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;
import lombok.Builder;
import java.time.Instant;

/**
 * 任务状态更新DTO
 * 从tasks.status topic接收的消息格式
 * 来自HTTP Worker的状态上报
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TaskStatusUpdate {
    
    @JsonProperty("taskId")
    private String taskId;
    
    @JsonProperty("status")
    private String status;
    
    @JsonProperty("statusCode")
    private Integer statusCode;
    
    @JsonProperty("message")
    private String message;
    
    @JsonProperty("durationMs")
    private Long durationMs;
    
    @JsonProperty("dataSize")
    private Integer dataSize;
    
    @JsonProperty("retryable")
    private Boolean retryable;
    
    @JsonProperty("timestamp")
    private Instant timestamp;
    
    /**
     * 判断状态是否为成功
     */
    public boolean isSuccess() {
        return "SUCCESS".equalsIgnoreCase(status) || 
               (statusCode != null && statusCode >= 200 && statusCode < 300);
    }
    
    /**
     * 判断错误是否可重试
     * 基于HTTP状态码判断
     */
    public boolean isRetryableError() {
        if (retryable != null) {
            return retryable;
        }
        
        if (statusCode == null) {
            return false;
        }
        
        // 可重试的HTTP状态码
        return statusCode == 429 ||  // Too Many Requests
               statusCode == 502 ||  // Bad Gateway
               statusCode == 503 ||  // Service Unavailable
               statusCode == 504 ||  // Gateway Timeout
               statusCode >= 500;    // 其他5xx错误一般可重试
    }
}