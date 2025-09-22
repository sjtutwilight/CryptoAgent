package com.crypto.control.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Max;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;
import lombok.Builder;

import java.time.LocalDateTime;
import java.util.Map;

/**
 * 任务创建请求DTO
 * 与HTTP Worker消息格式兼容
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder

public class TaskCreateRequest {
    
    @NotBlank(message = "数据源ID不能为空")
    @JsonProperty("dataSourceId")
    private String dataSourceId;
    
    @NotBlank(message = "目标URL不能为空")
    @JsonProperty("baseUrl")
    private String baseUrl;
    
    @NotBlank(message = "请求方法不能为空")
    @JsonProperty("method")
    private String method;
    
    @JsonProperty("params")
    private Map<String, Object> params;
    
    @JsonProperty("headers")
    private Map<String, String> headers;
    
    @JsonProperty("apiKey")
    private String apiKey;
    
    @JsonProperty("scheduledTime")
    private LocalDateTime scheduledTime;
    
    @Min(value = 1, message = "请求成本必须大于0")
    @JsonProperty("cost")
    @Builder.Default
    private Integer cost = 1;
    
    @Min(value = 1, message = "优先级必须大于0")
    @Max(value = 10, message = "优先级不能超过10")
    @JsonProperty("priority")
    @Builder.Default
    private Integer priority = 5;
    
    @Min(value = 0, message = "最大重试次数不能为负")
    @Max(value = 10, message = "最大重试次数不能超过10")
    @JsonProperty("maxRetryCount")
    private Integer maxRetryCount;
}