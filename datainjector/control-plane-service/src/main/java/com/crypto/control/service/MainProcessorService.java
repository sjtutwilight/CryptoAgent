package com.crypto.control.service;

import com.crypto.control.config.DataSourceConfigProperties;
import com.crypto.control.dto.TaskCreateRequest;
import com.crypto.control.dto.TaskResponse;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

/**
 * 主处理器服务
 * 协调限流器、任务调度器等组件
 * 实现完整的任务处理工作流
 */
@Slf4j
@Service
public class MainProcessorService {
    
    @Autowired
    private RateLimiterService rateLimiterService;
    
    @Autowired
    private TaskSchedulerService taskSchedulerService;

    
    @Autowired
    private DataSourceConfigProperties dataSourceConfigProperties;
    
    /**
     * 处理任务创建请求
     * 完整的工作流：验证 -> 限流 -> 调度
     */
    public TaskProcessResult processTask(TaskCreateRequest request) {
        try { 
            DataSourceConfigProperties.DataSourceConfig dataSourceConfig = 
                dataSourceConfigProperties.getDataSourceConfig(request.getDataSourceId());

            // 1. 验证数据源配置
            if (dataSourceConfig == null) {
                log.error("未找到数据源配置: dataSourceId={}", request.getDataSourceId());
                return TaskProcessResult.failed("未找到数据源配置: " + request.getDataSourceId());
            }
    
            if (!dataSourceConfig.getEnabled()) {
                return TaskProcessResult.failed("数据源已禁用: " + request.getDataSourceId());
            }
            
            // 确定调度时间
            LocalDateTime scheduledTime = request.getScheduledTime() != null 
                ? request.getScheduledTime() 
                : LocalDateTime.now();
            
            request.setScheduledTime(scheduledTime);            
            // 2. 限流检查
            int cost = request.getCost() != null ? request.getCost() : 1;
            RateLimiterService.RateLimitResult rateLimitResult = 
                rateLimiterService.checkRateLimit(dataSourceConfig, cost);
            
            if (!rateLimitResult.isAllowed()) {
                // 计算下一个可用时间并重新调度
                LocalDateTime nextAvailableTime = rateLimitResult.getResetTime();
                request.setScheduledTime(nextAvailableTime);
                
                log.debug("任务因限流延迟调度: dataSourceId={}, 延迟到: {}", 
                        request.getDataSourceId(), nextAvailableTime);
            }
            
            normalizeRequest(request, dataSourceConfig);

            // 3. 提交任务调度
            TaskResponse taskResponse = taskSchedulerService.createTask(request);
            
            return TaskProcessResult.success(taskResponse);
            
        } catch (Exception e) {
            log.error("任务处理失败: dataSourceId={}, error={}", 
                    request.getDataSourceId(), e.getMessage(), e);
            return TaskProcessResult.failed("任务处理失败: " + e.getMessage());
        }
    }

    private void normalizeRequest(TaskCreateRequest request, DataSourceConfigProperties.DataSourceConfig dataSourceConfig) {
        if (request.getTaskType() == null || request.getTaskType().isBlank()) {
            request.setTaskType("generic");
        }

        Map<String, Object> payload = request.getPayload() != null
                ? new HashMap<>(request.getPayload())
                : new HashMap<>();
        payload.putIfAbsent("dataSourceId", request.getDataSourceId());
        request.setPayload(payload);

        Map<String, Object> metadata = request.getMetadata() != null
                ? new HashMap<>(request.getMetadata())
                : new HashMap<>();
        metadata.putIfAbsent("origin", "control-plane");
        metadata.putIfAbsent("requestedAt", LocalDateTime.now().toString());
        request.setMetadata(metadata);
    }
    

    /**
     * 任务处理结果
     */
    public static class TaskProcessResult {
        private final boolean success;
        private final TaskResponse taskResponse;
        private final String errorMessage;        
        private TaskProcessResult(boolean success, TaskResponse taskResponse, String errorMessage) {
            this.success = success;
            this.taskResponse = taskResponse;
            this.errorMessage = errorMessage;
        }
        
        public static TaskProcessResult success(TaskResponse taskResponse) {
            return new TaskProcessResult(true, taskResponse, null);
        }
        
        public static TaskProcessResult failed(String errorMessage) {
            return new TaskProcessResult(false, null, errorMessage);
        }
        
        public boolean isSuccess() { return success; }
        public TaskResponse getTaskResponse() { return taskResponse; }
        public String getErrorMessage() { return errorMessage; }
    }
}
