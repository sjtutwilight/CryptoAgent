package com.crypto.control.controller;

import com.crypto.control.dto.TaskCreateRequest;
import com.crypto.control.dto.TaskResponse;
import com.crypto.control.service.MainProcessorService;
import com.crypto.control.service.TaskSchedulerService;
import lombok.extern.slf4j.Slf4j;
import lombok.Data;
import lombok.AllArgsConstructor;
import lombok.Builder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import jakarta.validation.Valid;
import java.util.Optional;

/**
 * 任务管理REST API控制器
 */
@Slf4j
@RestController
@RequestMapping("/tasks")
@Validated
public class TaskController {
    
    @Autowired
    private MainProcessorService mainProcessorService;
    
    @Autowired
    private TaskSchedulerService taskSchedulerService;
    
    /**
     * 创建单个任务
     */
    @PostMapping
    public ResponseEntity<ApiResponse<TaskResponse>> createTask(
            @Valid @RequestBody TaskCreateRequest request) {
        
        try {
            log.info("接收到任务创建请求: dataSource={}, method={}", 
                    request.getDataSourceId(), request.getMethod());
            
            MainProcessorService.TaskProcessResult result = 
                mainProcessorService.processTask(request);
            
            if (result.isSuccess()) {
                return ResponseEntity.ok(ApiResponse.success(result.getTaskResponse(), "任务创建成功"));
            } else {
                return ResponseEntity.badRequest()
                    .body(ApiResponse.error(result.getErrorMessage()));
            }
            
        } catch (Exception e) {
            log.error("创建任务失败: {}", e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(ApiResponse.error("内部服务器错误: " + e.getMessage()));
        }
    }
    
    /**
     * 根据任务ID查询任务
     */
    @GetMapping("/{taskId}")
    public ResponseEntity<ApiResponse<TaskResponse>> getTask(@PathVariable String taskId) {
        try {
            Optional<TaskResponse> task = taskSchedulerService.getTask(taskId);
            
            if (task.isPresent()) {
                return ResponseEntity.ok(ApiResponse.success(task.get()));
            } else {
                return ResponseEntity.notFound().build();
            }
            
        } catch (Exception e) {
            log.error("查询任务失败: taskId={}, error={}", taskId, e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(ApiResponse.error("查询任务失败: " + e.getMessage()));
        }
    }
    
    /**
     * 获取任务统计信息
     */
    @GetMapping("/statistics")
    public ResponseEntity<ApiResponse<TaskSchedulerService.TaskStatistics>> getTaskStatistics() {
        try {
            TaskSchedulerService.TaskStatistics statistics = 
                taskSchedulerService.getTaskStatistics();
            
            return ResponseEntity.ok(ApiResponse.success(statistics));
            
        } catch (Exception e) {
            log.error("获取任务统计信息失败: {}", e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(ApiResponse.error("获取统计信息失败: " + e.getMessage()));
        }
    }
    
    /**
     * API响应包装类
     */
    @Data
    @AllArgsConstructor
    @Builder
    public static class ApiResponse<T> {
        private boolean success;
        private T data;
        private String message;
        private long timestamp;
        
        public ApiResponse(boolean success, T data, String message) {
            this.success = success;
            this.data = data;
            this.message = message;
            this.timestamp = System.currentTimeMillis();
        }
        
        public static <T> ApiResponse<T> success(T data) {
            return new ApiResponse<>(true, data, "操作成功");
        }
        
        public static <T> ApiResponse<T> success(T data, String message) {
            return new ApiResponse<>(true, data, message);
        }
        
        public static <T> ApiResponse<T> error(String message) {
            return new ApiResponse<>(false, null, message);
        }
        
        // Getters and Setters
        public boolean isSuccess() { return success; }
        public void setSuccess(boolean success) { this.success = success; }
        public T getData() { return data; }
        public void setData(T data) { this.data = data; }
        public String getMessage() { return message; }
        public void setMessage(String message) { this.message = message; }
        public long getTimestamp() { return timestamp; }
        public void setTimestamp(long timestamp) { this.timestamp = timestamp; }
    }
}