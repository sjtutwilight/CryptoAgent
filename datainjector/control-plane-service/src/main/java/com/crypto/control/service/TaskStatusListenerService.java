package com.crypto.control.service;

import com.crypto.control.dto.TaskStatusUpdate;
import com.crypto.control.model.Task;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import java.time.LocalDateTime;

/**
 * 任务状态监听器服务
 * 监听Kafka中的任务状态更新消息，并更新数据库中的任务状态
 */
@Slf4j
@Service
public class TaskStatusListenerService {
    
    @Autowired
    private TaskSchedulerService taskSchedulerService;
    
    @Autowired
    private MainProcessorService mainProcessorService;
    
    @Autowired
    private ObjectMapper objectMapper;
    
    @Value("${app.task.max-retry-count:3}")
    private int maxRetryCount;
    
    /**
     * 监听任务状态更新消息
     */
    @KafkaListener(topics = "${app.kafka.topics.task-status:tasks.status}")
    @Transactional
    public void handleTaskStatusUpdate(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment acknowledgment) {
        
        try {
            log.debug("接收到任务状态更新消息: topic={}, partition={}, offset={}, message={}", 
                    topic, partition, offset, message);
            
            // 解析消息
            TaskStatusUpdate statusUpdate = objectMapper.readValue(message, TaskStatusUpdate.class);
            
            // 处理状态更新
            processTaskStatusUpdate(statusUpdate);
            
            // 手动确认消息
            acknowledgment.acknowledge();
            
            log.debug("任务状态更新处理完成: taskId={}, status={}", 
                    statusUpdate.getTaskId(), statusUpdate.getStatus());
            
        } catch (Exception e) {
            log.error("处理任务状态更新消息失败: message={}, error={}", message, e.getMessage(), e);
            
            // 即使处理失败也确认消息，避免重复处理
            // 实际生产环境中可能需要死信队列处理
            acknowledgment.acknowledge();
        }
    }
    
    /**
     * 处理任务状态更新
     */
    private void processTaskStatusUpdate(TaskStatusUpdate statusUpdate) {
        String taskId = statusUpdate.getTaskId();
        String status = statusUpdate.getStatus();
        
        log.info("处理任务状态更新: taskId={}, status={}", taskId, status);
        
        try {
            Optional<Task> taskOpt = taskSchedulerService.getTaskByTaskId(taskId);
            if (taskOpt.isEmpty()) {
                log.warn("任务不存在: taskId={}", taskId);
                return;
            }
            
            Task task = taskOpt.get();
            
            // 根据状态处理任务
            switch (status.toUpperCase()) {
                case "SUCCESS":
                    handleSuccess(task, statusUpdate);
                    break;
		case "FAILED":
			if (Boolean.TRUE.equals(statusUpdate.getRetryable()) || statusUpdate.isRetryableError()) {
				handleRetry(task, statusUpdate);
			} else {
				handleFailure(task, statusUpdate);
			}
			break;
                case "RETRY":
                    handleRetry(task, statusUpdate);
                    break;
                case "PROCESSING":
                    handleProcessing(task, statusUpdate);
                    break;
                default:
                    log.warn("未知的任务状态: taskId={}, status={}", taskId, status);
            }
            
        } catch (Exception e) {
            log.error("处理任务状态更新异常: taskId={}, status={}, error={}", 
                    taskId, status, e.getMessage(), e);
        }
    }
    
    /**
     * 处理任务成功
     */
    private void handleSuccess(Task task, TaskStatusUpdate statusUpdate) {
        log.info("任务执行成功: taskId={}", task.getTaskId());
        
        task.setStatus(Task.TaskStatus.SUCCESS);
        task.setStatusCode(statusUpdate.getStatusCode());
        task.setMessage(statusUpdate.getMessage());
        task.setCompletedAt(LocalDateTime.now());
        task.setDurationMs(statusUpdate.getDurationMs());
        task.setDataSize(statusUpdate.getDataSize());
        
        taskSchedulerService.updateTask(task);
    }
    
    /**
     * 处理任务失败
     */
    private void handleFailure(Task task, TaskStatusUpdate statusUpdate) {
        log.warn("任务执行失败: taskId={}, retryCount={}, maxRetryCount={}", 
                task.getTaskId(), task.getRetryCount(), task.getMaxRetryCount());

        // 更新失败信息
        task.setStatusCode(statusUpdate.getStatusCode());
        task.setMessage(statusUpdate.getMessage());
        task.setDurationMs(statusUpdate.getDurationMs());
        task.setStatus(Task.TaskStatus.FAILED);
        task.setCompletedAt(LocalDateTime.now());

        log.error("任务最终失败: taskId={}, retryCount={}", 
                task.getTaskId(), task.getRetryCount());
       
        taskSchedulerService.updateTask(task);
    }
    private void handleRetry(Task task, TaskStatusUpdate statusUpdate) {
        int currentRetries = task.getRetryCount();
        if (currentRetries >= task.getMaxRetryCount()) {
            handleFailure(task, statusUpdate);
            return;
        }

        int nextRetries = currentRetries + 1;
        long delaySeconds = calculateRetryDelay(currentRetries);
        LocalDateTime nextSchedule = LocalDateTime.now().plusSeconds(delaySeconds);

        task.setRetryCount(nextRetries);
        task.setStatus(Task.TaskStatus.PENDING);
        task.setStatusCode(statusUpdate.getStatusCode());
        task.setMessage(statusUpdate.getMessage());
        task.setDurationMs(statusUpdate.getDurationMs());
        task.setScheduledTime(nextSchedule);
        task.setStartedAt(null);
        task.setCompletedAt(null);

        Map<String, Object> metadata = task.getMetadata() != null
                ? new HashMap<>(task.getMetadata())
                : new HashMap<>();
        metadata.put("retryAttempt", nextRetries);
        metadata.put("lastStatusCode", statusUpdate.getStatusCode());
        metadata.put("lastError", statusUpdate.getMessage());
        metadata.put("lastRetryAt", LocalDateTime.now().toString());
        task.setMetadata(metadata);

        taskSchedulerService.updateTask(task);

        log.info("任务重试已排队: taskId={}, retryAttempt={}/{}, nextSchedule={}",
                task.getTaskId(), nextRetries, task.getMaxRetryCount(), nextSchedule);
    }
    /**
     * 处理任务开始执行
     */
    private void handleProcessing(Task task, TaskStatusUpdate statusUpdate) {
        log.info("任务开始执行: taskId={}", task.getTaskId());
        
        task.setStatus(Task.TaskStatus.PROCESSING);
        task.setStartedAt(LocalDateTime.now());
        
        taskSchedulerService.updateTask(task);
    }
    

    
    /**
     * 计算重试延迟时间（指数退避算法）
     */
    private long calculateRetryDelay(int retryCount) {
        // 基础延迟时间：1秒
        long baseDelay = 1;
        
        // 指数退避：2^retryCount * baseDelay
        return baseDelay * (1L << retryCount);
    }

    /**
     * 将字符串状态转换为TaskStatus枚举
     */
    private Task.TaskStatus convertStringToTaskStatus(String status) {
        try {
            return Task.TaskStatus.valueOf(status.toUpperCase());
        } catch (IllegalArgumentException e) {
            log.warn("未知的任务状态: {}, 默认为FAILED", status);
            return Task.TaskStatus.FAILED;
        }
    }
}
