package com.crypto.control.service;

import com.crypto.control.dto.TaskDispatchMessage;
import com.crypto.control.model.Task;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * 定时器生产者服务
 * 定期扫描数据库中的待执行任务，并发送到Kafka
 */
@Slf4j
@Service
public class TimerProducerService {
    
    @Autowired
    private TaskSchedulerService taskSchedulerService;
    
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;
    
    @Autowired
    private ObjectMapper objectMapper;
    
    @Value("${app.kafka.topics.http-tasks:http.tasks}")
    private String httpTasksTopic;
    
    @Value("${app.timer.max-tasks-per-scan:1000}")
    private int maxTasksPerScan;
    
    @Value("${app.timer.advance-schedule-time:5}")
    private int advanceScheduleTimeSeconds;
    
    /**
     * 定期扫描并发送待执行任务
     * 根据配置的间隔时间执行
     */
    @Scheduled(fixedDelayString = "${app.timer.scan-interval:1000}")
    public void scanAndSendTasks() {
        try {
            log.debug("开始扫描待执行任务...");
            
            // 1. 获取待执行任务
            LocalDateTime now = LocalDateTime.now().plusSeconds(advanceScheduleTimeSeconds);
            List<Task> pendingTasks = taskSchedulerService.getPendingTasks(now, maxTasksPerScan);
            
            // 2. 处理待执行任务
            if (!pendingTasks.isEmpty()) {
                log.info("找到 {} 个待执行任务", pendingTasks.size());
                processTasks(pendingTasks);
            } else {
                log.debug("没有找到待执行的任务");
            }
            
        } catch (Exception e) {
            log.error("扫描任务时发生错误: {}", e.getMessage(), e);
        }
    }
    
    /**
     * 处理任务列表
     */
    private void processTasks(List<Task> tasks) {
        LocalDateTime now = LocalDateTime.now();
        
        for (Task task : tasks) {
            try {
                // 检查是否到了执行时间
                if (task.getScheduledTime().isAfter(now)) {
                    log.debug("任务尚未到执行时间: taskId={}, scheduledTime={}, now={}", 
                            task.getTaskId(), task.getScheduledTime(), now);
                    continue;
                }
                
                // 标记任务为处理中
                taskSchedulerService.markTaskAsProcessing(task.getTaskId());
                
                // 发送任务到Kafka
                sendTaskToKafka(task);
                
            } catch (Exception e) {
                log.error("处理任务失败: taskId={}, error={}", task.getTaskId(), e.getMessage(), e);
            }
        }
    }
    
    /**
     * 发送任务到Kafka
     */
    private void sendTaskToKafka(Task task) {
        try {
            TaskDispatchMessage dispatchMessage = TaskDispatchMessage.fromTask(task);
            
            // 序列化为JSON
            String messageJson = objectMapper.writeValueAsString(dispatchMessage.toCommandPayload());
            
            // 详细日志输出，用于调试消息格式
            log.info("发送任务到Kafka: taskId={}, taskType={}, payloadKeys={}, kafkaMessage={}", 
                    task.getTaskId(),
                    task.getTaskType(),
                    dispatchMessage.getPayload().keySet(),
                    messageJson);
            
            // 发送到Kafka
            CompletableFuture<SendResult<String, String>> future = 
                kafkaTemplate.send(httpTasksTopic, task.getTaskId(), messageJson);
            
            // 异步处理发送结果
            future.whenComplete((result, exception) -> {
                if (exception == null) {
                    log.info("任务发送成功: taskId={}, topic={}", 
                            task.getTaskId(), httpTasksTopic);
                } else {
                    log.error("任务发送失败: taskId={}, error={}", 
                            task.getTaskId(), exception.getMessage(), exception);
                }
            });
            
        } catch (JsonProcessingException e) {
            log.error("序列化任务消息失败: taskId={}, error={}", task.getTaskId(), e.getMessage(), e);
            throw new RuntimeException("序列化任务消息失败", e);
        }
    }
    
    /**
     * 获取定时器状态信息
     */
    public TimerStatus getTimerStatus() {
        try {
            TaskSchedulerService.TaskStatistics stats = taskSchedulerService.getTaskStatistics();
            
            return new TimerStatus(
                    true, // enabled
                    stats.pendingTasks(),
                    stats.retryTasks(),
                    stats.processingTasks(),
                    LocalDateTime.now()
            );
            
        } catch (Exception e) {
            log.error("获取定时器状态失败: {}", e.getMessage(), e);
            return new TimerStatus(false, 0, 0, 0, LocalDateTime.now());
        }
    }
    
    /**
     * 定时器状态信息
     */
    public record TimerStatus(
            boolean enabled,
            long pendingTasks,
            long retryTasks,
            long processingTasks,
            LocalDateTime lastScanTime
    ) {}
}
