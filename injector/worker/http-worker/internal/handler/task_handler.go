package handler

import (
	"context"
	"fmt"
	"http-worker/internal/client"
	"http-worker/internal/config"
	"http-worker/internal/model"
	"http-worker/internal/producer"
	"http-worker/internal/ratelimit"
	"time"

	"github.com/sirupsen/logrus"
)

// TaskHandler 任务处理器接口
type TaskHandler interface {
	// HandleTask 处理单个任务
	HandleTask(ctx context.Context, task *model.HttpTask) error
	// Close 关闭处理器
	Close() error
}

// taskHandler 任务处理器实现
type taskHandler struct {
	httpClient       client.HTTPClient
	producer         producer.KafkaProducer
	rateLimitManager *ratelimit.TokenBucketManager
	config           *config.Config
	logger           *logrus.Logger
}

// NewTaskHandler 创建新的任务处理器
func NewTaskHandler(
	httpClient client.HTTPClient,
	producer producer.KafkaProducer,
	rateLimitManager *ratelimit.TokenBucketManager,
	config *config.Config,
	logger *logrus.Logger,
) TaskHandler {
	return &taskHandler{
		httpClient:       httpClient,
		producer:         producer,
		rateLimitManager: rateLimitManager,
		config:           config,
		logger:           logger,
	}
}

// HandleTask 处理单个任务
func (th *taskHandler) HandleTask(ctx context.Context, task *model.HttpTask) error {
	startTime := time.Now()
	
	th.logger.WithFields(logrus.Fields{
		"taskId":        task.TaskID,
		"dataSourceUrl": task.Payload.DataSourceURL,
		"method":        task.Payload.Method,
		"dataSourceId":  task.Payload.DataSourceID,
	}).Info("开始处理HTTP任务")
	
	// 1. 检查令牌桶限流
	if !th.checkRateLimit(task) {
		return th.sendTaskStatus(ctx, task.TaskID, "FAILED", "Rate limit exceeded", 0, 0, 429, 0)
	}
	
	// 2. 执行HTTP请求
	response, err := th.httpClient.ExecuteRequest(ctx, task)
	duration := time.Since(startTime).Milliseconds()
	
	if err != nil {
		th.logger.WithFields(logrus.Fields{
			"taskId": task.TaskID,
			"error":  err,
		}).Error("HTTP请求执行失败")
		
		return th.sendTaskStatus(ctx, task.TaskID, "FAILED", err.Error(), duration, 0, 0, 0)
	}
	
	// 3. 处理响应
	return th.processResponse(ctx, task, response, duration)
}

// checkRateLimit 检查限流
func (th *taskHandler) checkRateLimit(task *model.HttpTask) bool {
	dataSourceID := th.getDataSourceID(task)
	
	// 获取数据源配置
	dataSourceConfig, exists := th.config.DataSources[dataSourceID]
	if !exists {
		th.logger.WithField("dataSourceId", dataSourceID).Warn("数据源配置不存在，使用默认限流配置")
		dataSourceConfig = config.DataSourceConfig{
			RateLimit: config.DataSourceRateLimit{
				Interval: 60,
				Weight:   100,
				CostRule: "default",
			},
		}
	}
	
	// 计算令牌桶参数
	capacity := dataSourceConfig.RateLimit.Weight
	refillRate := th.calculateRefillRate(dataSourceConfig.RateLimit)
	
	// 获取令牌桶
	bucket := th.rateLimitManager.GetBucket(dataSourceID, capacity, refillRate)
	
	// 尝试获取令牌（默认消耗1个令牌）
	cost := th.calculateCost(task, dataSourceConfig.RateLimit.CostRule)
	success := bucket.TakeToken(cost)
	
	th.logger.WithFields(logrus.Fields{
		"dataSourceId":   dataSourceID,
		"available":      bucket.AvailableTokens(),
		"capacity":       bucket.Capacity(),
		"cost":          cost,
		"rateLimitPass": success,
	}).Debug("限流检查结果")
	
	return success
}

// calculateRefillRate 计算令牌补充速率
func (th *taskHandler) calculateRefillRate(rateLimitConfig config.DataSourceRateLimit) float64 {
	// 根据配置计算每次补充的令牌数
	// 例如：60秒限流60，每200ms补充 60 / (60 * 1000 / 200) = 0.2个令牌
	intervalMs := float64(rateLimitConfig.Interval * 1000)
	refillIntervalMs := float64(th.config.RateLimit.RefillIntervalMs)
	
	// 总的补充次数 = 时间窗口 / 补充间隔
	totalRefillTimes := intervalMs / refillIntervalMs
	
	// 每次补充的令牌数 = 总权重 / 总补充次数
	refillRate := float64(rateLimitConfig.Weight) / totalRefillTimes
	
	th.logger.WithFields(logrus.Fields{
		"weight":           rateLimitConfig.Weight,
		"interval":         rateLimitConfig.Interval,
		"refillIntervalMs": refillIntervalMs,
		"totalRefillTimes": totalRefillTimes,
		"refillRate":       refillRate,
	}).Debug("计算令牌补充速率")
	
	return refillRate
}

// calculateCost 计算请求成本
func (th *taskHandler) calculateCost(task *model.HttpTask, costRule string) int {
	// 根据不同的成本规则计算令牌消耗
	switch costRule {
	case "default":
		return 1
	case "read":
		return 1
	case "write":
		return 5
	default:
		// 可以根据请求的复杂度计算成本
		if task.Payload.Method == "POST" {
			return 2
		}
		return 1
	}
}

// processResponse 处理HTTP响应
func (th *taskHandler) processResponse(ctx context.Context, task *model.HttpTask, response *model.HTTPResponse, duration int64) error {
	// 检查响应状态码
	if response.StatusCode >= 200 && response.StatusCode < 300 {
		// 成功响应，发送数据到数据topic
		if err := th.sendDataRecord(ctx, task, response); err != nil {
			th.logger.WithFields(logrus.Fields{
				"taskId": task.TaskID,
				"error":  err,
			}).Error("发送数据记录失败")
		}
		
		// 发送成功状态
		return th.sendTaskStatus(ctx, task.TaskID, "SUCCESS", "Request completed successfully", 
			duration, response.Size, response.StatusCode, 0)
			
	} else if response.IsRetryable() {
		// 可重试错误
		th.logger.WithFields(logrus.Fields{
			"taskId":     task.TaskID,
			"statusCode": response.StatusCode,
		}).Warn("收到可重试错误")
		
		return th.sendTaskStatus(ctx, task.TaskID, "RETRY", 
			fmt.Sprintf("Retryable error: HTTP %d", response.StatusCode),
			duration, response.Size, response.StatusCode, 1)
			
	} else {
		// 不可重试错误
		th.logger.WithFields(logrus.Fields{
			"taskId":     task.TaskID,
			"statusCode": response.StatusCode,
		}).Error("收到不可重试错误")
		
		return th.sendTaskStatus(ctx, task.TaskID, "FAILED", 
			fmt.Sprintf("Non-retryable error: HTTP %d", response.StatusCode),
			duration, response.Size, response.StatusCode, 0)
	}
}

// sendDataRecord 发送数据记录
func (th *taskHandler) sendDataRecord(ctx context.Context, task *model.HttpTask, response *model.HTTPResponse) error {
	dataSourceID := th.getDataSourceID(task)
	
	// 构建数据记录
	dataRecord := &model.DataRecord{
		TaskID:       task.TaskID,
		DataSourceID: dataSourceID,
		Timestamp:    time.Now(),
		Data:         response.Body,
		Metadata: map[string]interface{}{
			"statusCode":    response.StatusCode,
			"headers":       response.Headers,
			"duration":      response.Duration.Milliseconds(),
			"size":          response.Size,
			"dataSourceUrl": task.Payload.DataSourceURL,
			"method":        task.Payload.Method,
		},
	}
	
	// 序列化数据记录
	dataJSON, err := dataRecord.ToJSON()
	if err != nil {
		return fmt.Errorf("序列化数据记录失败: %w", err)
	}
	
	// 发送到数据topic
	return th.producer.SendData(ctx, dataSourceID, dataJSON)
}

// sendTaskStatus 发送任务状态
func (th *taskHandler) sendTaskStatus(ctx context.Context, taskID, status, message string, 
	duration int64, dataSize, statusCode, retryCount int) error {
	
	taskStatus := &model.TaskStatus{
		TaskID:     taskID,
		Status:     status,
		Message:    message,
		Timestamp:  time.Now(),
		Duration:   duration,
		StatusCode: statusCode,
		DataSize:   dataSize,
		RetryCount: retryCount,
	}
	
	// 序列化任务状态
	statusJSON, err := taskStatus.ToJSON()
	if err != nil {
		return fmt.Errorf("序列化任务状态失败: %w", err)
	}
	
	// 发送到状态topic
	return th.producer.SendTaskStatus(ctx, taskID, statusJSON)
}

// getDataSourceID 获取数据源ID
func (th *taskHandler) getDataSourceID(task *model.HttpTask) string {
	// 如果payload中指定了dataSourceId，使用指定的值
	if task.Payload.DataSourceID != "" {
		return task.Payload.DataSourceID
	}
	
	// 否则根据URL推断数据源ID
	url := task.Payload.DataSourceURL
	
	if contains(url, "localhost:8080") || contains(url, "mock") {
		return "mock-ethereum"
	} else if contains(url, "coinmarketcap") {
		return "coinmarketcap"
	} else if contains(url, "binance") {
		return "binance"
	} else {
		return "unknown"
	}
}

// contains 检查字符串是否包含子字符串
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) && 
		(findSubstring(s, substr) >= 0))
}

// findSubstring 查找子字符串
func findSubstring(s, substr string) int {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}

// Close 关闭任务处理器
func (th *taskHandler) Close() error {
	th.logger.Info("关闭任务处理器")
	return nil
}