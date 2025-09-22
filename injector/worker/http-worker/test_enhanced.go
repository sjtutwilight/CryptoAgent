package main

import (
	"context"
	"encoding/json"
	"fmt"
	"http-worker/internal/client"
	"http-worker/internal/config"
	"http-worker/internal/handler"
	"http-worker/internal/model"
	"http-worker/internal/ratelimit"
	"log"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

// EnhancedMetrics 增强的测试指标
type EnhancedMetrics struct {
	mu                 sync.RWMutex
	totalRequests      int
	successRequests    int
	failedRequests     int
	rateLimitedCount   int
	faultInjectedCount int
	statusCodes        map[int]int
	errorTypes         map[string]int
}

func NewEnhancedMetrics() *EnhancedMetrics {
	return &EnhancedMetrics{
		statusCodes: make(map[int]int),
		errorTypes:  make(map[string]int),
	}
}

func (em *EnhancedMetrics) RecordRequest(statusCode int, errorType string) {
	em.mu.Lock()
	defer em.mu.Unlock()

	em.totalRequests++
	em.statusCodes[statusCode]++

	if errorType != "" {
		em.errorTypes[errorType]++
	}

	// 分类记录
	if statusCode == 200 {
		em.successRequests++
	} else {
		em.failedRequests++

		if statusCode == 429 {
			em.rateLimitedCount++
		}

		if statusCode >= 400 && statusCode < 600 {
			em.faultInjectedCount++
		}
	}
}

func (em *EnhancedMetrics) PrintDetailedStats() {
	em.mu.RLock()
	defer em.mu.RUnlock()

	fmt.Printf("\n📊 详细测试统计\n")
	fmt.Printf("=======================\n")
	fmt.Printf("总请求数: %d\n", em.totalRequests)
	fmt.Printf("成功请求: %d (%.2f%%)\n", em.successRequests, float64(em.successRequests)/float64(em.totalRequests)*100)
	fmt.Printf("失败请求: %d (%.2f%%)\n", em.failedRequests, float64(em.failedRequests)/float64(em.totalRequests)*100)
	fmt.Printf("限流拦截: %d (%.2f%%)\n", em.rateLimitedCount, float64(em.rateLimitedCount)/float64(em.totalRequests)*100)
	fmt.Printf("故障注入: %d (%.2f%%)\n", em.faultInjectedCount, float64(em.faultInjectedCount)/float64(em.totalRequests)*100)

	fmt.Printf("\n状态码分布:\n")
	for code, count := range em.statusCodes {
		fmt.Printf("  %d: %d次 (%.2f%%)\n", code, count, float64(count)/float64(em.totalRequests)*100)
	}

	fmt.Printf("\n错误类型分布:\n")
	for errorType, count := range em.errorTypes {
		fmt.Printf("  %s: %d次\n", errorType, count)
	}
	fmt.Printf("=======================\n")
}

// EnhancedTestProducer 增强的测试生产者
type EnhancedTestProducer struct {
	logger         *logrus.Logger
	metrics        *EnhancedMetrics
	mu             sync.RWMutex
	dataMessages   []string
	statusMessages []string
}

func NewEnhancedTestProducer(logger *logrus.Logger, metrics *EnhancedMetrics) *EnhancedTestProducer {
	return &EnhancedTestProducer{
		logger:         logger,
		metrics:        metrics,
		dataMessages:   make([]string, 0),
		statusMessages: make([]string, 0),
	}
}

func (etp *EnhancedTestProducer) SendMessage(ctx context.Context, topic string, key string, value string) error {
	etp.mu.Lock()
	defer etp.mu.Unlock()

	etp.dataMessages = append(etp.dataMessages, value)

	// 详细分析数据记录
	var dataRecord model.DataRecord
	if err := json.Unmarshal([]byte(value), &dataRecord); err == nil {
		if metadata, ok := dataRecord.Metadata["statusCode"]; ok {
			statusCode := int(metadata.(float64))
			etp.metrics.RecordRequest(statusCode, "")

			if statusCode >= 400 {
				etp.logger.WithFields(logrus.Fields{
					"statusCode": statusCode,
					"taskId":     dataRecord.TaskID,
					"topic":      topic,
				}).Info("🔴 故障注入检测")
			}
		}
	}

	return nil
}

func (etp *EnhancedTestProducer) SendTaskStatus(ctx context.Context, taskID string, status string) error {
	etp.mu.Lock()
	defer etp.mu.Unlock()

	etp.statusMessages = append(etp.statusMessages, status)

	// 详细分析任务状态
	var taskStatus model.TaskStatus
	if err := json.Unmarshal([]byte(status), &taskStatus); err == nil {
		errorType := ""
		if taskStatus.Status == "FAILED" {
			errorType = "FAILED"
		} else if taskStatus.Status == "RETRY" {
			errorType = "RETRY"
		}

		etp.metrics.RecordRequest(taskStatus.StatusCode, errorType)

		if taskStatus.StatusCode == 429 {
			etp.logger.WithFields(logrus.Fields{
				"taskId":     taskID,
				"statusCode": taskStatus.StatusCode,
			}).Info("🟡 限流检测")
		}

		if taskStatus.StatusCode >= 400 && taskStatus.StatusCode < 600 {
			etp.logger.WithFields(logrus.Fields{
				"taskId":     taskID,
				"statusCode": taskStatus.StatusCode,
				"message":    taskStatus.Message,
			}).Info("🔴 故障状态检测")
		}
	}

	return nil
}

func (etp *EnhancedTestProducer) SendData(ctx context.Context, dataSourceID string, data string) error {
	return etp.SendMessage(ctx, fmt.Sprintf("data.%s", dataSourceID), "", data)
}

func (etp *EnhancedTestProducer) Close() error {
	return nil
}

func (etp *EnhancedTestProducer) GetMessageCounts() (int, int) {
	etp.mu.RLock()
	defer etp.mu.RUnlock()
	return len(etp.dataMessages), len(etp.statusMessages)
}

func main() {
	// 创建日志器
	logger := logrus.New()
	logger.SetLevel(logrus.InfoLevel)
	logger.SetFormatter(&logrus.TextFormatter{
		FullTimestamp:   true,
		TimestampFormat: "15:04:05",
	})

	fmt.Println("🚀 HTTP Worker 增强测试")
	fmt.Println("=======================")
	fmt.Println("这个测试将更准确地检测:")
	fmt.Println("1. 故障注入的各种类型")
	fmt.Println("2. 限流机制的触发")
	fmt.Println("3. 错误状态的正确映射")
	fmt.Println("=======================")

	// 加载配置
	if err := config.Load("configs/config.yaml"); err != nil {
		log.Fatalf("加载配置失败: %v", err)
	}
	cfg := config.Get()

	// 创建增强的测试指标
	metrics := NewEnhancedMetrics()

	// 创建组件
	httpClient := client.NewHTTPClient(&cfg.HTTP.Client)
	testProducer := NewEnhancedTestProducer(logger, metrics)

	refillInterval := time.Duration(cfg.RateLimit.RefillIntervalMs) * time.Millisecond
	rateLimitManager := ratelimit.NewTokenBucketManager(refillInterval)

	taskHandler := handler.NewTaskHandler(
		httpClient,
		testProducer,
		rateLimitManager,
		cfg,
		logger,
	)

	// 创建测试任务模板
	taskTemplates := []model.TaskPayload{
		{
			DataSourceURL: "http://localhost:8090",
			Method:        "GET",
			Params: map[string]interface{}{
				"method":  "eth_getBlockByNumber",
				"block":   "latest",
				"full_tx": "false",
			},
			DataSourceID: "mock-ethereum",
		},
		{
			DataSourceURL: "http://localhost:8090",
			Method:        "GET",
			Params: map[string]interface{}{
				"method": "eth_blockNumber",
			},
			DataSourceID: "mock-ethereum",
		},
	}

	// 启动增强测试
	ctx := context.Background()
	testDuration := 20 * time.Second
	requestInterval := 50 * time.Millisecond // 更高频率以触发限流

	fmt.Printf("⏰ 开始增强测试 (持续时间: %v, 请求间隔: %v)\n", testDuration, requestInterval)
	fmt.Printf("📈 预期每秒请求数: %.1f\n", float64(time.Second)/float64(requestInterval))

	startTime := time.Now()
	ticker := time.NewTicker(requestInterval)
	defer ticker.Stop()

	taskCounter := 0

	// 统计显示定时器
	statsTicker := time.NewTicker(10 * time.Second)
	defer statsTicker.Stop()

	for {
		select {
		case <-ticker.C:
			taskCounter++

			// 选择任务模板
			template := taskTemplates[taskCounter%len(taskTemplates)]

			// 创建任务
			task := &model.HttpTask{
				TaskID:  fmt.Sprintf("enhanced-test-%d", taskCounter),
				Payload: template,
			}

			// 异步处理任务
			go func(t *model.HttpTask) {
				if err := taskHandler.HandleTask(ctx, t); err != nil {
					logger.WithError(err).WithField("taskId", t.TaskID).Error("任务处理失败")
				}
			}(task)

		case <-statsTicker.C:
			// 显示当前统计
			metrics.PrintDetailedStats()
			dataCount, statusCount := testProducer.GetMessageCounts()
			fmt.Printf("📨 消息数量 - 数据: %d, 状态: %d\n", dataCount, statusCount)
			fmt.Printf("⏱️ 运行时间: %v\n", time.Since(startTime))

		case <-time.After(testDuration):
			fmt.Printf("\n⏰ 测试时间结束 (已运行 %v)\n", testDuration)
			goto cleanup
		}

		// 检查是否超过测试时间
		if time.Since(startTime) > testDuration {
			break
		}
	}

cleanup:
	// 等待所有任务完成
	fmt.Println("⏳ 等待所有任务完成...")
	time.Sleep(3 * time.Second)

	// 清理资源
	httpClient.Close()
	taskHandler.Close()

	// 最终统计
	fmt.Printf("\n🎯 最终测试结果\n")
	metrics.PrintDetailedStats()

	dataCount, statusCount := testProducer.GetMessageCounts()
	fmt.Printf("\n📊 消息统计:\n")
	fmt.Printf("数据消息: %d\n", dataCount)
	fmt.Printf("状态消息: %d\n", statusCount)
	fmt.Printf("总任务数: %d\n", taskCounter)

	// 详细验证结果
	fmt.Printf("\n✅ 详细验证结果:\n")
	if metrics.rateLimitedCount > 0 {
		fmt.Printf("✅ 限流功能正常工作 (拦截了 %d 个请求, %.2f%%)\n", metrics.rateLimitedCount, float64(metrics.rateLimitedCount)/float64(metrics.totalRequests)*100)
	} else {
		fmt.Printf("⚠️  未检测到限流 (尝试增加请求频率或检查配置)\n")
	}

	if metrics.faultInjectedCount > 0 {
		fmt.Printf("✅ 故障注入功能正常工作 (注入了 %d 个故障, %.2f%%)\n", metrics.faultInjectedCount, float64(metrics.faultInjectedCount)/float64(metrics.totalRequests)*100)
	} else {
		fmt.Printf("⚠️  未检测到故障注入 (检查mock服务配置)\n")
	}

	// 性能统计
	totalTime := time.Since(startTime)
	avgRPS := float64(metrics.totalRequests) / totalTime.Seconds()
	fmt.Printf("\n📈 性能统计:\n")
	fmt.Printf("平均RPS: %.2f\n", avgRPS)
	fmt.Printf("成功率: %.2f%%\n", float64(metrics.successRequests)/float64(metrics.totalRequests)*100)
	fmt.Printf("故障率: %.2f%%\n", float64(metrics.faultInjectedCount)/float64(metrics.totalRequests)*100)

	fmt.Printf("\n🏁 增强测试完成！\n")
}
