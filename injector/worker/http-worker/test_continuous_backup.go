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

// TestMetrics 测试指标
type TestMetrics struct {
	mu                 sync.RWMutex
	totalRequests      int
	successRequests    int
	failedRequests     int
	rateLimitedCount   int
	faultInjectedCount int
	statusCodes        map[int]int
}

func NewTestMetrics() *TestMetrics {
	return &TestMetrics{
		statusCodes: make(map[int]int),
	}
}

func (tm *TestMetrics) RecordRequest(success bool, statusCode int, rateLimited bool, faultInjected bool) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	tm.totalRequests++
	if success {
		tm.successRequests++
	} else {
		tm.failedRequests++
	}

	if rateLimited {
		tm.rateLimitedCount++
	}

	if faultInjected {
		tm.faultInjectedCount++
	}

	tm.statusCodes[statusCode]++
}

func (tm *TestMetrics) PrintStats() {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	fmt.Printf("\n📊 测试统计\n")
	fmt.Printf("=======================\n")
	fmt.Printf("总请求数: %d\n", tm.totalRequests)
	fmt.Printf("成功请求: %d (%.2f%%)\n", tm.successRequests, float64(tm.successRequests)/float64(tm.totalRequests)*100)
	fmt.Printf("失败请求: %d (%.2f%%)\n", tm.failedRequests, float64(tm.failedRequests)/float64(tm.totalRequests)*100)
	fmt.Printf("限流拦截: %d (%.2f%%)\n", tm.rateLimitedCount, float64(tm.rateLimitedCount)/float64(tm.totalRequests)*100)
	fmt.Printf("故障注入: %d (%.2f%%)\n", tm.faultInjectedCount, float64(tm.faultInjectedCount)/float64(tm.totalRequests)*100)

	fmt.Printf("\n状态码分布:\n")
	for code, count := range tm.statusCodes {
		fmt.Printf("  %d: %d次 (%.2f%%)\n", code, count, float64(count)/float64(tm.totalRequests)*100)
	}
	fmt.Printf("=======================\n")
}

// ContinuousTestProducer 持续测试的生产者
type ContinuousTestProducer struct {
	logger         *logrus.Logger
	metrics        *TestMetrics
	mu             sync.RWMutex
	dataMessages   []string
	statusMessages []string
}

func NewContinuousTestProducer(logger *logrus.Logger, metrics *TestMetrics) *ContinuousTestProducer {
	return &ContinuousTestProducer{
		logger:         logger,
		metrics:        metrics,
		dataMessages:   make([]string, 0),
		statusMessages: make([]string, 0),
	}
}

func (ctp *ContinuousTestProducer) SendMessage(ctx context.Context, topic string, key string, value string) error {
	ctp.mu.Lock()
	defer ctp.mu.Unlock()

	ctp.dataMessages = append(ctp.dataMessages, value)

	// 解析数据以检查是否为故障注入
	var dataRecord model.DataRecord
	if err := json.Unmarshal([]byte(value), &dataRecord); err == nil {
		if metadata, ok := dataRecord.Metadata["statusCode"]; ok {
			statusCode := int(metadata.(float64))
			if statusCode >= 400 {
				ctp.metrics.RecordRequest(false, statusCode, false, true)
				ctp.logger.WithFields(logrus.Fields{
					"statusCode": statusCode,
					"taskId":     dataRecord.TaskID,
				}).Info("🔴 检测到故障注入")
			} else {
				ctp.metrics.RecordRequest(true, statusCode, false, false)
			}
		}
	}

	return nil
}

func (ctp *ContinuousTestProducer) SendTaskStatus(ctx context.Context, taskID string, status string) error {
	ctp.mu.Lock()
	defer ctp.mu.Unlock()

	ctp.statusMessages = append(ctp.statusMessages, status)

	// 解析状态以检查限流
	var taskStatus model.TaskStatus
	if err := json.Unmarshal([]byte(status), &taskStatus); err == nil {
		if taskStatus.Status == "RATE_LIMITED" {
			ctp.metrics.RecordRequest(false, 429, true, false)
			ctp.logger.WithFields(logrus.Fields{
				"taskId": taskID,
			}).Info("🟡 检测到限流")
		}
	}

	return nil
}

func (ctp *ContinuousTestProducer) SendData(ctx context.Context, dataSourceID string, data string) error {
	return ctp.SendMessage(ctx, fmt.Sprintf("data.%s", dataSourceID), "", data)
}

func (ctp *ContinuousTestProducer) Close() error {
	return nil
}

func (ctp *ContinuousTestProducer) GetMessageCounts() (int, int) {
	ctp.mu.RLock()
	defer ctp.mu.RUnlock()
	return len(ctp.dataMessages), len(ctp.statusMessages)
}

func main() {
	// 创建日志器
	logger := logrus.New()
	logger.SetLevel(logrus.InfoLevel)
	logger.SetFormatter(&logrus.TextFormatter{
		FullTimestamp:   true,
		TimestampFormat: "15:04:05",
	})

	fmt.Println("🚀 HTTP Worker 持续测试")
	fmt.Println("=======================")
	fmt.Println("这个测试将持续发送请求来验证:")
	fmt.Println("1. 限流模块是否正常工作")
	fmt.Println("2. 故障注入是否正确映射到task.status")
	fmt.Println("3. 数据是否正确输出到data topic")
	fmt.Println("=======================")

	// 加载配置
	if err := config.Load("configs/config.yaml"); err != nil {
		log.Fatalf("加载配置失败: %v", err)
	}
	cfg := config.Get()

	// 创建测试指标
	metrics := NewTestMetrics()

	// 创建组件
	httpClient := client.NewHTTPClient(&cfg.HTTP.Client)
	testProducer := NewContinuousTestProducer(logger, metrics)

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
		{
			DataSourceURL: "http://localhost:8090/health",
			Method:        "GET",
			DataSourceID:  "mock-ethereum",
		},
	}

	// 启动持续测试
	ctx := context.Background()
	testDuration := 30 * time.Second
	requestInterval := 100 * time.Millisecond

	fmt.Printf("⏰ 开始持续测试 (持续时间: %v, 请求间隔: %v)\n", testDuration, requestInterval)

	startTime := time.Now()
	ticker := time.NewTicker(requestInterval)
	defer ticker.Stop()

	taskCounter := 0

	// 统计显示定时器
	statsTicker := time.NewTicker(5 * time.Second)
	defer statsTicker.Stop()

	for {
		select {
		case <-ticker.C:
			taskCounter++

			// 选择任务模板
			template := taskTemplates[taskCounter%len(taskTemplates)]

			// 创建任务
			task := &model.HttpTask{
				TaskID:  fmt.Sprintf("continuous-test-%d", taskCounter),
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
			metrics.PrintStats()
			dataCount, statusCount := testProducer.GetMessageCounts()
			fmt.Printf("📨 消息数量 - 数据: %d, 状态: %d\n", dataCount, statusCount)

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
	time.Sleep(2 * time.Second)

	// 清理资源
	httpClient.Close()
	taskHandler.Close()

	// 最终统计
	fmt.Printf("\n🎯 最终测试结果\n")
	metrics.PrintStats()

	dataCount, statusCount := testProducer.GetMessageCounts()
	fmt.Printf("\n📊 消息统计:\n")
	fmt.Printf("数据消息: %d\n", dataCount)
	fmt.Printf("状态消息: %d\n", statusCount)

	// 验证结果
	fmt.Printf("\n✅ 验证结果:\n")
	if metrics.rateLimitedCount > 0 {
		fmt.Printf("✅ 限流功能正常工作 (拦截了 %d 个请求)\n", metrics.rateLimitedCount)
	} else {
		fmt.Printf("⚠️  未检测到限流 (可能需要增加请求频率)\n")
	}

	if metrics.faultInjectedCount > 0 {
		fmt.Printf("✅ 故障注入功能正常工作 (注入了 %d 个故障)\n", metrics.faultInjectedCount)
	} else {
		fmt.Printf("⚠️  未检测到故障注入 (检查mock服务配置)\n")
	}

	fmt.Printf("\n🏁 测试完成！\n")
}
