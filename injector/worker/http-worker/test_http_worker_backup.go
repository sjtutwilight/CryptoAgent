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
	"time"

	"github.com/sirupsen/logrus"
)

// MockProducer 模拟的Kafka生产者
type MockProducer struct {
	logger *logrus.Logger
}

func NewMockProducer(logger *logrus.Logger) *MockProducer {
	return &MockProducer{logger: logger}
}

func (mp *MockProducer) SendMessage(ctx context.Context, topic string, key string, value string) error {
	mp.logger.WithFields(logrus.Fields{
		"topic": topic,
		"key":   key,
		"size":  len(value),
	}).Info("📤 发送消息到Kafka")
	
	fmt.Printf("=== Kafka消息 ===\n")
	fmt.Printf("Topic: %s\n", topic)
	fmt.Printf("Key: %s\n", key)
	fmt.Printf("Value: %s\n", value)
	fmt.Printf("================\n\n")
	
	return nil
}

func (mp *MockProducer) SendTaskStatus(ctx context.Context, taskID string, status string) error {
	mp.logger.WithFields(logrus.Fields{
		"taskId": taskID,
		"topic":  "tasks.status",
	}).Info("📊 发送任务状态")
	
	fmt.Printf("=== 任务状态 ===\n")
	fmt.Printf("TaskID: %s\n", taskID)
	fmt.Printf("Status: %s\n", status)
	fmt.Printf("===============\n\n")
	
	return nil
}

func (mp *MockProducer) SendData(ctx context.Context, dataSourceID string, data string) error {
	mp.logger.WithFields(logrus.Fields{
		"dataSourceId": dataSourceID,
		"topic":        fmt.Sprintf("data.%s", dataSourceID),
	}).Info("📊 发送数据")
	
	fmt.Printf("=== 数据输出 ===\n")
	fmt.Printf("DataSourceID: %s\n", dataSourceID)
	fmt.Printf("Topic: data.%s\n", dataSourceID)
	fmt.Printf("Data: %s\n", data)
	fmt.Printf("===============\n\n")
	
	return nil
}

func (mp *MockProducer) Close() error {
	return nil
}

func main() {
	// 创建日志器
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)
	logger.SetFormatter(&logrus.TextFormatter{
		FullTimestamp:   true,
		TimestampFormat: "15:04:05",
	})
	
	fmt.Println("🚀 HTTP Worker 功能测试")
	fmt.Println("=======================")
	
	// 加载配置
	if err := config.Load("configs/config.yaml"); err != nil {
		log.Fatalf("加载配置失败: %v", err)
	}
	cfg := config.Get()
	
	// 创建组件
	httpClient := client.NewHTTPClient(&cfg.HTTP.Client)
	mockProducer := NewMockProducer(logger)
	
	refillInterval := time.Duration(cfg.RateLimit.RefillIntervalMs) * time.Millisecond
	rateLimitManager := ratelimit.NewTokenBucketManager(refillInterval)
	
	taskHandler := handler.NewTaskHandler(
		httpClient,
		mockProducer,
		rateLimitManager,
		cfg,
		logger,
	)
	
	// 创建测试任务
	testTasks := []*model.HttpTask{
		{
			TaskID: "test-001",
			Payload: model.TaskPayload{
				DataSourceURL: "http://localhost:8080",
				Method:        "POST",
				Params: map[string]interface{}{
					"method": "eth_getBlockByNumber",
					"params": []interface{}{"latest", false},
					"id":     1,
				},
				DataSourceID: "mock-ethereum",
			},
		},
		{
			TaskID: "test-002",
			Payload: model.TaskPayload{
				DataSourceURL: "http://localhost:8080",
				Method:        "POST",
				Params: map[string]interface{}{
					"method": "eth_blockNumber",
					"params": []interface{}{},
					"id":     2,
				},
				DataSourceID: "mock-ethereum",
			},
		},
		{
			TaskID: "test-003",
			Payload: model.TaskPayload{
				DataSourceURL: "http://localhost:8080/health",
				Method:        "GET",
				DataSourceID:  "mock-ethereum",
			},
		},
	}
	
	// 执行测试任务
	ctx := context.Background()
	
	for i, task := range testTasks {
		fmt.Printf("\n🔄 执行测试任务 %d/%d: %s\n", i+1, len(testTasks), task.TaskID)
		fmt.Printf("URL: %s\n", task.Payload.DataSourceURL)
		fmt.Printf("Method: %s\n", task.Payload.Method)
		
		if task.Payload.Params != nil {
			paramsJSON, _ := json.MarshalIndent(task.Payload.Params, "", "  ")
			fmt.Printf("Params: %s\n", string(paramsJSON))
		}
		
		fmt.Println("---")
		
		// 处理任务
		if err := taskHandler.HandleTask(ctx, task); err != nil {
			logger.WithError(err).Error("任务处理失败")
		}
		
		// 等待一小段时间
		time.Sleep(time.Second)
	}
	
	// 测试限流
	fmt.Println("\n🔄 测试限流功能")
	fmt.Println("发送多个快速请求...")
	
	for i := 0; i < 5; i++ {
		task := &model.HttpTask{
			TaskID: fmt.Sprintf("rate-limit-test-%d", i+1),
			Payload: model.TaskPayload{
				DataSourceURL: "http://localhost:8080/health",
				Method:        "GET",
				DataSourceID:  "mock-ethereum",
			},
		}
		
		start := time.Now()
		err := taskHandler.HandleTask(ctx, task)
		duration := time.Since(start)
		
		fmt.Printf("任务 %d: %v (耗时: %v)\n", i+1, err == nil, duration)
	}
	
	// 清理资源
	httpClient.Close()
	taskHandler.Close()
	
	fmt.Println("\n✅ 测试完成！")
	fmt.Println("\n检查上面的输出:")
	fmt.Println("1. ✅ HTTP请求是否成功调用mock服务")
	fmt.Println("2. ✅ 数据是否正确输出到data.mock-ethereum topic")
	fmt.Println("3. ✅ 任务状态是否正确输出到tasks.status topic") 
	fmt.Println("4. ✅ 限流机制是否正常工作")
}