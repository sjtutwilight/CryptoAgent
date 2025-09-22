package main

import (
	"context"
	"fmt"
	"http-worker/internal/client"
	"http-worker/internal/config"
	"http-worker/internal/consumer"
	"http-worker/internal/handler"
	"http-worker/internal/producer"
	"http-worker/internal/ratelimit"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"
)

func main() {
	// 创建日志器
	logger := logrus.New()
	logger.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
		TimestampFormat: "2006-01-02 15:04:05",
	})
	
	// 加载配置
	configPath := "configs/config.yaml"
	if len(os.Args) > 1 {
		configPath = os.Args[1]
	}
	
	if err := config.Load(configPath); err != nil {
		logger.Fatalf("加载配置失败: %v", err)
	}
	
	cfg := config.Get()
	
	// 设置日志级别
	level, err := logrus.ParseLevel(cfg.Logging.Level)
	if err != nil {
		logger.Warnf("无效的日志级别 %s，使用默认级别 info", cfg.Logging.Level)
		level = logrus.InfoLevel
	}
	logger.SetLevel(level)
	
	// 设置日志格式
	if cfg.Logging.Format == "json" {
		logger.SetFormatter(&logrus.JSONFormatter{
			TimestampFormat: "2006-01-02T15:04:05.000Z",
		})
	}
	
	logger.WithFields(logrus.Fields{
		"version": "1.0.0",
		"config":  configPath,
	}).Info("HTTP Worker启动中...")
	
	// 创建上下文
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	
	// 创建组件
	components, err := createComponents(cfg, logger)
	if err != nil {
		logger.Fatalf("创建组件失败: %v", err)
	}
	defer closeComponents(components, logger)
	
	// 启动消费者
	if err := components.consumer.Start(ctx); err != nil {
		logger.Fatalf("启动Kafka消费者失败: %v", err)
	}
	
	logger.Info("HTTP Worker启动完成，等待任务...")
	
	// 等待中断信号
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	
	logger.Info("收到停止信号，正在关闭HTTP Worker...")
	
	// 取消上下文
	cancel()
	
	// 停止消费者
	if err := components.consumer.Stop(); err != nil {
		logger.Errorf("停止Kafka消费者失败: %v", err)
	}
	
	logger.Info("HTTP Worker已关闭")
}

// Components 组件集合
type Components struct {
	httpClient       client.HTTPClient
	producer         producer.KafkaProducer
	rateLimitManager *ratelimit.TokenBucketManager
	taskHandler      handler.TaskHandler
	consumer         consumer.KafkaConsumer
}

// createComponents 创建所有组件
func createComponents(cfg *config.Config, logger *logrus.Logger) (*Components, error) {
	// 创建HTTP客户端
	httpClient := client.NewHTTPClient(&cfg.HTTP.Client)
	
	// 创建Kafka生产者
	kafkaProducer, err := producer.NewKafkaProducer(cfg, logger)
	if err != nil {
		return nil, fmt.Errorf("创建Kafka生产者失败: %w", err)
	}
	
	// 创建令牌桶管理器
	refillInterval := time.Duration(cfg.RateLimit.RefillIntervalMs) * time.Millisecond
	rateLimitManager := ratelimit.NewTokenBucketManager(refillInterval)
	
	// 创建任务处理器
	taskHandler := handler.NewTaskHandler(
		httpClient,
		kafkaProducer,
		rateLimitManager,
		cfg,
		logger,
	)
	
	// 创建Kafka消费者
	kafkaConsumer, err := consumer.NewKafkaConsumer(cfg, taskHandler, logger)
	if err != nil {
		return nil, fmt.Errorf("创建Kafka消费者失败: %w", err)
	}
	
	return &Components{
		httpClient:       httpClient,
		producer:         kafkaProducer,
		rateLimitManager: rateLimitManager,
		taskHandler:      taskHandler,
		consumer:         kafkaConsumer,
	}, nil
}

// closeComponents 关闭所有组件
func closeComponents(components *Components, logger *logrus.Logger) {
	if components == nil {
		return
	}
	
	// 关闭任务处理器
	if err := components.taskHandler.Close(); err != nil {
		logger.Errorf("关闭任务处理器失败: %v", err)
	}
	
	// 关闭HTTP客户端
	if err := components.httpClient.Close(); err != nil {
		logger.Errorf("关闭HTTP客户端失败: %v", err)
	}
	
	// 关闭Kafka生产者
	if err := components.producer.Close(); err != nil {
		logger.Errorf("关闭Kafka生产者失败: %v", err)
	}
	
	logger.Info("所有组件已关闭")
}