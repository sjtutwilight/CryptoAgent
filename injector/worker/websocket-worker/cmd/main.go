package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"websocket-worker/internal/client"
	"websocket-worker/internal/config"
	"websocket-worker/internal/producer"

	"github.com/sirupsen/logrus"
)

func main() {
	// 命令行参数
	var (
		configPath = flag.String("config", "configs/config.yaml", "配置文件路径")
		logLevel   = flag.String("log-level", "info", "日志级别")
	)
	flag.Parse()

	// 设置日志
	logger := setupLogger(*logLevel)
	logger.Info("启动WebSocket Worker")

	// 加载配置
	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		logger.WithError(err).Fatal("加载配置失败")
	}

	logger.WithFields(logrus.Fields{
		"kafkaBrokers": cfg.Kafka.Brokers,
		"binanceURL":   cfg.Websocket.Binance.URL,
		"quicknodeURL": cfg.Websocket.Quicknode.URL,
	}).Info("配置加载成功")

	// 创建应用程序
	app, err := NewApplication(cfg, logger)
	if err != nil {
		logger.WithError(err).Fatal("创建应用程序失败")
	}

	// 启动应用程序
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := app.Start(ctx); err != nil {
		logger.WithError(err).Fatal("启动应用程序失败")
	}

	// 等待退出信号
	waitForShutdown(logger, cancel)

	// 停止应用程序
	if err := app.Stop(); err != nil {
		logger.WithError(err).Error("停止应用程序失败")
	}

	logger.Info("WebSocket Worker已停止")
}

// Application WebSocket Worker应用程序
type Application struct {
	config          *config.Config
	logger          *logrus.Logger
	producer        producer.DataProducer
	binanceClient   *client.BinanceClient
	quicknodeClient *client.QuicknodeClient
	wg              sync.WaitGroup
}

// NewApplication 创建新的应用程序
func NewApplication(cfg *config.Config, logger *logrus.Logger) (*Application, error) {
	// 创建数据生产者
	dataProducer, err := producer.NewDataProducer(cfg, logger)
	if err != nil {
		return nil, err
	}

	// 创建Binance客户端
	binanceClient := client.NewBinanceClient(cfg, logger.WithField("client", "binance"), dataProducer)

	// 创建QuickNode客户端
	quicknodeClient := client.NewQuicknodeClient(cfg, logger.WithField("client", "quicknode"), dataProducer)

	return &Application{
		config:          cfg,
		logger:          logger,
		producer:        dataProducer,
		binanceClient:   binanceClient,
		quicknodeClient: quicknodeClient,
	}, nil
}

// Start 启动应用程序
func (app *Application) Start(ctx context.Context) error {
	app.logger.Info("启动WebSocket Worker应用程序")

	// 启动Binance客户端
	app.wg.Add(1)
	go func() {
		defer app.wg.Done()
		if err := app.binanceClient.Start(); err != nil {
			app.logger.WithError(err).Error("启动Binance客户端失败")
			return
		}

		// 等待上下文取消
		<-ctx.Done()
		app.binanceClient.Stop()
	}()

	// 启动QuickNode客户端
	app.wg.Add(1)
	go func() {
		defer app.wg.Done()
		if err := app.quicknodeClient.Start(); err != nil {
			app.logger.WithError(err).Error("启动QuickNode客户端失败")
			return
		}

		// 等待上下文取消
		<-ctx.Done()
		app.quicknodeClient.Stop()
	}()

	// 等待所有客户端启动完成
	time.Sleep(2 * time.Second)
	app.logger.Info("所有WebSocket客户端已启动")

	return nil
}

// Stop 停止应用程序
func (app *Application) Stop() error {
	app.logger.Info("停止WebSocket Worker应用程序")

	// 停止所有客户端
	app.binanceClient.Stop()
	app.quicknodeClient.Stop()

	// 等待所有goroutine结束
	app.wg.Wait()

	// 关闭生产者
	if err := app.producer.Close(); err != nil {
		app.logger.WithError(err).Error("关闭数据生产者失败")
		return err
	}

	app.logger.Info("WebSocket Worker应用程序已停止")
	return nil
}

// setupLogger 设置日志记录器
func setupLogger(level string) *logrus.Logger {
	logger := logrus.New()
	logger.SetFormatter(&logrus.JSONFormatter{
		TimestampFormat: time.RFC3339,
	})

	// 设置日志级别
	switch level {
	case "debug":
		logger.SetLevel(logrus.DebugLevel)
	case "info":
		logger.SetLevel(logrus.InfoLevel)
	case "warn":
		logger.SetLevel(logrus.WarnLevel)
	case "error":
		logger.SetLevel(logrus.ErrorLevel)
	default:
		logger.SetLevel(logrus.InfoLevel)
	}

	return logger
}

// waitForShutdown 等待关闭信号
func waitForShutdown(logger *logrus.Logger, cancel context.CancelFunc) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	sig := <-sigChan
	logger.WithField("signal", sig).Info("收到退出信号")
	cancel()
}

