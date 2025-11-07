package main

import (
	"context"
	"fmt"
	"log"
	"mock-service/internal/config"
	"mock-service/internal/controller"
	"mock-service/internal/fault"
	"mock-service/internal/generator"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
)

func main() {
	// 设置日志格式
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	// 加载配置
	configPath := "configs/config.yaml"
	if len(os.Args) > 1 {
		configPath = os.Args[1]
	}

	if err := config.Load(configPath); err != nil {
		log.Fatalf("加载配置失败: %v", err)
	}

	cfg := config.Get()
	log.Printf("配置加载成功: %s:%d", cfg.Server.Host, cfg.Server.Port)

	// 创建组件
	dataGen := generator.NewDataGenerator(cfg)
	faultInj := fault.NewFaultInjector(cfg)
	binanceSim := generator.NewBinanceOrderBookSimulator(&cfg.Data.Binance)

	// 创建控制器
	wsController := controller.NewWebSocketController(cfg, dataGen, faultInj)
	httpController := controller.NewHTTPController(cfg, dataGen, faultInj)
	binanceController := controller.NewBinanceOrderBookController(cfg, binanceSim, faultInj)

	// 启动WebSocket控制器
	wsController.Start()
	defer wsController.Stop()
	defer dataGen.Stop() // 确保数据生成器优雅关闭
	if binanceSim != nil {
		binanceSim.Start()
		defer binanceSim.Stop()
	}

	// 设置Gin模式
	gin.SetMode(gin.ReleaseMode)

	// 创建HTTP服务器
	r := gin.Default()

	// 设置路由
	httpController.SetupRoutes(r)
	if binanceController != nil {
		binanceController.RegisterRoutes(r)
	}

	// WebSocket端点
	r.GET("/ws", gin.WrapH(http.HandlerFunc(wsController.HandleWebSocket)))

	// 创建HTTP服务器
	server := &http.Server{
		Addr:    fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Server.Port),
		Handler: r,
	}

	// 启动服务器
	go func() {
		log.Printf("Mock服务器启动在 %s:%d", cfg.Server.Host, cfg.Server.Port)
		log.Println("WebSocket端点: /ws")
		log.Println("HTTP JSON-RPC端点: /")
		log.Println("健康检查端点: /health")
		log.Println("故障注入统计端点: /fault/stats")

		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("服务器启动失败: %v", err)
		}
	}()

	// 等待中断信号
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("正在关闭服务器...")

	// 优雅关闭
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		log.Printf("服务器关闭失败: %v", err)
	} else {
		log.Println("服务器已优雅关闭")
	}
}
