package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	
	"unified-worker/internal/config"
	"unified-worker/internal/worker"
)

var (
	configPath = flag.String("config", "configs/config.yaml", "配置文件路径")
)

func main() {
	flag.Parse()
	
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.Printf("🚀 Unified Worker 启动中...")
	
	// 加载配置
	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("❌ 加载配置失败: %v", err)
	}
	
	log.Printf("✅ 配置加载成功: worker_id=%s, roles=%d", 
		cfg.Worker.ID, len(cfg.Worker.Roles))
	
	// 创建Worker管理器
	mgr, err := worker.NewManager(cfg)
	if err != nil {
		log.Fatalf("❌ 创建Worker管理器失败: %v", err)
	}
	
	// 启动Worker管理器
	if err := mgr.Start(); err != nil {
		log.Fatalf("❌ 启动Worker管理器失败: %v", err)
	}
	
	log.Printf("✅ Unified Worker 启动完成")
	log.Printf("📊 运行状态:")
	log.Printf("   - Worker ID: %s", cfg.Worker.ID)
	log.Printf("   - 角色数量: %d", len(cfg.Worker.Roles))
	log.Printf("   - Kafka Brokers: %v", cfg.Kafka.Brokers)
	
	// 等待中断信号
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	
	sig := <-sigChan
	log.Printf("🛑 收到停止信号: %v", sig)
	
	// 优雅停止
	if err := mgr.Stop(); err != nil {
		log.Printf("❌ 停止Worker管理器失败: %v", err)
	}
	
	log.Printf("👋 Unified Worker 已停止")
}
