package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"unified-worker/internal/config"
	"unified-worker/internal/handler"
	"unified-worker/internal/role_v2"
)

func main() {
	log.Println("🚀 Unified Worker v2.0 启动...")

	// 解析命令行参数
	configPath := flag.String("config", "configs/config.yaml", "配置文件路径")
	flag.Parse()

	// 加载配置
	log.Printf("📖 加载配置文件: %s", *configPath)
	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("❌ 加载配置失败: %v", err)
	}

	log.Printf("✅ 配置加载成功，共 %d 个角色", len(cfg.Roles))

	// 创建所有角色实例
	var roles []*role_v2.RoleInstance
	for _, roleYaml := range cfg.Roles {
		log.Printf("🔧 创建角色: %s (protocol=%s, task_type=%s)",
			roleYaml.RoleID, roleYaml.Protocol, roleYaml.TaskType)

		// 转换配置
		roleConfigMap := config.ConvertToRoleV2Config(roleYaml)

		// DEBUG: 打印handler配置
		if handlersConfig, ok := roleConfigMap["handlers_config"].([]map[string]interface{}); ok {
			log.Printf("  [DEBUG] handlers_config 数量: %d", len(handlersConfig))
			for i, h := range handlersConfig {
				log.Printf("    [%d] type=%s, name=%s, config=%+v",
					i, h["type"], h["name"], h["config"])
			}
		}

		// 构建RoleConfig
		roleConfig := role_v2.RoleConfig{
			RoleID:          roleConfigMap["role_id"].(string),
			Protocol:        roleConfigMap["protocol"].(string),
			TaskType:        roleConfigMap["task_type"].(string),
			ProtocolConfig:  roleConfigMap["protocol_config"].(map[string]interface{}),
			TaskConfig:      roleConfigMap["task_config"].(map[string]interface{}),
			ResourcesConfig: roleConfigMap["resources_config"].(map[string]interface{}),
		}

		// 处理HandlersConfig
		if handlersConfig, ok := roleConfigMap["handlers_config"].([]map[string]interface{}); ok {
			for _, h := range handlersConfig {
				handlerType := h["type"].(string)
				handlerName := ""
				if name, ok := h["name"].(string); ok {
					handlerName = name
				}
				handlerCfg := make(map[string]interface{})
				if cfg, ok := h["config"].(map[string]interface{}); ok {
					handlerCfg = cfg
				}

				roleConfig.HandlersConfig = append(roleConfig.HandlersConfig, handler.HandlerConfig{
					Type:   handlerType,
					Name:   handlerName,
					Config: handlerCfg,
				})
			}
		}

		// 创建角色实例
		role, err := role_v2.NewRoleInstance(roleConfig)
		if err != nil {
			log.Fatalf("❌ 创建角色 %s 失败: %v", roleYaml.RoleID, err)
		}

		roles = append(roles, role)
		log.Printf("✅ 角色 %s 创建成功", roleYaml.RoleID)
	}

	// 启动所有角色
	_, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	for _, role := range roles {
		wg.Add(1)
		r := role // 捕获变量
		go func() {
			defer wg.Done()
			if err := r.Start(); err != nil {
				log.Printf("❌ 角色运行错误: %v", err)
			}
		}()
	}

	log.Printf("✅ Unified Worker v2.0 运行中，共 %d 个角色", len(roles))

	// 等待信号
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("🛑 接收到停止信号，优雅关闭...")
	cancel()

	// 停止所有角色
	for _, role := range roles {
		if err := role.Stop(); err != nil {
			log.Printf("⚠️ 停止角色失败: %v", err)
		}
	}

	wg.Wait()
	log.Println("👋 Unified Worker v2.0 已停止")
}
