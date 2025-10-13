package worker

import (
	"fmt"
	"log"

	"unified-worker/internal/config"
	"unified-worker/internal/fetcher"
	"unified-worker/internal/handler"
	"unified-worker/internal/protocol"
	"unified-worker/internal/resource"
	"unified-worker/internal/runtime"
	"unified-worker/internal/task"
	"unified-worker/pkg/types"
)

// createRoleInstanceV2 创建角色实例（v2.0架构）
// 通过检测配置中是否有handlers字段来判断使用v2.0还是v1.0
func (m *Manager) createRoleInstanceV2(roleConfig config.RoleConfig) (*RoleInstance, error) {
	log.Printf("[v2.0] 开始创建角色实例: role_id=%s, protocol=%s, task_type=%s",
		roleConfig.RoleID, roleConfig.Protocol, roleConfig.TaskType)

	// 1. 创建协议处理器
	protocolMeta, protocolHandler, err := m.createProtocolHandler(roleConfig)
	if err != nil {
		return nil, fmt.Errorf("创建协议处理器失败: %w", err)
	}

	// 2. 创建资源管理器
	runtimeConfig := config.GetRuntimeConfig(roleConfig)
	resourceMgr, err := resource.NewResourceManager(roleConfig.RoleID, runtimeConfig, protocolMeta)
	if err != nil {
		return nil, fmt.Errorf("创建资源管理器失败: %w", err)
	}

	// 3. 创建数据获取器（仅polling任务需要）
	var dataFetcher fetcher.DataFetcher
	if roleConfig.TaskType == "polling" {
		pollingTask, _ := roleConfig.TaskConfig["polling_task"].(string)
		if pollingTask != "" {
			factory := fetcher.NewFetcherFactory()
			// 传递protocol handler作为client
			dataFetcher, err = factory.Create(pollingTask, protocolHandler)
			if err != nil {
				return nil, fmt.Errorf("创建数据获取器失败: %w", err)
			}
			log.Printf("[v2.0] 创建数据获取器: %s", pollingTask)
		}
	}

	// 4. 创建处理链
	handlerChain, err := m.createHandlerChain(roleConfig)
	if err != nil {
		return nil, fmt.Errorf("创建处理链失败: %w", err)
	}

	// 5. 创建任务配置和执行器
	taskConfig := m.buildTaskConfig(roleConfig)

	// v2.0: 使用资源管理器的限流器（可能为nil）
	var rateLimiter runtime.RateLimiter
	rateLimiter = resourceMgr.GetRateLimiter()

	executor := task.NewTaskExecutor(
		m.config.Worker.ID,
		taskConfig,
		protocolHandler,
		m.producer,
		rateLimiter,
	)

	log.Printf("[v2.0] 角色实例创建成功: role_id=%s", roleConfig.RoleID)

	return &RoleInstance{
		roleID:          roleConfig.RoleID,
		protocol:        protocolHandler,
		executor:        executor,
		resourceManager: resourceMgr,
		dataFetcher:     dataFetcher,
		handlerChain:    handlerChain,
		taskConfig:      taskConfig,
	}, nil
}

// createProtocolHandler 创建协议处理器
func (m *Manager) createProtocolHandler(roleConfig config.RoleConfig) (types.ProtocolMetadata, types.ProtocolHandler, error) {
	switch roleConfig.Protocol {
	case "ethereum-sdk":
		handler := protocol.NewEthereumSDKHandler()

		// 使用Initialize初始化
		if err := handler.Initialize(nil, roleConfig.ProtocolConfig); err != nil {
			return types.ProtocolMetadata{}, nil, err
		}

		meta := types.ProtocolMetadata{
			HasBuiltInRateLimit: false,
			HasBuiltInReconnect: true, // ethclient自带重连
			HasBuiltInHeartbeat: false,
		}

		return meta, handler, nil

	case "http":
		runtimeConfig := config.GetRuntimeConfig(roleConfig)
		handler := protocol.NewHTTPHandler(runtimeConfig)

		meta := types.ProtocolMetadata{
			HasBuiltInRateLimit: false,
			HasBuiltInReconnect: false,
			HasBuiltInHeartbeat: false,
		}
		return meta, handler, nil

	case "websocket":
		runtimeConfig := config.GetRuntimeConfig(roleConfig)
		handler := protocol.NewWebSocketHandler(runtimeConfig)

		// 使用Initialize初始化
		if err := handler.Initialize(nil, roleConfig.ProtocolConfig); err != nil {
			return types.ProtocolMetadata{}, nil, err
		}

		meta := types.ProtocolMetadata{
			HasBuiltInRateLimit: false,
			HasBuiltInReconnect: true, // WebSocket自带重连
			HasBuiltInHeartbeat: true, // WebSocket自带心跳
		}
		return meta, handler, nil

	default:
		return types.ProtocolMetadata{}, nil, fmt.Errorf("未知协议类型: %s", roleConfig.Protocol)
	}
}

// createHandlerChain 创建处理链
func (m *Manager) createHandlerChain(roleConfig config.RoleConfig) (handler.Handler, error) {
	// 如果配置了handlers，使用v2.0模式
	if len(roleConfig.Handlers) > 0 {
		factory := handler.NewHandlerFactory()

		// 转换配置格式
		var handlerConfigs []handler.HandlerConfig
		for _, h := range roleConfig.Handlers {
			handlerConfigs = append(handlerConfigs, handler.HandlerConfig{
				Type:   h.Type,
				Name:   h.Name,
				Config: h.Config,
			})
		}

		return factory.BuildChain(handlerConfigs)
	}

	// v1.0兼容：使用默认处理链（Parser → Kafka）
	if roleConfig.OutputTopic != "" {
		log.Printf("[v2.0] 使用v1.0兼容模式，构建默认处理链")

		// 默认Parser（根据task_type推断）
		parserName := inferParserName(roleConfig.TaskType, roleConfig.TaskConfig)
		kafkaConfig := handler.KafkaSinkConfig{
			Topic:   roleConfig.OutputTopic,
			Brokers: m.config.Kafka.Brokers,
		}

		factory := handler.NewHandlerFactory()
		return factory.BuildChain([]handler.HandlerConfig{
			{
				Type: "parser",
				Name: parserName,
			},
			{
				Type: "kafka_sink",
				Config: map[string]interface{}{
					"topic":   kafkaConfig.Topic,
					"brokers": kafkaConfig.Brokers,
				},
			},
		})
	}

	return nil, fmt.Errorf("未配置handlers或output_topic")
}

// inferParserName 根据任务类型推断Parser名称
func inferParserName(taskType string, taskConfig map[string]interface{}) string {
	if taskType == "polling" {
		if pollingTask, ok := taskConfig["polling_task"].(string); ok {
			switch pollingTask {
			case "balance":
				return "BalanceParser"
			case "block":
				return "BlockParser"
			}
		}
	}
	return "GenericParser"
}
