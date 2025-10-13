package worker

import (
	"context"
	"fmt"
	"log"
	"sync"

	"unified-worker/internal/chain"
	"unified-worker/internal/config"
	"unified-worker/internal/kafka"
	"unified-worker/internal/runtime"
	"unified-worker/internal/task"
	"unified-worker/pkg/types"
)

// Manager Worker管理器
type Manager struct {
	config   *config.Config
	roles    map[string]*RoleInstance
	producer *kafka.Producer
	consumer *kafka.Consumer
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

// NewManager 创建Worker管理器
func NewManager(cfg *config.Config) (*Manager, error) {
	ctx, cancel := context.WithCancel(context.Background())

	// 创建Kafka生产者
	producer, err := kafka.NewProducer(cfg.Kafka)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("创建Kafka生产者失败: %w", err)
	}

	mgr := &Manager{
		config:   cfg,
		roles:    make(map[string]*RoleInstance),
		producer: producer,
		ctx:      ctx,
		cancel:   cancel,
	}

	// 初始化所有角色
	if err := mgr.initializeRoles(); err != nil {
		cancel()
		return nil, fmt.Errorf("初始化角色失败: %w", err)
	}

	return mgr, nil
}

// initializeRoles 初始化所有角色
func (m *Manager) initializeRoles() error {
	for _, roleConfig := range m.config.Worker.Roles {
		roleInstance, err := m.createRoleInstance(roleConfig)
		if err != nil {
			return fmt.Errorf("创建角色实例失败: role_id=%s, error=%w",
				roleConfig.RoleID, err)
		}

		m.roles[roleConfig.RoleID] = roleInstance
		log.Printf("角色初始化成功: role_id=%s, protocol=%s, task_type=%s",
			roleConfig.RoleID, roleConfig.Protocol, roleConfig.TaskType)
	}

	return nil
}

// createRoleInstance 创建角色实例（自动选择v1.0或v2.0）
func (m *Manager) createRoleInstance(roleConfig config.RoleConfig) (*RoleInstance, error) {
	// 检测使用v2.0还是v1.0：如果配置了handlers或resources，使用v2.0
	useV2 := len(roleConfig.Handlers) > 0 || !isEmptyResourcesConfig(roleConfig.Resources)

	if useV2 {
		log.Printf("[架构] 使用v2.0架构: role_id=%s", roleConfig.RoleID)
		return m.createRoleInstanceV2(roleConfig)
	}

	// v1.0兼容模式（保留原有责任链逻辑）
	log.Printf("[架构] 使用v1.0架构（责任链）: role_id=%s", roleConfig.RoleID)
	return m.createRoleInstanceV1(roleConfig)
}

// createRoleInstanceV1 创建角色实例（v1.0责任链模式）
func (m *Manager) createRoleInstanceV1(roleConfig config.RoleConfig) (*RoleInstance, error) {
	log.Printf("【责任链】开始创建角色实例: role_id=%s, protocol=%s, task_type=%s",
		roleConfig.RoleID, roleConfig.Protocol, roleConfig.TaskType)

	// 构建责任链: Protocol → RateLimit → Executor
	protocolHandler := chain.NewProtocolHandler()
	rateLimitHandler := chain.NewRateLimitHandler()
	executorHandler := chain.NewExecutorHandler(m.config.Worker.ID, m.producer)

	// 链接处理器
	protocolHandler.SetNext(rateLimitHandler)
	rateLimitHandler.SetNext(executorHandler)

	// 创建责任链
	roleChain := chain.NewChain(protocolHandler)

	// 创建请求
	req := &chain.Request{
		RoleConfig: roleConfig,
		Data:       make(map[string]interface{}),
		Skip:       false,
	}

	// 执行责任链
	if err := roleChain.Execute(m.ctx, req); err != nil {
		return nil, fmt.Errorf("责任链执行失败: %w", err)
	}

	// 从请求中提取结果
	protocolHandlerResult, ok := req.Data["protocol"].(types.ProtocolHandler)
	if !ok {
		return nil, fmt.Errorf("责任链未返回protocol")
	}

	executorRaw := req.Data["executor"]
	if executorRaw == nil {
		return nil, fmt.Errorf("责任链未返回executor")
	}

	// 需要导入task包来进行类型断言
	executorResult, ok := executorRaw.(*task.TaskExecutor)
	if !ok {
		return nil, fmt.Errorf("executor类型错误")
	}

	taskConfigResult, ok := req.Data["task_config"].(*types.TaskConfig)
	if !ok {
		return nil, fmt.Errorf("责任链未返回task_config")
	}

	// rate_limiter可能为nil（如WebSocket长连接）
	var rateLimiterResult runtime.RateLimiter
	if rl, ok := req.Data["rate_limiter"].(runtime.RateLimiter); ok {
		rateLimiterResult = rl
	}

	log.Printf("【责任链】角色实例创建成功: role_id=%s", roleConfig.RoleID)

	return &RoleInstance{
		roleID:      roleConfig.RoleID,
		protocol:    protocolHandlerResult,
		executor:    executorResult,
		rateLimiter: rateLimiterResult,
		taskConfig:  taskConfigResult,
	}, nil
}

// isEmptyResourcesConfig 检查resources配置是否为空
func isEmptyResourcesConfig(config config.ResourcesConfigYAML) bool {
	return !config.RateLimit.Enabled &&
		!config.ConnectionPool.Enabled &&
		!config.Reconnect.Enabled &&
		!config.Heartbeat.Enabled
}
