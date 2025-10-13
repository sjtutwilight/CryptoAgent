package role_v2

import (
	"context"
	"fmt"
	"log"

	"unified-worker/internal/fetcher"
	"unified-worker/internal/handler"
	"unified-worker/internal/protocol_v2"
	"unified-worker/internal/resource"
	"unified-worker/internal/task_v2"
	"unified-worker/pkg/types"
)

// RoleInstance 角色实例（新架构）
type RoleInstance struct {
	roleID string

	// 核心组件
	protocol protocol_v2.Protocol
	resource *resource.ResourceManager
	fetcher  fetcher.DataFetcher
	handler  handler.Handler
	task     task_v2.Task

	// 上下文
	ctx    context.Context
	cancel context.CancelFunc
}

// RoleConfig 角色配置
type RoleConfig struct {
	RoleID          string
	Protocol        string
	TaskType        string
	ProtocolConfig  map[string]interface{}
	TaskConfig      map[string]interface{}
	ResourcesConfig map[string]interface{}
	HandlersConfig  []handler.HandlerConfig
}

// NewRoleInstance 创建角色实例
func NewRoleInstance(config RoleConfig) (*RoleInstance, error) {
	ctx, cancel := context.WithCancel(context.Background())

	role := &RoleInstance{
		roleID: config.RoleID,
		ctx:    ctx,
		cancel: cancel,
	}

	// 1. 创建Protocol
	protocolFactory := protocol_v2.NewProtocolFactory()
	proto, err := protocolFactory.Create(config.Protocol)
	if err != nil {
		return nil, fmt.Errorf("创建协议失败: %w", err)
	}
	role.protocol = proto

	// 初始化Protocol
	if err := proto.Initialize(ctx, config.ProtocolConfig); err != nil {
		return nil, fmt.Errorf("初始化协议失败: %w", err)
	}
	log.Printf("[RoleInstance][%s] Protocol初始化成功: %s", config.RoleID, config.Protocol)

	// 2. 创建ResourceManager
	runtimeConfig := convertToRuntimeConfig(config.ResourcesConfig)
	protocolMeta := proto.Metadata()

	rm, err := resource.NewResourceManager(config.RoleID, runtimeConfig, protocolMeta)
	if err != nil {
		return nil, fmt.Errorf("创建资源管理器失败: %w", err)
	}
	role.resource = rm

	// 3. 创建Fetcher（可选）
	if pollingTask, ok := config.TaskConfig["polling_task"].(string); ok {
		fetcherFactory := fetcher.NewFetcherFactory()
		// TODO: 传入正确的client
		f, err := fetcherFactory.Create(pollingTask, nil)
		if err == nil && f != nil {
			role.fetcher = f
			log.Printf("[RoleInstance][%s] Fetcher创建成功: %s", config.RoleID, pollingTask)
		}
	}

	// 4. 创建Handler链
	handlerFactory := handler.NewHandlerFactory()
	h, err := handlerFactory.BuildChain(config.HandlersConfig)
	if err != nil {
		return nil, fmt.Errorf("创建Handler链失败: %w", err)
	}
	role.handler = h

	// 5. 创建Task
	taskCtx := &task_v2.TaskContext{
		Protocol: proto,
		Resource: rm,
		Fetcher:  role.fetcher,
		Handler:  h,
	}

	taskFactory := task_v2.NewTaskFactory()
	t, err := taskFactory.Create(config.TaskType, taskCtx, config.TaskConfig)
	if err != nil {
		return nil, fmt.Errorf("创建任务失败: %w", err)
	}
	role.task = t

	log.Printf("[RoleInstance][%s] 角色实例创建成功", config.RoleID)

	return role, nil
}

// Start 启动角色
func (r *RoleInstance) Start() error {
	log.Printf("[RoleInstance][%s] 启动角色", r.roleID)
	return r.task.Start(r.ctx)
}

// Stop 停止角色
func (r *RoleInstance) Stop() error {
	log.Printf("[RoleInstance][%s] 停止角色", r.roleID)

	r.cancel()

	if r.task != nil {
		r.task.Stop()
	}

	if r.protocol != nil {
		r.protocol.Close()
	}

	if r.resource != nil {
		r.resource.Close()
	}

	return nil
}

// convertToRuntimeConfig 转换资源配置为RuntimeConfig
// 只要配置项存在就自动启用,不需要enabled字段
func convertToRuntimeConfig(cfg map[string]interface{}) types.RuntimeConfig {
	runtimeCfg := types.RuntimeConfig{}

	// 解析rate_limit(存在即启用)
	if rl, ok := cfg["rate_limit"].(map[string]interface{}); ok {
		runtimeCfg.RateLimit = types.RateLimitConfig{
			Enabled:    true, // 只要配置了就启用
			Capacity:   getInt(rl, "capacity", 100),
			RefillRate: getFloat64(rl, "refill_rate", 10),
			RefillUnit: getString(rl, "refill_unit", "second"),
		}
	}

	// 解析connection_pool(存在即启用)
	if cp, ok := cfg["connection_pool"].(map[string]interface{}); ok {
		runtimeCfg.ConnectionPool = types.ConnectionPoolConfig{
			Enabled:         true, // 只要配置了就启用
			MaxIdleConns:    getInt(cp, "max_idle_conns", 10),
			MaxConnsPerHost: getInt(cp, "max_conns_per_host", 5),
		}
	}

	// 解析reconnect(存在即启用)
	if rc, ok := cfg["reconnect"].(map[string]interface{}); ok {
		runtimeCfg.Reconnect = types.ReconnectConfig{
			Enabled:     true, // 只要配置了就启用
			MaxRetries:  getInt(rc, "max_retries", -1),
			BackoffBase: getInt(rc, "backoff_base_seconds", 2),
			BackoffMax:  getInt(rc, "backoff_max_seconds", 60),
		}
	}

	// 解析heartbeat(存在即启用)
	if hb, ok := cfg["heartbeat"].(map[string]interface{}); ok {
		runtimeCfg.Heartbeat = types.HeartbeatConfig{
			Enabled:  true, // 只要配置了就启用
			Interval: getInt(hb, "interval_seconds", 30),
			Timeout:  getInt(hb, "timeout_seconds", 60),
		}
	}

	return runtimeCfg
}

// 辅助函数
func getBool(m map[string]interface{}, key string, def bool) bool {
	if v, ok := m[key].(bool); ok {
		return v
	}
	return def
}

func getInt(m map[string]interface{}, key string, def int) int {
	if v, ok := m[key].(float64); ok {
		return int(v)
	}
	if v, ok := m[key].(int); ok {
		return v
	}
	return def
}

func getString(m map[string]interface{}, key, def string) string {
	if v, ok := m[key].(string); ok {
		return v
	}
	return def
}

func getFloat64(m map[string]interface{}, key string, def float64) float64 {
	if v, ok := m[key].(float64); ok {
		return v
	}
	return def
}
