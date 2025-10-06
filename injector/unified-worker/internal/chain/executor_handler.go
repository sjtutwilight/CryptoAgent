package chain

import (
	"context"
	"fmt"
	"log"
	"time"

	"unified-worker/internal/config"
	"unified-worker/internal/kafka"
	"unified-worker/internal/runtime"
	"unified-worker/internal/task"
	"unified-worker/pkg/types"
)

// ExecutorHandler 任务执行器处理器
type ExecutorHandler struct {
	BaseHandler
	workerID string
	producer *kafka.Producer
}

// NewExecutorHandler 创建执行器处理器
func NewExecutorHandler(workerID string, producer *kafka.Producer) *ExecutorHandler {
	return &ExecutorHandler{
		BaseHandler: *NewBaseHandler("ExecutorHandler"),
		workerID:    workerID,
		producer:    producer,
	}
}

// Handle 处理执行器创建
func (h *ExecutorHandler) Handle(ctx context.Context, req *Request) error {
	roleConfig, ok := req.RoleConfig.(config.RoleConfig)
	if !ok {
		return fmt.Errorf("[%s] 无效的角色配置类型", h.GetName())
	}

	log.Printf("[%s] 创建任务执行器: task_id=%s", h.GetName(), roleConfig.RoleID)

	// 获取protocol
	protocolHandler, ok := req.Data["protocol"].(types.ProtocolHandler)
	if !ok {
		return fmt.Errorf("[%s] 缺少protocol", h.GetName())
	}

	// 获取rate_limiter（可能为nil）
	var rateLimiter runtime.RateLimiter
	if rl, ok := req.Data["rate_limiter"].(runtime.RateLimiter); ok {
		rateLimiter = rl
	}

	// 构建任务配置
	taskConfig := h.buildTaskConfig(roleConfig)
	req.Data["task_config"] = taskConfig

	// 创建任务执行器
	executor := task.NewTaskExecutor(
		h.workerID,
		taskConfig,
		protocolHandler,
		h.producer,
		rateLimiter,
	)

	req.Data["executor"] = executor

	log.Printf("[%s] 任务执行器创建成功", h.GetName())

	// 继续下一个处理器
	return h.CallNext(ctx, req)
}

// buildTaskConfig 构建任务配置（从manager_impl.go迁移）
func (h *ExecutorHandler) buildTaskConfig(roleConfig config.RoleConfig) *types.TaskConfig {
	taskConfig := &types.TaskConfig{
		TaskID:               roleConfig.RoleID,
		TaskType:             types.TaskType(roleConfig.TaskType),
		Protocol:             types.ProtocolType(roleConfig.Protocol),
		DataSourceID:         roleConfig.DataSourceID,
		ProtocolConfig:       roleConfig.ProtocolConfig,
		TaskSpecificConfig:   h.parseTaskSpecificConfig(roleConfig),
		OutputTopic:          roleConfig.OutputTopic,
		SequenceField:        roleConfig.SequenceField,
		ReportToControlPlane: roleConfig.ReportToControlPlane,
		RetryConfig: types.RetryConfig{
			MaxRetries:  3,
			BackoffBase: 2,
			BackoffMax:  30,
		},
	}

	return taskConfig
}

// parseTaskSpecificConfig 解析任务特定配置
func (h *ExecutorHandler) parseTaskSpecificConfig(roleConfig config.RoleConfig) types.TaskSpecificConfig {
	config := types.TaskSpecificConfig{}

	switch roleConfig.TaskType {
	case "long_connection":
		if sub, ok := roleConfig.TaskConfig["subscription"].(map[string]interface{}); ok {
			var topics []string
			if topicsRaw, ok := sub["topics"].([]interface{}); ok {
				for _, t := range topicsRaw {
					if topic, ok := t.(string); ok {
						topics = append(topics, topic)
					}
				}
			}
			config.Subscription = &types.SubscriptionConfig{
				Topics: topics,
			}
		}

	case "polling":
		if poll, ok := roleConfig.TaskConfig["polling"].(map[string]interface{}); ok {
			intervalSec, _ := poll["interval_seconds"].(int)
			method, _ := poll["method"].(string)
			params, _ := poll["params"].(map[string]interface{})

			config.Polling = &types.PollingConfig{
				Interval: time.Duration(intervalSec) * time.Second,
				Method:   method,
				Params:   params,
			}
		}

	case "one_time":
		if oneTime, ok := roleConfig.TaskConfig["one_time"].(map[string]interface{}); ok {
			method, _ := oneTime["method"].(string)
			params, _ := oneTime["params"].(map[string]interface{})

			config.OneTime = &types.OneTimeConfig{
				Method: method,
				Params: params,
			}
		}
	}

	return config
}
