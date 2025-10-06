package worker

import (
	"context"
	"fmt"
	"log"
	"time"

	"unified-worker/internal/config"
	"unified-worker/internal/kafka"
	"unified-worker/internal/task"
	"unified-worker/pkg/types"
)

// buildTaskConfig 构建任务配置
func (m *Manager) buildTaskConfig(roleConfig config.RoleConfig) *types.TaskConfig {
	taskConfig := &types.TaskConfig{
		TaskID:               roleConfig.RoleID, // 使用role_id作为task_id
		TaskType:             types.TaskType(roleConfig.TaskType),
		Protocol:             types.ProtocolType(roleConfig.Protocol),
		DataSourceID:         roleConfig.DataSourceID,
		ProtocolConfig:       roleConfig.ProtocolConfig,
		TaskSpecificConfig:   m.parseTaskSpecificConfig(roleConfig),
		OutputTopic:          roleConfig.OutputTopic,
		SequenceField:        roleConfig.SequenceField,
		ReportToControlPlane: roleConfig.ReportToControlPlane,
		RetryConfig: types.RetryConfig{
			MaxRetries:  3,
			BackoffBase: 2 * time.Second,
			BackoffMax:  30 * time.Second,
		},
	}

	return taskConfig
}

// parseTaskSpecificConfig 解析任务特定配置
func (m *Manager) parseTaskSpecificConfig(roleConfig config.RoleConfig) types.TaskSpecificConfig {
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

// Start 启动Worker管理器
func (m *Manager) Start() error {
	log.Printf("启动Worker管理器: worker_id=%s, roles=%d",
		m.config.Worker.ID, len(m.roles))

	// 启动所有长连接和轮询任务
	for roleID, roleInstance := range m.roles {
		if roleInstance.taskConfig.TaskType == types.TaskTypeLongConnection ||
			roleInstance.taskConfig.TaskType == types.TaskTypePolling {
			m.wg.Add(1)
			go func(rid string, ri *RoleInstance) {
				defer m.wg.Done()

				log.Printf("启动任务: role_id=%s, type=%s", rid, ri.taskConfig.TaskType)
				if err := ri.executor.Execute(m.ctx); err != nil {
					log.Printf("任务执行失败: role_id=%s, error=%v", rid, err)
				}
			}(roleID, roleInstance)
		}
	}

	// 启动Kafka消费者（处理命令式任务）
	if err := m.startConsumer(); err != nil {
		return fmt.Errorf("启动Kafka消费者失败: %w", err)
	}

	log.Printf("Worker管理器启动完成")
	return nil
}

// startConsumer 启动Kafka消费者
func (m *Manager) startConsumer() error {
	consumer, err := kafka.NewConsumer(m.config.Kafka, m)
	if err != nil {
		return err
	}

	m.consumer = consumer
	return m.consumer.Start(m.ctx)
}

// HandleTask 实现TaskHandler接口（处理从Kafka消费的命令式任务）
func (m *Manager) HandleTask(ctx context.Context, taskConfig *types.TaskConfig) error {
	log.Printf("接收到Kafka任务: task_id=%s, type=%s",
		taskConfig.TaskID, taskConfig.TaskType)

	// 为命令式任务创建临时执行器
	// 找到匹配的角色配置（根据data_source_id和protocol）
	var matchedRole *RoleInstance
	for _, role := range m.roles {
		if role.taskConfig.DataSourceID == taskConfig.DataSourceID &&
			role.taskConfig.Protocol == taskConfig.Protocol {
			matchedRole = role
			break
		}
	}

	if matchedRole == nil {
		return fmt.Errorf("未找到匹配的角色配置: data_source_id=%s",
			taskConfig.DataSourceID)
	}

	// 使用匹配角色的protocol和rateLimiter创建执行器
	executor := task.NewTaskExecutor(
		m.config.Worker.ID,
		taskConfig,
		matchedRole.protocol,
		m.producer,
		matchedRole.rateLimiter,
	)

	// 执行任务
	return executor.Execute(ctx)
}

// Stop 停止Worker管理器
func (m *Manager) Stop() error {
	log.Printf("停止Worker管理器")

	// 取消上下文
	m.cancel()

	// 等待所有任务完成
	m.wg.Wait()

	// 关闭所有Protocol
	for roleID, role := range m.roles {
		if err := role.protocol.Close(); err != nil {
			log.Printf("关闭协议失败: role_id=%s, error=%v", roleID, err)
		}
	}

	// 关闭Kafka
	if m.consumer != nil {
		m.consumer.Close()
	}
	if m.producer != nil {
		m.producer.Close()
	}

	log.Printf("Worker管理器已停止")
	return nil
}
