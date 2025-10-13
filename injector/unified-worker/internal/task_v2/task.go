package task_v2

import (
	"context"

	"unified-worker/internal/fetcher"
	"unified-worker/internal/handler"
	"unified-worker/internal/protocol_v2"
	"unified-worker/internal/resource"
)

// Task 任务接口
type Task interface {
	// Start 启动任务
	Start(ctx context.Context) error

	// Stop 停止任务
	Stop() error

	// Type 返回任务类型
	Type() string
}

// TaskContext 任务上下文
type TaskContext struct {
	Protocol protocol_v2.Protocol
	Resource *resource.ResourceManager
	Fetcher  fetcher.DataFetcher
	Handler  handler.Handler
}

// TaskFactory 任务工厂
type TaskFactory struct{}

// NewTaskFactory 创建任务工厂
func NewTaskFactory() *TaskFactory {
	return &TaskFactory{}
}

// Create 创建任务
func (tf *TaskFactory) Create(taskType string, ctx *TaskContext, config map[string]interface{}) (Task, error) {
	switch taskType {
	case "polling":
		return NewPollingTask(ctx, config), nil
	case "long_connection":
		return NewLongConnectionTask(ctx, config), nil
	// case "one_time":
	// 	return NewOneTimeTask(ctx, config), nil
	default:
		return nil, nil
	}
}
