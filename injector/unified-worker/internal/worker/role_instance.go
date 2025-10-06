package worker

import (
	"unified-worker/internal/runtime"
	"unified-worker/internal/task"
	"unified-worker/pkg/types"
)

// RoleInstance 角色实例
type RoleInstance struct {
	roleID      string                   // 角色ID
	protocol    types.ProtocolHandler    // 协议处理器
	executor    *task.TaskExecutor       // 任务执行器
	rateLimiter runtime.RateLimiter      // 限流器
	taskConfig  *types.TaskConfig        // 任务配置
}

// GetRoleID 获取角色ID
func (ri *RoleInstance) GetRoleID() string {
	return ri.roleID
}

// GetProtocol 获取协议处理器
func (ri *RoleInstance) GetProtocol() types.ProtocolHandler {
	return ri.protocol
}

// GetExecutor 获取任务执行器
func (ri *RoleInstance) GetExecutor() *task.TaskExecutor {
	return ri.executor
}

// GetRateLimiter 获取限流器
func (ri *RoleInstance) GetRateLimiter() runtime.RateLimiter {
	return ri.rateLimiter
}

// GetTaskConfig 获取任务配置
func (ri *RoleInstance) GetTaskConfig() *types.TaskConfig {
	return ri.taskConfig
}
