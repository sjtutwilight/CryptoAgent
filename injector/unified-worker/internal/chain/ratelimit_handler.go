package chain

import (
	"context"
	"fmt"
	"log"

	"unified-worker/internal/config"
	"unified-worker/internal/runtime"
	"unified-worker/pkg/types"
)

// RateLimitHandler 限流器处理器
type RateLimitHandler struct {
	BaseHandler
}

// NewRateLimitHandler 创建限流处理器
func NewRateLimitHandler() *RateLimitHandler {
	return &RateLimitHandler{
		BaseHandler: *NewBaseHandler("RateLimitHandler"),
	}
}

// Handle 处理限流器创建
func (h *RateLimitHandler) Handle(ctx context.Context, req *Request) error {
	roleConfig, ok := req.RoleConfig.(config.RoleConfig)
	if !ok {
		return fmt.Errorf("[%s] 无效的角色配置类型", h.GetName())
	}

	// 获取runtime配置
	runtimeConfig, ok := req.Data["runtime_config"].(types.RuntimeConfig)
	if !ok {
		return fmt.Errorf("[%s] 缺少runtime配置", h.GetName())
	}

	// 检查是否需要限流器（传入req以访问protocol）
	needsRateLimit := h.shouldCreateRateLimiter(req, roleConfig, runtimeConfig)

	if !needsRateLimit {
		log.Printf("[%s] 跳过限流器创建: protocol=%s, task_type=%s",
			h.GetName(), roleConfig.Protocol, roleConfig.TaskType)
		req.Data["rate_limiter"] = nil
		return h.CallNext(ctx, req)
	}

	log.Printf("[%s] 创建限流器: protocol=%s", h.GetName(), roleConfig.Protocol)

	// 创建限流器
	if runtimeConfig.RateLimit.Enabled {
		rateLimiter, err := runtime.NewTokenBucketRateLimiter(runtimeConfig.RateLimit)
		if err != nil {
			return fmt.Errorf("[%s] 创建限流器失败: %w", h.GetName(), err)
		}
		req.Data["rate_limiter"] = rateLimiter
		log.Printf("[%s] 限流器创建成功", h.GetName())
	} else {
		req.Data["rate_limiter"] = nil
		log.Printf("[%s] 限流已禁用", h.GetName())
	}

	// 继续下一个处理器
	return h.CallNext(ctx, req)
}

// shouldCreateRateLimiter 判断是否需要创建限流器（优化：基于Protocol Metadata动态判断）
func (h *RateLimitHandler) shouldCreateRateLimiter(req *Request, roleConfig config.RoleConfig, runtimeConfig types.RuntimeConfig) bool {
	// 优先使用Protocol的能力声明
	if protocolHandler, ok := req.Data["protocol"].(types.ProtocolHandler); ok {
		metadata := protocolHandler.Metadata()

		// 如果SDK内置了限流，跳过
		if metadata.HasBuiltInRateLimit {
			log.Printf("[%s] SDK内置限流，跳过创建", h.GetName())
			return false
		}

		// 如果协议不需要限流，跳过
		if !metadata.RequiresRateLimit {
			log.Printf("[%s] 协议不需要限流，跳过创建", h.GetName())
			return false
		}
	}

	// 兜底逻辑：基于协议类型和任务类型判断
	// WebSocket长连接不需要限流器（数据是推送的）
	if roleConfig.Protocol == "websocket" && roleConfig.TaskType == "long_connection" {
		return false
	}

	// HTTP轮询和命令式任务需要限流器
	if roleConfig.Protocol == "http" {
		return true
	}

	// SDK协议：检查是否内置限流
	if roleConfig.Protocol == "ethereum-sdk" || roleConfig.Protocol == "binance-sdk" {
		// SDK协议通常内置连接管理，但需要业务层限流
		return true
	}

	// 默认：如果配置中启用了限流，就创建
	return runtimeConfig.RateLimit.Enabled
}
