package chain

import (
	"context"
	"fmt"
	"log"

	"unified-worker/internal/config"
	"unified-worker/internal/protocol"
	"unified-worker/pkg/types"
)

// ProtocolHandler 协议初始化处理器
type ProtocolHandler struct {
	BaseHandler
}

// NewProtocolHandler 创建协议处理器
func NewProtocolHandler() *ProtocolHandler {
	return &ProtocolHandler{
		BaseHandler: *NewBaseHandler("ProtocolHandler"),
	}
}

// Handle 处理协议初始化
func (h *ProtocolHandler) Handle(ctx context.Context, req *Request) error {
	roleConfig, ok := req.RoleConfig.(config.RoleConfig)
	if !ok {
		return fmt.Errorf("[%s] 无效的角色配置类型", h.GetName())
	}

	log.Printf("[%s] 初始化协议: %s", h.GetName(), roleConfig.Protocol)

	// 转换Runtime配置
	runtimeConfig := config.ConvertToRuntimeConfig(roleConfig.RuntimeConfig)
	req.Data["runtime_config"] = runtimeConfig

	// 创建Protocol Handler
	var protocolHandler types.ProtocolHandler
	switch roleConfig.Protocol {
	case "http":
		protocolHandler = protocol.NewHTTPHandler(runtimeConfig)
	case "websocket":
		protocolHandler = protocol.NewWebSocketHandler(runtimeConfig)
	case "ethereum-sdk":
		protocolHandler = protocol.NewEthereumSDKHandler()
		log.Printf("[%s] 使用Ethereum SDK协议（内置重连、心跳、连接池）", h.GetName())
	default:
		return fmt.Errorf("[%s] 不支持的协议: %s", h.GetName(), roleConfig.Protocol)
	}

	// 初始化Protocol
	if err := protocolHandler.Initialize(ctx, roleConfig.ProtocolConfig); err != nil {
		return fmt.Errorf("[%s] 初始化协议失败: %w", h.GetName(), err)
	}

	req.Data["protocol"] = protocolHandler

	log.Printf("[%s] 协议初始化成功: %s", h.GetName(), roleConfig.Protocol)

	// 继续下一个处理器
	return h.CallNext(ctx, req)
}
