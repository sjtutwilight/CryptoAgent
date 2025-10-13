package protocol_v2

import (
	"context"
	
	"unified-worker/pkg/types"
)

// Protocol 协议接口（重连、心跳内置）
type Protocol interface {
	// Type 返回协议类型
	Type() types.ProtocolType
	
	// Initialize 初始化协议
	Initialize(ctx context.Context, config map[string]interface{}) error
	
	// Send 发送请求
	Send(ctx context.Context, message []byte) ([]byte, error)
	
	// Receive 接收消息（长连接）
	Receive(ctx context.Context) (<-chan []byte, <-chan error)
	
	// Close 关闭连接
	Close() error
	
	// Metadata 返回协议元数据（用于Resource判断）
	Metadata() types.ProtocolMetadata
}

// ProtocolFactory 协议工厂
type ProtocolFactory struct {
	creators map[string]func() Protocol
}

// NewProtocolFactory 创建工厂
func NewProtocolFactory() *ProtocolFactory {
	pf := &ProtocolFactory{
		creators: make(map[string]func() Protocol),
	}
	
	// 注册协议
	pf.Register("http", func() Protocol { return NewHTTPProtocol() })
	pf.Register("websocket", func() Protocol { return NewWebSocketProtocol() })
	pf.Register("ethereum-sdk", func() Protocol { return NewEthereumSDKProtocol() })
	
	return pf
}

// Register 注册协议
func (pf *ProtocolFactory) Register(name string, creator func() Protocol) {
	pf.creators[name] = creator
}

// Create 创建协议实例
func (pf *ProtocolFactory) Create(name string) (Protocol, error) {
	creator, exists := pf.creators[name]
	if !exists {
		return nil, nil
	}
	
	return creator(), nil
}
