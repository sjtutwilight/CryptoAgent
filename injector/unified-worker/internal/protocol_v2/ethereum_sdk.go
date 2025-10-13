package protocol_v2

import (
	"context"

	"unified-worker/pkg/types"

	"github.com/ethereum/go-ethereum/ethclient"
)

// EthereumSDKProtocol以太坊SDK协议（内置所有能力）
type EthereumSDKProtocol struct {
	client *ethclient.Client
}

// NewEthereumSDKProtocol 创建SDK协议
func NewEthereumSDKProtocol() Protocol {
	return &EthereumSDKProtocol{}
}

// Type 返回协议类型
func (e *EthereumSDKProtocol) Type() types.ProtocolType {
	return types.ProtocolEthereumSDK
}

// Initialize 初始化
func (e *EthereumSDKProtocol) Initialize(ctx context.Context, config map[string]interface{}) error {
	endpoint := config["endpoint"].(string)
	client, err := ethclient.DialContext(ctx, endpoint)
	if err != nil {
		return err
	}
	e.client = client
	return nil
}

// Send 发送请求
func (e *EthereumSDKProtocol) Send(ctx context.Context, message []byte) ([]byte, error) {
	// TODO: 实现JSON-RPC调用
	return nil, nil
}

// Receive SDK不支持
func (e *EthereumSDKProtocol) Receive(ctx context.Context) (<-chan []byte, <-chan error) {
	errChan := make(chan error, 1)
	errChan <- nil
	return nil, errChan
}

// Close 关闭
func (e *EthereumSDKProtocol) Close() error {
	if e.client != nil {
		e.client.Close()
	}
	return nil
}

// Metadata 返回元数据
func (e *EthereumSDKProtocol) Metadata() types.ProtocolMetadata {
	return types.ProtocolMetadata{
		SupportsBidirectional:  false,
		RequiresHeartbeat:      false,
		RequiresReconnect:      false,
		RequiresConnectionPool: false,
		RequiresRateLimit:      true,
		HasBuiltInReconnect:    true,
		HasBuiltInHeartbeat:    true,
		HasBuiltInRateLimit:    false,
	}
}

// GetClient 获取底层eth client（给Fetcher使用）
func (e *EthereumSDKProtocol) GetClient() *ethclient.Client {
	// TODO: 暴露client
	return nil
}
