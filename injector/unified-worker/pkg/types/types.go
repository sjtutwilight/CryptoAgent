package types

import "context"

// ProtocolType 协议类型
type ProtocolType string

const (
	// 原生协议（需要完整Runtime支持）
	ProtocolHTTP      ProtocolType = "http"
	ProtocolWebSocket ProtocolType = "websocket"

	// SDK协议（内置部分Runtime能力）
	ProtocolEthereumSDK ProtocolType = "ethereum-sdk" // go-ethereum
	ProtocolBinanceSDK  ProtocolType = "binance-sdk"  // binance sdk
)

// TaskType 任务类型
type TaskType string

const (
	TaskTypeLongConnection TaskType = "long_connection" // 长连接订阅
	TaskTypePolling        TaskType = "polling"         // 定期轮询
	TaskTypeOneTime        TaskType = "one_time"        // 命令式单次调用
)

// TaskStatus 任务状态
type TaskStatus string

const (
	TaskStatusPending TaskStatus = "pending"
	TaskStatusRunning TaskStatus = "running"
	TaskStatusStopped TaskStatus = "stopped"
	TaskStatusFailed  TaskStatus = "failed"
)

// ProtocolMetadata 协议元数据（用于能力协商）
type ProtocolMetadata struct {
	SupportsBidirectional  bool // 是否支持双向通信
	RequiresHeartbeat      bool // 是否需要心跳
	RequiresReconnect      bool // 是否需要重连
	RequiresConnectionPool bool // 是否需要连接池
	RequiresRateLimit      bool // 是否需要限流

	// SDK内置能力声明
	HasBuiltInReconnect bool // SDK内置重连（如go-ethereum）
	HasBuiltInRateLimit bool // SDK内置限流
	HasBuiltInHeartbeat bool // SDK内置心跳
}

// ProtocolHandler 协议处理器接口
type ProtocolHandler interface {
	// Type 返回协议类型
	Type() ProtocolType

	// Initialize 初始化协议
	Initialize(ctx context.Context, config map[string]interface{}) error

	// Send 发送消息（用于HTTP请求或WebSocket发送）
	Send(ctx context.Context, message []byte) ([]byte, error)

	// Receive 接收消息（用于WebSocket订阅）
	Receive(ctx context.Context) (<-chan []byte, <-chan error)

	// HealthCheck 健康检查
	HealthCheck(ctx context.Context) error

	// Close 关闭连接
	Close() error

	// Metadata 返回协议元数据
	Metadata() ProtocolMetadata
}
