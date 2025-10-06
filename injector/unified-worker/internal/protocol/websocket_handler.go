package protocol

import (
	"context"
	"fmt"
	"sync"

	"unified-worker/internal/runtime"
	"unified-worker/pkg/types"

	"github.com/gorilla/websocket"
)

// WebSocketHandler WebSocket协议处理器
type WebSocketHandler struct {
	url             string
	conn            *websocket.Conn
	reconnectMgr    *runtime.ReconnectManager
	heartbeatMgr    *runtime.HeartbeatManager
	runtimeConfig   types.RuntimeConfig
	dataChan        chan []byte
	errChan         chan error
	stopChan        chan struct{}
	mu              sync.RWMutex
	connected       bool
	subscriptionIDs []string // 订阅ID列表
}

// NewWebSocketHandler 创建WebSocket处理器
func NewWebSocketHandler(runtimeCfg types.RuntimeConfig) *WebSocketHandler {
	return &WebSocketHandler{
		reconnectMgr:  runtime.NewReconnectManager(runtimeCfg.Reconnect),
		heartbeatMgr:  runtime.NewHeartbeatManager(runtimeCfg.Heartbeat),
		runtimeConfig: runtimeCfg,
		dataChan:      make(chan []byte, 100),
		errChan:       make(chan error, 10),
		stopChan:      make(chan struct{}),
		connected:     false,
	}
}

// Type 返回协议类型
func (w *WebSocketHandler) Type() types.ProtocolType {
	return types.ProtocolWebSocket
}

// Initialize 初始化WebSocket处理器
func (w *WebSocketHandler) Initialize(ctx context.Context, config map[string]interface{}) error {
	// 解析配置
	url, ok := config["url"].(string)
	if !ok {
		return fmt.Errorf("缺少url配置")
	}
	w.url = url

	// 连接WebSocket
	return w.connect(ctx)
}
