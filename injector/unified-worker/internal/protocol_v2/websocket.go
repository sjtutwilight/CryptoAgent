package protocol_v2

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"unified-worker/pkg/types"

	"github.com/gorilla/websocket"
)

// WebSocketProtocol WebSocket协议（内置重连、心跳）
type WebSocketProtocol struct {
	url       string
	conn      *websocket.Conn
	dataChan  chan []byte
	errChan   chan error
	closeChan chan struct{}
	mu        sync.Mutex

	// 内置能力配置
	reconnectConfig ReconnectConfig
	heartbeatConfig HeartbeatConfig

	// 状态
	connected    bool
	reconnecting bool
}

// ReconnectConfig 重连配置
type ReconnectConfig struct {
	Enabled            bool
	MaxRetries         int
	BackoffBaseSeconds int
	BackoffMaxSeconds  int
}

// HeartbeatConfig 心跳配置
type HeartbeatConfig struct {
	Enabled         bool
	IntervalSeconds int
	TimeoutSeconds  int
}

// NewWebSocketProtocol 创建WebSocket协议
func NewWebSocketProtocol() Protocol {
	return &WebSocketProtocol{
		dataChan:  make(chan []byte, 100),
		errChan:   make(chan error, 10),
		closeChan: make(chan struct{}),
	}
}

// Type 返回协议类型
func (ws *WebSocketProtocol) Type() types.ProtocolType {
	return types.ProtocolWebSocket
}

// Initialize 初始化协议
func (ws *WebSocketProtocol) Initialize(ctx context.Context, config map[string]interface{}) error {
	// 解析URL
	url, ok := config["url"].(string)
	if !ok {
		return fmt.Errorf("缺少url配置")
	}
	ws.url = url

	// 解析重连配置
	if reconnectCfg, ok := config["reconnect"].(map[string]interface{}); ok {
		ws.reconnectConfig = ReconnectConfig{
			Enabled:            getBool(reconnectCfg, "enabled", true),
			MaxRetries:         getInt(reconnectCfg, "max_retries", -1),
			BackoffBaseSeconds: getInt(reconnectCfg, "backoff_base_seconds", 2),
			BackoffMaxSeconds:  getInt(reconnectCfg, "backoff_max_seconds", 60),
		}
	} else {
		// 默认启用重连
		ws.reconnectConfig = ReconnectConfig{
			Enabled:            true,
			MaxRetries:         -1,
			BackoffBaseSeconds: 2,
			BackoffMaxSeconds:  60,
		}
	}

	// 解析心跳配置
	if heartbeatCfg, ok := config["heartbeat"].(map[string]interface{}); ok {
		ws.heartbeatConfig = HeartbeatConfig{
			Enabled:         getBool(heartbeatCfg, "enabled", true),
			IntervalSeconds: getInt(heartbeatCfg, "interval_seconds", 30),
			TimeoutSeconds:  getInt(heartbeatCfg, "timeout_seconds", 60),
		}
	} else {
		// 默认启用心跳
		ws.heartbeatConfig = HeartbeatConfig{
			Enabled:         true,
			IntervalSeconds: 30,
			TimeoutSeconds:  60,
		}
	}

	// 建立连接
	return ws.connect(ctx)
}

// connect 建立WebSocket连接
func (ws *WebSocketProtocol) connect(ctx context.Context) error {
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, ws.url, nil)
	if err != nil {
		return fmt.Errorf("连接失败: %w", err)
	}

	ws.mu.Lock()
	ws.conn = conn
	ws.connected = true
	ws.mu.Unlock()

	log.Printf("[WebSocketProtocol] 连接成功: %s", ws.url)

	// 启动接收协程
	go ws.receiveLoop()

	// 启动心跳协程
	if ws.heartbeatConfig.Enabled {
		go ws.heartbeatLoop()
	}

	return nil
}

// Send 发送消息
func (ws *WebSocketProtocol) Send(ctx context.Context, message []byte) ([]byte, error) {
	ws.mu.Lock()
	defer ws.mu.Unlock()

	if !ws.connected || ws.conn == nil {
		return nil, fmt.Errorf("未连接")
	}

	if err := ws.conn.WriteMessage(websocket.TextMessage, message); err != nil {
		return nil, fmt.Errorf("发送失败: %w", err)
	}

	// TODO: 等待响应（需要请求ID匹配）
	return nil, nil
}

// Receive 接收消息
func (ws *WebSocketProtocol) Receive(ctx context.Context) (<-chan []byte, <-chan error) {
	return ws.dataChan, ws.errChan
}

// receiveLoop 接收循环
func (ws *WebSocketProtocol) receiveLoop() {
	for {
		select {
		case <-ws.closeChan:
			return
		default:
			ws.mu.Lock()
			conn := ws.conn
			ws.mu.Unlock()

			if conn == nil {
				time.Sleep(1 * time.Second)
				continue
			}

			_, message, err := conn.ReadMessage()
			if err != nil {
				log.Printf("[WebSocketProtocol] 接收错误: %v", err)
				ws.handleDisconnect()
				continue
			}

			ws.dataChan <- message
		}
	}
}

// heartbeatLoop 心跳循环
func (ws *WebSocketProtocol) heartbeatLoop() {
	ticker := time.NewTicker(time.Duration(ws.heartbeatConfig.IntervalSeconds) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ws.closeChan:
			return
		case <-ticker.C:
			ws.mu.Lock()
			conn := ws.conn
			ws.mu.Unlock()

			if conn == nil {
				continue
			}

			// 发送Ping
			if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				log.Printf("[WebSocketProtocol] 心跳失败: %v", err)
				ws.handleDisconnect()
			}
		}
	}
}

// handleDisconnect 处理断开连接
func (ws *WebSocketProtocol) handleDisconnect() {
	ws.mu.Lock()
	ws.connected = false
	ws.mu.Unlock()

	// 触发重连
	if ws.reconnectConfig.Enabled && !ws.reconnecting {
		go ws.reconnect()
	}
}

// reconnect 重连逻辑
func (ws *WebSocketProtocol) reconnect() {
	ws.reconnecting = true
	defer func() { ws.reconnecting = false }()

	retries := 0
	backoff := time.Duration(ws.reconnectConfig.BackoffBaseSeconds) * time.Second

	for {
		if ws.reconnectConfig.MaxRetries > 0 && retries >= ws.reconnectConfig.MaxRetries {
			log.Printf("[WebSocketProtocol] 重连失败，达到最大重试次数")
			ws.errChan <- fmt.Errorf("重连失败")
			return
		}

		log.Printf("[WebSocketProtocol] 尝试重连 (第%d次)...", retries+1)

		if err := ws.connect(context.Background()); err != nil {
			log.Printf("[WebSocketProtocol] 重连失败: %v, %v后重试", err, backoff)
			time.Sleep(backoff)

			// 指数退避
			backoff *= 2
			maxBackoff := time.Duration(ws.reconnectConfig.BackoffMaxSeconds) * time.Second
			if backoff > maxBackoff {
				backoff = maxBackoff
			}

			retries++
		} else {
			log.Printf("[WebSocketProtocol] 重连成功")
			return
		}
	}
}

// Close 关闭连接
func (ws *WebSocketProtocol) Close() error {
	close(ws.closeChan)

	ws.mu.Lock()
	defer ws.mu.Unlock()

	if ws.conn != nil {
		ws.conn.Close()
		ws.conn = nil
	}

	ws.connected = false
	return nil
}

// Metadata 返回元数据
func (ws *WebSocketProtocol) Metadata() types.ProtocolMetadata {
	return types.ProtocolMetadata{
		SupportsBidirectional:  true,
		RequiresHeartbeat:      false, // 内置心跳
		RequiresReconnect:      false, // 内置重连
		RequiresConnectionPool: false,
		RequiresRateLimit:      false,
		HasBuiltInReconnect:    true, // 声明内置
		HasBuiltInHeartbeat:    true, // 声明内置
	}
}

// 辅助函数
func getBool(m map[string]interface{}, key string, def bool) bool {
	if v, ok := m[key].(bool); ok {
		return v
	}
	return def
}

func getInt(m map[string]interface{}, key string, def int) int {
	if v, ok := m[key].(int); ok {
		return v
	}
	if v, ok := m[key].(float64); ok {
		return int(v)
	}
	return def
}
