package protocol

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/observability/logging"
)

// WebSocketConfig websocket连接配置
type WebSocketConfig struct {
	URL                string // ws://localhost:8090/ws
	HeartbeatMs        int    // 心跳间隔(毫秒)
	BackoffBaseSeconds int    // 重连基础延迟(秒)
	BackoffMaxSeconds  int    // 重连最大延迟(秒)
	HeartbeatPayload   []byte // 自定义心跳载荷（留空则发送 Ping frame）
	HeartbeatOpcode    int    // websocket 消息类型，默认 websocket.PingMessage
}

// WebSocketClient websocket客户端，负责连接管理、心跳、重连
type WebSocketClient struct {
	config WebSocketConfig
	conn   *websocket.Conn
	mu     sync.RWMutex
	writeMu sync.Mutex

	msgChan chan []byte   // 接收到的消息通道
	errChan chan error    // 错误通道
	closeCh chan struct{} // 关闭信号
	subMu   sync.Mutex    // 订阅消息记录锁
	unsubMu sync.Mutex    // 退订消息记录锁

	lastSubscribeMsgs   [][]byte
	lastUnsubscribeMsgs [][]byte

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewWebSocketClient 创建websocket客户端
func NewWebSocketClient(cfg WebSocketConfig) *WebSocketClient {
	ctx, cancel := context.WithCancel(context.Background())

	// 设置默认值
	if cfg.HeartbeatMs <= 0 {
		cfg.HeartbeatMs = 30000
	}
	if cfg.BackoffBaseSeconds <= 0 {
		cfg.BackoffBaseSeconds = 2
	}
	if cfg.BackoffMaxSeconds <= 0 {
		cfg.BackoffMaxSeconds = 60
	}
	if cfg.HeartbeatOpcode == 0 {
		cfg.HeartbeatOpcode = websocket.PingMessage
	}

	return &WebSocketClient{
		config:  cfg,
		msgChan: make(chan []byte, 100),
		errChan: make(chan error, 10),
		closeCh: make(chan struct{}),
		ctx:     ctx,
		cancel:  cancel,
	}
}

// Connect 建立websocket连接并启动心跳、读取协程
func (c *WebSocketClient) Connect() error {
	conn, _, err := websocket.DefaultDialer.Dial(c.config.URL, nil)
	if err != nil {
		return fmt.Errorf("websocket连接失败: %w", err)
	}

	c.mu.Lock()
	c.conn = conn
	c.mu.Unlock()

	logging.Info(context.Background(), logging.EventWSConnect, "websocket connected", logging.Fields{
		"ws_url": c.config.URL,
	})

	// 启动心跳
	c.wg.Add(1)
	go c.heartbeatLoop()

	// 启动读取消息
	c.wg.Add(1)
	go c.readLoop()

	return nil
}

// JSONRPCRequest 通用 JSON-RPC 请求结构
// Subscribe 发送订阅消息（JSON-RPC）
func (c *WebSocketClient) Subscribe(req JSONRPCRequest) error {
	if req.JSONRPC == "" {
		req.JSONRPC = "2.0"
	}
	if req.Method == "" {
		return fmt.Errorf("websocket订阅缺少method")
	}

	data, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("序列化订阅消息失败: %w", err)
	}

	if err := c.writeMessage(websocket.TextMessage, data); err != nil {
		return fmt.Errorf("发送订阅消息失败: %w", err)
	}
	payloadCopy := make([]byte, len(data))
	copy(payloadCopy, data)
	c.subMu.Lock()
	c.lastSubscribeMsgs = [][]byte{payloadCopy}
	c.subMu.Unlock()

	logging.Info(context.Background(), logging.EventWSSubscribeSent, "websocket subscribe sent", logging.Fields{
		"ws_url":      c.config.URL,
		"payload":     truncateBytes(data, 256),
		"payload_len": len(data),
	})
	return nil
}

// SendRawSubscribe 发送原生订阅payload，并记录用于重连
func (c *WebSocketClient) SendRawSubscribe(payload []byte) error {
	if len(payload) == 0 {
		return fmt.Errorf("订阅消息为空")
	}

	if err := c.writeMessage(websocket.TextMessage, payload); err != nil {
		return fmt.Errorf("发送订阅消息失败: %w", err)
	}

	c.subMu.Lock()
	payloadCopy := make([]byte, len(payload))
	copy(payloadCopy, payload)
	c.lastSubscribeMsgs = append(c.lastSubscribeMsgs, payloadCopy)
	c.subMu.Unlock()

	logging.Info(context.Background(), logging.EventWSSubscribeSent, "websocket subscribe sent", logging.Fields{
		"ws_url":      c.config.URL,
		"payload":     truncateBytes(payload, 256),
		"payload_len": len(payload),
		"raw":         true,
	})
	return nil
}

// SendRawSubscribes 批量发送原生订阅，并记录以便重连
func (c *WebSocketClient) SendRawSubscribes(payloads [][]byte) error {
	if len(payloads) == 0 {
		return fmt.Errorf("订阅消息为空")
	}
	c.subMu.Lock()
	c.lastSubscribeMsgs = c.lastSubscribeMsgs[:0]
	c.subMu.Unlock()
	for _, payload := range payloads {
		if len(payload) == 0 {
			continue
		}
		if err := c.SendRawSubscribe(payload); err != nil {
			return err
		}
	}
	return nil
}

// Unsubscribe 发送退订消息（JSON-RPC）
func (c *WebSocketClient) Unsubscribe(req JSONRPCRequest) error {
	if req.JSONRPC == "" {
		req.JSONRPC = "2.0"
	}
	if req.Method == "" {
		return fmt.Errorf("websocket退订缺少method")
	}

	data, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("序列化退订消息失败: %w", err)
	}

	if err := c.writeMessage(websocket.TextMessage, data); err != nil {
		return fmt.Errorf("发送退订消息失败: %w", err)
	}

	payloadCopy := make([]byte, len(data))
	copy(payloadCopy, data)
	c.unsubMu.Lock()
	c.lastUnsubscribeMsgs = [][]byte{payloadCopy}
	c.unsubMu.Unlock()

	logging.Info(context.Background(), logging.EventWSUnsubscribeSent, "websocket unsubscribe sent", logging.Fields{
		"ws_url":      c.config.URL,
		"payload":     truncateBytes(data, 256),
		"payload_len": len(data),
	})
	return nil
}

// MessageChan 返回接收消息的通道
func (c *WebSocketClient) MessageChan() <-chan []byte {
	return c.msgChan
}

// ErrorChan 返回错误通道
func (c *WebSocketClient) ErrorChan() <-chan error {
	return c.errChan
}

// Close 关闭websocket连接
func (c *WebSocketClient) Close() error {
	c.cancel()

	c.writeMu.Lock()
	c.mu.Lock()
	if c.conn != nil {
		_ = c.conn.Close()
		c.conn = nil
	}
	c.mu.Unlock()
	c.writeMu.Unlock()

	close(c.closeCh)
	c.wg.Wait()

	logging.Info(context.Background(), logging.EventWSClose, "websocket closed", logging.Fields{
		"ws_url": c.config.URL,
	})
	return nil
}

// heartbeatLoop 心跳循环，定期发送ping消息
func (c *WebSocketClient) heartbeatLoop() {
	defer c.wg.Done()

	ticker := time.NewTicker(time.Duration(c.config.HeartbeatMs) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-c.closeCh:
			return
		case <-ticker.C:
			if len(c.config.HeartbeatPayload) > 0 {
				if err := c.writeMessage(c.config.HeartbeatOpcode, c.config.HeartbeatPayload); err != nil {
					logging.Warn(context.Background(), logging.EventWSHeartbeatError, "websocket heartbeat failed", logging.Fields{
						"ws_url": c.config.URL,
						"error":  err.Error(),
					})
					c.reconnect()
				}
				continue
			}

			if err := c.writeMessage(websocket.PingMessage, []byte{}); err != nil {
				logging.Warn(context.Background(), logging.EventWSHeartbeatError, "websocket heartbeat failed", logging.Fields{
					"ws_url": c.config.URL,
					"error":  err.Error(),
				})
				c.reconnect()
			}
		}
	}
}

// readLoop 读取消息循环
func (c *WebSocketClient) readLoop() {
	defer c.wg.Done()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-c.closeCh:
			return
		default:
			c.mu.RLock()
			conn := c.conn
			c.mu.RUnlock()

			if conn == nil {
				time.Sleep(time.Second)
				continue
			}

			_, data, err := conn.ReadMessage()
			if err != nil {
				logging.Warn(context.Background(), logging.EventWSReadError, "websocket read failed", logging.Fields{
					"ws_url": c.config.URL,
					"error":  err.Error(),
				})
				c.reconnect()
				continue
			}

			// 将消息发送到通道
			select {
			case c.msgChan <- data:
			case <-c.ctx.Done():
				return
			default:
				logging.Warn(context.Background(), logging.EventWSBufferDrop, "websocket message buffer full, dropping message", logging.Fields{
					"ws_url": c.config.URL,
					"buffer": cap(c.msgChan),
				})
			}
		}
	}
}

// reconnect 重连逻辑，带指数退避
func (c *WebSocketClient) reconnect() {
	c.writeMu.Lock()
	c.mu.Lock()
	if c.conn != nil {
		_ = c.conn.Close()
		c.conn = nil
	}
	c.mu.Unlock()
	c.writeMu.Unlock()

	backoff := time.Duration(c.config.BackoffBaseSeconds) * time.Second
	maxBackoff := time.Duration(c.config.BackoffMaxSeconds) * time.Second

	for attempt := 1; ; attempt++ {
		select {
		case <-c.ctx.Done():
			return
		case <-c.closeCh:
			return
		default:
		}

		logging.Info(context.Background(), logging.EventWSReconnectStart, "websocket reconnect attempt", logging.Fields{
			"ws_url":    c.config.URL,
			"attempt":   attempt,
			"backoff_ms": backoff.Milliseconds(),
		})
		time.Sleep(backoff)

		conn, _, err := websocket.DefaultDialer.Dial(c.config.URL, nil)
		if err != nil {
			logging.Warn(context.Background(), logging.EventWSReconnectError, "websocket reconnect failed", logging.Fields{
				"ws_url": c.config.URL,
				"error":  err.Error(),
			})
			// 指数退避
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
			continue
		}

		c.mu.Lock()
		c.conn = conn
		c.mu.Unlock()

		logging.Info(context.Background(), logging.EventWSReconnectSuccess, "websocket reconnected", logging.Fields{
			"ws_url": c.config.URL,
		})

		// 重连后尝试恢复订阅
		c.subMu.Lock()
		subMsgs := make([][]byte, len(c.lastSubscribeMsgs))
		for i, payload := range c.lastSubscribeMsgs {
			if len(payload) == 0 {
				continue
			}
			copied := make([]byte, len(payload))
			copy(copied, payload)
			subMsgs[i] = copied
		}
		c.subMu.Unlock()
		for _, payload := range subMsgs {
			if len(payload) == 0 {
				continue
			}
			if err := c.writeMessage(websocket.TextMessage, payload); err != nil {
				logging.Warn(context.Background(), logging.EventWSSubscribeRetryErr, "websocket resubscribe failed", logging.Fields{
					"ws_url": c.config.URL,
					"error":  err.Error(),
				})
			} else {
				logging.Info(context.Background(), logging.EventWSSubscribeRetryOK, "websocket resubscribe ok", logging.Fields{
					"ws_url": c.config.URL,
				})
			}
		}
		return
	}
}

func truncateBytes(data []byte, limit int) string {
	if limit <= 0 || len(data) <= limit {
		return string(data)
	}
	return string(data[:limit])
}

// IsConnected 返回当前是否有活跃连接
func (c *WebSocketClient) IsConnected() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.conn != nil
}

// WriteMessage 发送消息到websocket连接
func (c *WebSocketClient) WriteMessage(data []byte) error {
	return c.writeMessage(websocket.TextMessage, data)
}

func (c *WebSocketClient) writeMessage(messageType int, data []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	c.mu.RLock()
	conn := c.conn
	c.mu.RUnlock()

	if conn == nil {
		return fmt.Errorf("websocket未连接")
	}

	return conn.WriteMessage(messageType, data)
}
