package protocol

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// WebSocketConfig websocket连接配置
type WebSocketConfig struct {
	URL                string // ws://localhost:8090/ws
	HeartbeatMs        int    // 心跳间隔(毫秒)
	BackoffBaseSeconds int    // 重连基础延迟(秒)
	BackoffMaxSeconds  int    // 重连最大延迟(秒)
}

// WebSocketClient websocket客户端，负责连接管理、心跳、重连
type WebSocketClient struct {
	config WebSocketConfig
	conn   *websocket.Conn
	mu     sync.RWMutex

	msgChan chan []byte   // 接收到的消息通道
	errChan chan error    // 错误通道
	closeCh chan struct{} // 关闭信号

	lastSubscribeMsg   []byte
	lastUnsubscribeMsg []byte

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

	log.Printf("[WebSocket] 连接成功: %s", c.config.URL)

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
	c.mu.RLock()
	conn := c.conn
	c.mu.RUnlock()

	if conn == nil {
		return fmt.Errorf("websocket未连接")
	}

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

	if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
		return fmt.Errorf("发送订阅消息失败: %w", err)
	}

	c.mu.Lock()
	c.lastSubscribeMsg = data
	c.mu.Unlock()

	log.Printf("[WebSocket] 发送订阅: %s", string(data))
	return nil
}

// Unsubscribe 发送退订消息（JSON-RPC）
func (c *WebSocketClient) Unsubscribe(req JSONRPCRequest) error {
	c.mu.RLock()
	conn := c.conn
	c.mu.RUnlock()

	if conn == nil {
		return fmt.Errorf("websocket未连接")
	}

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

	if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
		return fmt.Errorf("发送退订消息失败: %w", err)
	}

	c.mu.Lock()
	c.lastUnsubscribeMsg = data
	c.mu.Unlock()

	log.Printf("[WebSocket] 发送退订: %s", string(data))
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

	c.mu.Lock()
	if c.conn != nil {
		_ = c.conn.Close()
		c.conn = nil
	}
	c.mu.Unlock()

	close(c.closeCh)
	c.wg.Wait()

	log.Printf("[WebSocket] 连接已关闭")
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
			c.mu.RLock()
			conn := c.conn
			c.mu.RUnlock()

			if conn == nil {
				continue
			}

			if err := conn.WriteMessage(websocket.PingMessage, []byte{}); err != nil {
				log.Printf("[WebSocket] 心跳失败: %v, 尝试重连", err)
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
				log.Printf("[WebSocket] 读取消息失败: %v, 尝试重连", err)
				c.reconnect()
				continue
			}

			// 将消息发送到通道
			select {
			case c.msgChan <- data:
			case <-c.ctx.Done():
				return
			default:
				log.Printf("[WebSocket] 消息通道已满，丢弃消息")
			}
		}
	}
}

// reconnect 重连逻辑，带指数退避
func (c *WebSocketClient) reconnect() {
	c.mu.Lock()
	if c.conn != nil {
		_ = c.conn.Close()
		c.conn = nil
	}
	c.mu.Unlock()

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

		log.Printf("[WebSocket] 重连尝试 #%d，延迟 %v", attempt, backoff)
		time.Sleep(backoff)

		conn, _, err := websocket.DefaultDialer.Dial(c.config.URL, nil)
		if err != nil {
			log.Printf("[WebSocket] 重连失败: %v", err)
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

		log.Printf("[WebSocket] 重连成功")

		// 重连后尝试恢复订阅
		c.mu.RLock()
		subMsg := c.lastSubscribeMsg
		c.mu.RUnlock()
		if len(subMsg) > 0 {
			if err := conn.WriteMessage(websocket.TextMessage, subMsg); err != nil {
				log.Printf("[WebSocket] 重发订阅失败: %v", err)
			} else {
				log.Printf("[WebSocket] 重发订阅成功")
			}
		}
		return
	}
}

// IsConnected 返回当前是否有活跃连接
func (c *WebSocketClient) IsConnected() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.conn != nil
}

// WriteMessage 发送消息到websocket连接
func (c *WebSocketClient) WriteMessage(data []byte) error {
	c.mu.RLock()
	conn := c.conn
	c.mu.RUnlock()

	if conn == nil {
		return fmt.Errorf("websocket未连接")
	}

	return conn.WriteMessage(websocket.TextMessage, data)
}
