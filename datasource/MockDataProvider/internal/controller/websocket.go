package controller

import (
	"encoding/json"
	"fmt"
	"log"
	"mock-service/internal/config"
	"mock-service/internal/fault"
	"mock-service/internal/generator"
	"mock-service/internal/model"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// WebSocketController WebSocket控制器
type WebSocketController struct {
	upgrader      websocket.Upgrader
	dataGenerator *generator.DataGenerator
	faultInjector *fault.FaultInjector
	config        *config.Config

	// 连接管理
	connections   map[*websocket.Conn]*ConnectionInfo
	connectionsMu sync.RWMutex

	// 区块生成器
	blockTicker *time.Ticker
	stopChan    chan struct{}
}

// ConnectionInfo 连接信息
type ConnectionInfo struct {
	conn          *websocket.Conn
	subscriptions map[string]string // subscription_id -> subscription_type
	mu            sync.RWMutex
}

// NewWebSocketController 创建新的WebSocket控制器
func NewWebSocketController(cfg *config.Config, dataGen *generator.DataGenerator, faultInj *fault.FaultInjector) *WebSocketController {
	return &WebSocketController{
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
				return true // 允许所有跨域请求
			},
		},
		dataGenerator: dataGen,
		faultInjector: faultInj,
		config:        cfg,
		connections:   make(map[*websocket.Conn]*ConnectionInfo),
		stopChan:      make(chan struct{}),
	}
}

// Start 启动WebSocket控制器
func (c *WebSocketController) Start() {
	// 启动区块生成器
	c.blockTicker = time.NewTicker(time.Duration(c.config.Data.Ethereum.BlockInterval) * time.Second)
	go c.blockGenerator()

	log.Printf("WebSocket控制器启动成功，区块间隔: %d秒", c.config.Data.Ethereum.BlockInterval)
}

// Stop 停止WebSocket控制器
func (c *WebSocketController) Stop() {
	close(c.stopChan)
	if c.blockTicker != nil {
		c.blockTicker.Stop()
	}

	// 关闭所有连接
	c.connectionsMu.Lock()
	for conn := range c.connections {
		conn.Close()
	}
	c.connectionsMu.Unlock()

	log.Println("WebSocket控制器已停止")
}

// HandleWebSocket 处理WebSocket连接
func (c *WebSocketController) HandleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := c.upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("WebSocket升级失败: %v", err)
		return
	}

	// 创建连接信息
	connInfo := &ConnectionInfo{
		conn:          conn,
		subscriptions: make(map[string]string),
	}

	// 添加到连接管理
	c.connectionsMu.Lock()
	c.connections[conn] = connInfo
	c.connectionsMu.Unlock()

	log.Printf("新的WebSocket连接建立: %s", conn.RemoteAddr())

	// 启动消息处理
	go c.handleConnection(connInfo)
}

// handleConnection 处理单个连接
func (c *WebSocketController) handleConnection(connInfo *ConnectionInfo) {
	defer func() {
		// 清理连接
		c.connectionsMu.Lock()
		delete(c.connections, connInfo.conn)
		c.connectionsMu.Unlock()

		connInfo.conn.Close()
		log.Printf("WebSocket连接关闭: %s", connInfo.conn.RemoteAddr())
	}()

	// 设置心跳
	connInfo.conn.SetPongHandler(func(string) error {
		connInfo.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	// 启动ping定时器
	pingTicker := time.NewTicker(30 * time.Second)
	defer pingTicker.Stop()

	go func() {
		for {
			select {
			case <-pingTicker.C:
				// 检查是否应该注入心跳异常
				if c.faultInjector.ShouldInjectWebSocketHeartbeatAnomaly() {
					log.Printf("注入心跳异常故障，忽略ping消息")
					continue
				}

				if err := connInfo.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
					log.Printf("发送ping消息失败: %v", err)
					return
				}
			case <-c.stopChan:
				return
			}
		}
	}()

	// 消息处理循环
	for {
		// 检查是否应该注入断开连接故障
		if c.faultInjector.ShouldInjectWebSocketDisconnection() {
			log.Printf("注入连接断开故障，主动关闭连接")
			break
		}

		_, message, err := connInfo.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("WebSocket读取错误: %v", err)
			}
			break
		}

		// 处理消息
		c.handleMessage(connInfo, message)
	}
}

// handleMessage 处理接收到的消息
func (c *WebSocketController) handleMessage(connInfo *ConnectionInfo, message []byte) {
	var request model.JSONRPCRequest
	if err := json.Unmarshal(message, &request); err != nil {
		log.Printf("解析JSON-RPC请求失败: %v", err)
		c.sendError(connInfo, nil, -32700, "Parse error")
		return
	}

	log.Printf("收到请求: %s, ID: %v", request.Method, request.ID)

	switch request.Method {
	case "eth_subscribe":
		c.handleSubscribe(connInfo, &request)
	case "eth_unsubscribe":
		c.handleUnsubscribe(connInfo, &request)
	default:
		c.sendError(connInfo, request.ID, -32601, "Method not found")
	}
}

// handleSubscribe 处理订阅请求
func (c *WebSocketController) handleSubscribe(connInfo *ConnectionInfo, request *model.JSONRPCRequest) {
	var params []interface{}
	if err := json.Unmarshal(request.Params, &params); err != nil || len(params) == 0 {
		c.sendError(connInfo, request.ID, -32602, "Invalid params")
		return
	}

	subscriptionType, ok := params[0].(string)
	if !ok {
		c.sendError(connInfo, request.ID, -32602, "Invalid subscription type")
		return
	}

	if subscriptionType != "newHeads" {
		c.sendError(connInfo, request.ID, -32602, "Unsupported subscription type")
		return
	}

	// 生成订阅ID
	subscriptionID := fmt.Sprintf("0x%x", time.Now().UnixNano())

	// 添加订阅
	connInfo.mu.Lock()
	connInfo.subscriptions[subscriptionID] = subscriptionType
	connInfo.mu.Unlock()

	// 发送响应
	response := model.JSONRPCResponse{
		ID:      request.ID,
		Result:  subscriptionID,
		JSONRpc: "2.0",
	}

	c.sendResponse(connInfo, &response)
	log.Printf("订阅成功: %s, ID: %s", subscriptionType, subscriptionID)
}

// handleUnsubscribe 处理取消订阅请求
func (c *WebSocketController) handleUnsubscribe(connInfo *ConnectionInfo, request *model.JSONRPCRequest) {
	var params []interface{}
	if err := json.Unmarshal(request.Params, &params); err != nil || len(params) == 0 {
		c.sendError(connInfo, request.ID, -32602, "Invalid params")
		return
	}

	subscriptionID, ok := params[0].(string)
	if !ok {
		c.sendError(connInfo, request.ID, -32602, "Invalid subscription ID")
		return
	}

	// 移除订阅
	connInfo.mu.Lock()
	_, exists := connInfo.subscriptions[subscriptionID]
	if exists {
		delete(connInfo.subscriptions, subscriptionID)
	}
	connInfo.mu.Unlock()

	// 发送响应
	response := model.JSONRPCResponse{
		ID:      request.ID,
		Result:  exists,
		JSONRpc: "2.0",
	}

	c.sendResponse(connInfo, &response)
	log.Printf("取消订阅: %s, 成功: %t", subscriptionID, exists)
}

// sendResponse 发送响应
func (c *WebSocketController) sendResponse(connInfo *ConnectionInfo, response *model.JSONRPCResponse) {
	data, err := json.Marshal(response)
	if err != nil {
		log.Printf("序列化响应失败: %v", err)
		return
	}

	if err := connInfo.conn.WriteMessage(websocket.TextMessage, data); err != nil {
		log.Printf("发送响应失败: %v", err)
	}
}

// sendError 发送错误响应
func (c *WebSocketController) sendError(connInfo *ConnectionInfo, id interface{}, code int, message string) {
	response := model.JSONRPCResponse{
		ID:      id,
		Error:   &model.JSONRPCError{Code: code, Message: message},
		JSONRpc: "2.0",
	}

	c.sendResponse(connInfo, &response)
}

// blockGenerator 区块生成器
func (c *WebSocketController) blockGenerator() {
	for {
		select {
		case <-c.blockTicker.C:
			c.generateAndBroadcastBlock()
		case <-c.stopChan:
			return
		}
	}
}

// generateAndBroadcastBlock 生成并广播新区块
func (c *WebSocketController) generateAndBroadcastBlock() {
	block := c.dataGenerator.GenerateNextBlock()

	log.Printf("生成新区块: %s, 高度: %s", block.Hash, block.Number)

	// 广播到所有订阅了newHeads的连接
	c.connectionsMu.RLock()
	defer c.connectionsMu.RUnlock()

	for conn, connInfo := range c.connections {
		connInfo.mu.RLock()
		for subscriptionID, subscriptionType := range connInfo.subscriptions {
			if subscriptionType == "newHeads" {
				// 检查是否应该注入数据丢失故障
				if c.faultInjector.ShouldInjectWebSocketDataLoss() {
					log.Printf("注入数据丢失故障，跳过发送区块到连接: %s", conn.RemoteAddr())
					continue
				}

				// 创建通知消息
				notification := model.NewHeadsNotification{
					JSONRpc: "2.0",
					Method:  "eth_subscription",
					Params: struct {
						Subscription string            `json:"subscription"`
						Result       model.BlockHeader `json:"result"`
					}{
						Subscription: subscriptionID,
						Result:       *block,
					},
				}

				// 发送通知
				data, err := json.Marshal(notification)
				if err != nil {
					log.Printf("序列化区块通知失败: %v", err)
					continue
				}

				if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
					log.Printf("发送区块通知失败: %v", err)
					continue
				}
			}
		}
		connInfo.mu.RUnlock()
	}
}
