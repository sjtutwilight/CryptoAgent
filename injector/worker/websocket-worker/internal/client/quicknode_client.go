package client

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"websocket-worker/internal/config"
	"websocket-worker/internal/connection"
	"websocket-worker/internal/producer"
	"websocket-worker/internal/util"

	"github.com/sirupsen/logrus"
)

// QuicknodeClient QuickNode WebSocket客户端
type QuicknodeClient struct {
	config   *config.Config
	logger   *logrus.Logger
	producer producer.DataProducer
	connMgr  *connection.ConnectionManager
	ctx      context.Context
	cancel   context.CancelFunc
	subID    int
}

// NewQuicknodeClient 创建新的QuickNode客户端
func NewQuicknodeClient(cfg *config.Config, logger *logrus.Logger, producer producer.DataProducer) *QuicknodeClient {
	ctx, cancel := context.WithCancel(context.Background())

	// 创建连接管理器
	headers := http.Header{}
	if cfg.Websocket.Quicknode.ApiKey != "" {
		headers.Set("Authorization", "Bearer "+cfg.Websocket.Quicknode.ApiKey)
	}

	connMgr := connection.NewConnectionManager(cfg, cfg.Websocket.Quicknode.URL, headers)

	client := &QuicknodeClient{
		config:   cfg,
		logger:   logger,
		producer: producer,
		connMgr:  connMgr,
		ctx:      ctx,
		cancel:   cancel,
		subID:    1,
	}

	// 设置消息和错误处理器
	connMgr.SetMessageHandler(client.handleMessage)
	connMgr.SetErrorHandler(client.handleError)

	return client
}

// Start 启动QuickNode客户端
func (qc *QuicknodeClient) Start() error {
	qc.logger.Info("启动QuickNode WebSocket客户端")

	// 连接到QuickNode WebSocket
	if err := qc.connMgr.Connect(); err != nil {
		return fmt.Errorf("连接QuickNode WebSocket失败: %w", err)
	}

	// 发送订阅消息
	if err := qc.subscribe(); err != nil {
		return fmt.Errorf("订阅QuickNode流失败: %w", err)
	}

	qc.logger.Info("QuickNode WebSocket客户端启动成功")
	return nil
}

// Stop 停止QuickNode客户端
func (qc *QuicknodeClient) Stop() {
	qc.logger.Info("停止QuickNode WebSocket客户端")
	qc.cancel()
	qc.connMgr.Disconnect()
}

// subscribe 订阅数据流
func (qc *QuicknodeClient) subscribe() error {
	// 订阅newHeads（新区块头）
	for _, subscription := range qc.config.Websocket.Quicknode.Subscriptions {
		if err := qc.subscribeToEvent(subscription); err != nil {
			return fmt.Errorf("订阅%s失败: %w", subscription, err)
		}
	}
	return nil
}

// subscribeToEvent 订阅特定事件
func (qc *QuicknodeClient) subscribeToEvent(eventType string) error {
	subMsg := QuicknodeSubscribeMessage{
		ID:     qc.subID,
		Method: "eth_subscribe",
		Params: []interface{}{eventType},
	}
	qc.subID++

	msgBytes, err := json.Marshal(subMsg)
	if err != nil {
		return fmt.Errorf("序列化订阅消息失败: %w", err)
	}

	if err := qc.connMgr.SendMessage(msgBytes); err != nil {
		return fmt.Errorf("发送订阅消息失败: %w", err)
	}

	qc.logger.WithField("eventType", eventType).Info("发送订阅消息")
	return nil
}

// handleMessage 处理接收到的消息
func (qc *QuicknodeClient) handleMessage(data []byte) {
	qc.logger.WithField("size", len(data)).Debug("收到QuickNode消息")

	// 尝试解析为订阅确认消息
	var subResponse QuicknodeSubscribeResponse
	if err := json.Unmarshal(data, &subResponse); err == nil && subResponse.ID != 0 {
		qc.handleSubscribeResponse(&subResponse)
		return
	}

	// 尝试解析为通知消息
	var notification QuicknodeNotification
	if err := json.Unmarshal(data, &notification); err == nil && notification.Method != "" {
		qc.handleNotification(&notification)
		return
	}

	qc.logger.WithField("data", string(data)).Warn("未知的QuickNode消息格式")
}

// handleSubscribeResponse 处理订阅响应
func (qc *QuicknodeClient) handleSubscribeResponse(response *QuicknodeSubscribeResponse) {
	if response.Error != nil {
		qc.logger.WithFields(logrus.Fields{
			"id":    response.ID,
			"error": response.Error,
		}).Error("订阅失败")
		return
	}

	qc.logger.WithFields(logrus.Fields{
		"id":             response.ID,
		"subscriptionId": response.Result,
	}).Info("订阅成功")
}

// handleNotification 处理通知消息
func (qc *QuicknodeClient) handleNotification(notification *QuicknodeNotification) {
	switch notification.Method {
	case "eth_subscription":
		qc.handleEthSubscription(notification)
	default:
		qc.logger.WithField("method", notification.Method).Warn("未知的通知方法")
	}
}

// handleEthSubscription 处理以太坊订阅通知
func (qc *QuicknodeClient) handleEthSubscription(notification *QuicknodeNotification) {
	params, ok := notification.Params.(map[string]interface{})
	if !ok {
		qc.logger.Error("无效的订阅参数格式")
		return
	}

	result, ok := params["result"].(map[string]interface{})
	if !ok {
		qc.logger.Error("无效的订阅结果格式")
		return
	}

	// 构建区块数据
	blockData := producer.QuicknodeBlockData{
		Number:       util.GetString(result, "number"),
		Hash:         util.GetString(result, "hash"),
		ParentHash:   util.GetString(result, "parentHash"),
		Timestamp:    util.GetString(result, "timestamp"),
		Difficulty:   util.GetString(result, "difficulty"),
		TotalDiff:    util.GetString(result, "totalDifficulty"),
		Size:         util.GetString(result, "size"),
		GasLimit:     util.GetString(result, "gasLimit"),
		GasUsed:      util.GetString(result, "gasUsed"),
		Transactions: util.GetTransactionCount(result),
	}

	// 发送到Kafka
	ctx, cancel := context.WithTimeout(qc.ctx, 5*time.Second)
	defer cancel()

	if err := qc.producer.SendQuicknodeData(ctx, blockData); err != nil {
		qc.logger.WithError(err).Error("发送QuickNode区块数据失败")
	} else {
		qc.logger.WithFields(logrus.Fields{
			"number": blockData.Number,
			"hash":   blockData.Hash,
		}).Debug("QuickNode区块数据发送成功")
	}
}

// handleError 处理错误
func (qc *QuicknodeClient) handleError(err error) {
	qc.logger.WithError(err).Error("QuickNode WebSocket连接错误")
}

// QuickNode消息结构
type QuicknodeSubscribeMessage struct {
	ID     int           `json:"id"`
	Method string        `json:"method"`
	Params []interface{} `json:"params"`
}

type QuicknodeSubscribeResponse struct {
	ID     int                    `json:"id"`
	Result string                 `json:"result,omitempty"`
	Error  map[string]interface{} `json:"error,omitempty"`
}

type QuicknodeNotification struct {
	Method string      `json:"method"`
	Params interface{} `json:"params"`
}
