package client

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"websocket-worker/internal/config"
	"websocket-worker/internal/connection"
	"websocket-worker/internal/producer"
	"websocket-worker/internal/util"

	"github.com/sirupsen/logrus"
)

// BinanceClient Binance WebSocket客户端
type BinanceClient struct {
	config   *config.Config
	logger   *logrus.Logger
	producer producer.DataProducer
	connMgr  *connection.ConnectionManager
	ctx      context.Context
	cancel   context.CancelFunc
}

// NewBinanceClient 创建新的Binance客户端
func NewBinanceClient(cfg *config.Config, logger *logrus.Logger, producer producer.DataProducer) *BinanceClient {
	ctx, cancel := context.WithCancel(context.Background())

	// 构建WebSocket URL
	url := buildBinanceStreamURL(cfg.Websocket.Binance.URL, cfg.Websocket.Binance.Symbols, cfg.Websocket.Binance.Interval)

	// 创建连接管理器
	headers := http.Header{}
	if cfg.Websocket.Binance.ApiKey != "" {
		headers.Set("X-MBX-APIKEY", cfg.Websocket.Binance.ApiKey)
	}

	connMgr := connection.NewConnectionManager(cfg, url, headers)

	client := &BinanceClient{
		config:   cfg,
		logger:   logger,
		producer: producer,
		connMgr:  connMgr,
		ctx:      ctx,
		cancel:   cancel,
	}

	// 设置消息和错误处理器
	connMgr.SetMessageHandler(client.handleMessage)
	connMgr.SetErrorHandler(client.handleError)

	return client
}

// Start 启动Binance客户端
func (bc *BinanceClient) Start() error {
	bc.logger.Info("启动Binance WebSocket客户端")

	// 连接到Binance WebSocket
	if err := bc.connMgr.Connect(); err != nil {
		return fmt.Errorf("连接Binance WebSocket失败: %w", err)
	}

	// 发送订阅消息
	if err := bc.subscribe(); err != nil {
		return fmt.Errorf("订阅Binance流失败: %w", err)
	}

	bc.logger.Info("Binance WebSocket客户端启动成功")
	return nil
}

// Stop 停止Binance客户端
func (bc *BinanceClient) Stop() {
	bc.logger.Info("停止Binance WebSocket客户端")
	bc.cancel()
	bc.connMgr.Disconnect()
}

// subscribe 订阅数据流
func (bc *BinanceClient) subscribe() error {
	// Binance使用预定义的stream URL，不需要发送订阅消息
	// 如果需要动态订阅，可以在这里实现
	return nil
}

// handleMessage 处理接收到的消息
func (bc *BinanceClient) handleMessage(data []byte) {
	bc.logger.WithField("size", len(data)).Debug("收到Binance消息")

	// 解析消息
	var message BinanceStreamMessage
	if err := json.Unmarshal(data, &message); err != nil {
		bc.logger.WithError(err).Error("解析Binance消息失败")
		return
	}

	// 根据消息类型处理
	switch {
	case strings.Contains(message.Stream, "@kline_"):
		bc.handleKlineMessage(&message)
	default:
		bc.logger.WithField("stream", message.Stream).Warn("未知的Binance消息类型")
	}
}

// handleKlineMessage 处理K线消息
func (bc *BinanceClient) handleKlineMessage(message *BinanceStreamMessage) {
	// 解析K线数据
	klineData, ok := message.Data.(map[string]interface{})
	if !ok {
		bc.logger.Error("K线数据格式错误")
		return
	}

	klineInfo, ok := klineData["k"].(map[string]interface{})
	if !ok {
		bc.logger.Error("K线信息格式错误")
		return
	}

	// 构建K线数据结构
	kline := producer.BinanceKlineData{
		Symbol:    util.GetString(klineInfo, "s"),
		Interval:  util.GetString(klineInfo, "i"),
		OpenTime:  util.GetInt64(klineInfo, "t"),
		CloseTime: util.GetInt64(klineInfo, "T"),
		Open:      util.GetString(klineInfo, "o"),
		High:      util.GetString(klineInfo, "h"),
		Low:       util.GetString(klineInfo, "l"),
		Close:     util.GetString(klineInfo, "c"),
		Volume:    util.GetString(klineInfo, "v"),
		IsFinal:   util.GetBool(klineInfo, "x"),
	}

	// 发送到Kafka
	ctx, cancel := context.WithTimeout(bc.ctx, 5*time.Second)
	defer cancel()

	if err := bc.producer.SendBinanceData(ctx, kline); err != nil {
		bc.logger.WithError(err).Error("发送Binance K线数据失败")
	} else {
		bc.logger.WithFields(logrus.Fields{
			"symbol":   kline.Symbol,
			"interval": kline.Interval,
			"isFinal":  kline.IsFinal,
		}).Debug("Binance K线数据发送成功")
	}
}

// handleError 处理错误
func (bc *BinanceClient) handleError(err error) {
	bc.logger.WithError(err).Error("Binance WebSocket连接错误")
}

// buildBinanceStreamURL 构建Binance流URL
func buildBinanceStreamURL(baseURL string, symbols []string, interval string) string {
	// 使用正确的 Binance WebSocket 格式
	// 对于多个流，应该使用 wss://stream.binance.com:9443/stream?streams=btcusdt@kline_1m/ethusdt@kline_1m
	if !strings.HasSuffix(baseURL, "/stream") {
		if strings.HasSuffix(baseURL, "/") {
			baseURL += "stream"
		} else {
			baseURL += "/stream"
		}
	}

	var streams []string
	for _, symbol := range symbols {
		stream := fmt.Sprintf("%s@kline_%s", strings.ToLower(symbol), interval)
		streams = append(streams, stream)
	}

	return fmt.Sprintf("%s?streams=%s", baseURL, strings.Join(streams, "/"))
}

// BinanceStreamMessage Binance流消息结构
type BinanceStreamMessage struct {
	Stream string      `json:"stream"`
	Data   interface{} `json:"data"`
}

// 辅助函数已移动到 util 包
