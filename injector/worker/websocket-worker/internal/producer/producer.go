package producer

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"websocket-worker/internal/config"

	"github.com/sirupsen/logrus"
	"github.com/sjtutwilight/Twilight/common/pkg/kafka"
)

// DataProducer 数据生产者接口
type DataProducer interface {
	// SendBinanceData 发送Binance数据
	SendBinanceData(ctx context.Context, data interface{}) error
	// SendQuicknodeData 发送Quicknode数据
	SendQuicknodeData(ctx context.Context, data interface{}) error
	// Close 关闭生产者
	Close() error
}

// dataProducer 数据生产者实现
type dataProducer struct {
	kafkaProducer kafka.KafkaProducer
	config        *config.Config
	logger        *logrus.Logger
}

// NewDataProducer 创建新的数据生产者
func NewDataProducer(cfg *config.Config, logger *logrus.Logger) (DataProducer, error) {
	// 创建Kafka生产者配置
	producerConfig := &kafka.ProducerConfig{
		Brokers:       cfg.Kafka.Brokers,
		Retries:       3,
		Timeout:       5000,
		BatchSize:     100,
		FlushMessages: 100,
		FlushBytes:    1024 * 1024,
		FlushInterval: 100,
	}

	// 创建Kafka生产者
	kafkaProducer, err := kafka.NewKafkaProducer(
		producerConfig,
		logger,
		"websocket_status", // 状态topic
		"websocket_data",   // 默认数据topic
	)
	if err != nil {
		return nil, fmt.Errorf("创建Kafka生产者失败: %w", err)
	}

	// 设置topic映射
	topicMapping := map[string]string{
		"binance":   cfg.Kafka.Topics.Binance,
		"quicknode": cfg.Kafka.Topics.Quicknode,
	}

	// 设置topic映射
	kafkaProducer.SetTopicMapping(topicMapping)

	return &dataProducer{
		kafkaProducer: kafkaProducer,
		config:        cfg,
		logger:        logger,
	}, nil
}

// SendBinanceData 发送Binance数据
func (dp *dataProducer) SendBinanceData(ctx context.Context, data interface{}) error {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("序列化Binance数据失败: %w", err)
	}

	return dp.kafkaProducer.SendData(ctx, "binance", string(jsonData))
}

// SendQuicknodeData 发送Quicknode数据
func (dp *dataProducer) SendQuicknodeData(ctx context.Context, data interface{}) error {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("序列化Quicknode数据失败: %w", err)
	}

	return dp.kafkaProducer.SendData(ctx, "quicknode", string(jsonData))
}

// Close 关闭生产者
func (dp *dataProducer) Close() error {
	if dp.kafkaProducer != nil {
		return dp.kafkaProducer.Close()
	}
	return nil
}

// WebSocketMessage WebSocket消息结构
type WebSocketMessage struct {
	Source    string      `json:"source"`
	Type      string      `json:"type"`
	Data      interface{} `json:"data"`
	Timestamp time.Time   `json:"timestamp"`
}

// BinanceKlineData Binance K线数据
type BinanceKlineData struct {
	Symbol    string `json:"symbol"`
	Interval  string `json:"interval"`
	OpenTime  int64  `json:"openTime"`
	CloseTime int64  `json:"closeTime"`
	Open      string `json:"open"`
	High      string `json:"high"`
	Low       string `json:"low"`
	Close     string `json:"close"`
	Volume    string `json:"volume"`
	IsFinal   bool   `json:"isFinal"`
}

// QuicknodeBlockData Quicknode区块数据
type QuicknodeBlockData struct {
	Number       string `json:"number"`
	Hash         string `json:"hash"`
	ParentHash   string `json:"parentHash"`
	Timestamp    string `json:"timestamp"`
	Difficulty   string `json:"difficulty"`
	TotalDiff    string `json:"totalDifficulty"`
	Size         string `json:"size"`
	GasLimit     string `json:"gasLimit"`
	GasUsed      string `json:"gasUsed"`
	Transactions int    `json:"transactionCount"`
}
