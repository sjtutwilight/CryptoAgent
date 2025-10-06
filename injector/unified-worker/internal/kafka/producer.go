package kafka

import (
	"encoding/json"
	"fmt"

	"unified-worker/internal/config"

	"github.com/IBM/sarama"
)

// Producer Kafka生产者
type Producer struct {
	producer sarama.SyncProducer
	config   config.KafkaConfig
}

// NewProducer 创建Kafka生产者
func NewProducer(cfg config.KafkaConfig) (*Producer, error) {
	saramaConfig := sarama.NewConfig()
	saramaConfig.Producer.Return.Successes = true
	saramaConfig.Producer.RequiredAcks = sarama.WaitForAll

	// 设置压缩
	switch cfg.Producer.Compression {
	case "snappy":
		saramaConfig.Producer.Compression = sarama.CompressionSnappy
	case "gzip":
		saramaConfig.Producer.Compression = sarama.CompressionGZIP
	case "lz4":
		saramaConfig.Producer.Compression = sarama.CompressionLZ4
	default:
		saramaConfig.Producer.Compression = sarama.CompressionNone
	}

	producer, err := sarama.NewSyncProducer(cfg.Brokers, saramaConfig)
	if err != nil {
		return nil, fmt.Errorf("创建Kafka生产者失败: %w", err)
	}

	return &Producer{
		producer: producer,
		config:   cfg,
	}, nil
}

// SendMessage 发送消息
func (p *Producer) SendMessage(topic string, key string, value interface{}) error {
	// 序列化消息
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("序列化消息失败: %w", err)
	}

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(data),
	}

	_, _, err = p.producer.SendMessage(msg)
	if err != nil {
		return fmt.Errorf("发送Kafka消息失败: %w", err)
	}

	return nil
}

// SendData 发送数据消息
func (p *Producer) SendData(topic string, key string, message interface{}) error {
	return p.SendMessage(topic, key, message)
}

// SendFailure 发送失败报告
func (p *Producer) SendFailure(key string, report interface{}) error {
	return p.SendMessage("worker.failures", key, report)
}

// Close 关闭生产者
func (p *Producer) Close() error {
	if p.producer != nil {
		return p.producer.Close()
	}
	return nil
}
