package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/IBM/sarama"
)

// KafkaSinkHandler Kafka输出处理器
// 将数据发送到Kafka topic
type KafkaSinkHandler struct {
	*BaseHandler
	producer sarama.SyncProducer
	topic    string
}

// KafkaSinkConfig Kafka输出配置
type KafkaSinkConfig struct {
	Topic   string   `json:"topic"`
	Brokers []string `json:"brokers"`
}

// NewKafkaSinkHandler 创建Kafka输出处理器
func NewKafkaSinkHandler(config KafkaSinkConfig) (*KafkaSinkHandler, error) {
	// 创建Kafka配置
	kafkaConfig := sarama.NewConfig()
	kafkaConfig.Producer.Return.Successes = true
	kafkaConfig.Producer.RequiredAcks = sarama.WaitForAll
	kafkaConfig.Producer.Retry.Max = 3

	// 创建同步生产者
	producer, err := sarama.NewSyncProducer(config.Brokers, kafkaConfig)
	if err != nil {
		return nil, fmt.Errorf("创建Kafka生产者失败: %w", err)
	}

	return &KafkaSinkHandler{
		BaseHandler: NewBaseHandler(fmt.Sprintf("KafkaSinkHandler[%s]", config.Topic)),
		producer:    producer,
		topic:       config.Topic,
	}, nil
}

// Handle 处理数据
func (h *KafkaSinkHandler) Handle(ctx context.Context, data []byte) ([]byte, error) {
	log.Printf("[%s] 开始发送数据到Kafka, 大小: %d bytes", h.Name(), len(data))

	// 解析数据，确定是单条还是批量
	var messages []*sarama.ProducerMessage

	var parsed interface{}
	if err := json.Unmarshal(data, &parsed); err != nil {
		return nil, fmt.Errorf("JSON解析失败: %w", err)
	}

	switch v := parsed.(type) {
	case map[string]interface{}:
		// 单条数据
		msg := &sarama.ProducerMessage{
			Topic: h.topic,
			Value: sarama.ByteEncoder(data),
		}
		messages = append(messages, msg)

	case []interface{}:
		// 批量数据，拆分为多条消息
		for _, item := range v {
			itemBytes, err := json.Marshal(item)
			if err != nil {
				log.Printf("[%s] 序列化单条数据失败: %v", h.Name(), err)
				continue
			}
			msg := &sarama.ProducerMessage{
				Topic: h.topic,
				Value: sarama.ByteEncoder(itemBytes),
			}
			messages = append(messages, msg)
		}

	default:
		return nil, fmt.Errorf("不支持的数据类型: %T", parsed)
	}

	// 批量发送
	successCount := 0
	for _, msg := range messages {
		partition, offset, err := h.producer.SendMessage(msg)
		if err != nil {
			log.Printf("[%s] 发送失败: %v", h.Name(), err)
			continue
		}
		successCount++
		log.Printf("[%s] 发送成功: partition=%d, offset=%d", h.Name(), partition, offset)
	}

	log.Printf("[%s] 批量发送完成: 成功=%d/%d", h.Name(), successCount, len(messages))

	// Kafka Sink是链尾，不传递数据
	return data, nil
}

// Close 关闭Kafka生产者
func (h *KafkaSinkHandler) Close() error {
	if h.producer != nil {
		return h.producer.Close()
	}
	return nil
}
