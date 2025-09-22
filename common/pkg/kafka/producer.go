package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/IBM/sarama"
	"github.com/sirupsen/logrus"
)

// ProducerConfig Kafka生产者配置
type ProducerConfig struct {
	Brokers       []string
	Retries       int
	Timeout       int // milliseconds
	BatchSize     int
	FlushMessages int
	FlushBytes    int
	FlushInterval int // milliseconds
}

// DefaultProducerConfig 默认生产者配置
func DefaultProducerConfig() *ProducerConfig {
	return &ProducerConfig{
		Retries:       3,
		Timeout:       5000,
		BatchSize:     100,
		FlushMessages: 100,
		FlushBytes:    1024 * 1024, // 1MB
		FlushInterval: 100,         // 100ms
	}
}

// KafkaProducer Kafka生产者接口
type KafkaProducer interface {
	// SendMessage 发送消息到指定topic
	SendMessage(ctx context.Context, topic string, key string, value string) error
	// SendTaskStatus 发送任务状态消息
	SendTaskStatus(ctx context.Context, taskID string, status string) error
	// SendData 发送数据消息
	SendData(ctx context.Context, dataSourceID string, data string) error
	// SetTopicMapping 设置数据源到topic的映射
	SetTopicMapping(mapping map[string]string)
	// Close 关闭生产者
	Close() error
}

// kafkaProducer Kafka生产者实现
type kafkaProducer struct {
	producer     sarama.SyncProducer
	config       *ProducerConfig
	logger       *logrus.Logger
	statusTopic  string
	defaultTopic string
	topicMapping map[string]string
}

// NewKafkaProducer 创建新的Kafka生产者
func NewKafkaProducer(cfg *ProducerConfig, logger *logrus.Logger, statusTopic, defaultTopic string) (KafkaProducer, error) {
	// 配置Sarama
	saramaConfig := sarama.NewConfig()
	saramaConfig.Producer.Return.Successes = true
	saramaConfig.Producer.Return.Errors = true
	saramaConfig.Producer.RequiredAcks = sarama.WaitForAll // 等待所有副本确认
	saramaConfig.Producer.Retry.Max = cfg.Retries
	saramaConfig.Producer.Timeout = time.Duration(cfg.Timeout) * time.Millisecond

	// 设置分区器
	saramaConfig.Producer.Partitioner = sarama.NewRandomPartitioner

	// 启用幂等性
	saramaConfig.Producer.Idempotent = true
	saramaConfig.Net.MaxOpenRequests = 1

	// 创建生产者
	producer, err := sarama.NewSyncProducer(cfg.Brokers, saramaConfig)
	if err != nil {
		return nil, fmt.Errorf("创建Kafka生产者失败: %w", err)
	}

	logger.WithFields(logrus.Fields{
		"brokers": cfg.Brokers,
	}).Info("Kafka生产者创建成功")

	return &kafkaProducer{
		producer:     producer,
		config:       cfg,
		logger:       logger,
		statusTopic:  statusTopic,
		defaultTopic: defaultTopic,
		topicMapping: make(map[string]string),
	}, nil
}

// SetTopicMapping 设置数据源到topic的映射
func (kp *kafkaProducer) SetTopicMapping(mapping map[string]string) {
	kp.topicMapping = mapping
}

// SendMessage 发送消息到指定topic
func (kp *kafkaProducer) SendMessage(ctx context.Context, topic string, key string, value string) error {
	// 检查上下文是否已取消
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// 构建消息
	msg := &sarama.ProducerMessage{
		Topic:     topic,
		Key:       sarama.StringEncoder(key),
		Value:     sarama.StringEncoder(value),
		Timestamp: time.Now(),
	}

	// 添加消息头
	msg.Headers = []sarama.RecordHeader{
		{
			Key:   []byte("producer"),
			Value: []byte("common-kafka"),
		},
		{
			Key:   []byte("timestamp"),
			Value: []byte(time.Now().Format(time.RFC3339)),
		},
	}

	// 发送消息
	partition, offset, err := kp.producer.SendMessage(msg)
	if err != nil {
		kp.logger.WithFields(logrus.Fields{
			"topic": topic,
			"key":   key,
			"error": err,
		}).Error("发送Kafka消息失败")
		return fmt.Errorf("发送Kafka消息失败: %w", err)
	}

	kp.logger.WithFields(logrus.Fields{
		"topic":     topic,
		"key":       key,
		"partition": partition,
		"offset":    offset,
		"size":      len(value),
	}).Debug("Kafka消息发送成功")

	return nil
}

// SendTaskStatus 发送任务状态消息
func (kp *kafkaProducer) SendTaskStatus(ctx context.Context, taskID string, status string) error {
	return kp.SendMessage(ctx, kp.statusTopic, taskID, status)
}

// SendData 发送数据消息
func (kp *kafkaProducer) SendData(ctx context.Context, dataSourceID string, data string) error {
	topic := kp.getDataSourceTopic(dataSourceID)

	// 使用时间戳作为key以确保顺序
	key := fmt.Sprintf("%s_%d", dataSourceID, time.Now().UnixNano())

	return kp.SendMessage(ctx, topic, key, data)
}

// getDataSourceTopic 获取数据源对应的topic
func (kp *kafkaProducer) getDataSourceTopic(dataSourceID string) string {
	if topic, exists := kp.topicMapping[dataSourceID]; exists {
		return topic
	}
	return kp.defaultTopic
}

// Close 关闭生产者
func (kp *kafkaProducer) Close() error {
	if kp.producer != nil {
		err := kp.producer.Close()
		if err != nil {
			kp.logger.WithError(err).Error("关闭Kafka生产者失败")
			return fmt.Errorf("关闭Kafka生产者失败: %w", err)
		}
		kp.logger.Info("Kafka生产者已关闭")
	}
	return nil
}

// AsyncKafkaProducer 异步Kafka生产者接口
type AsyncKafkaProducer interface {
	// SendMessageAsync 异步发送消息
	SendMessageAsync(topic string, key string, value string) error
	// StartErrorHandler 启动错误处理器
	StartErrorHandler()
	// Close 关闭生产者
	Close() error
}

// asyncKafkaProducer 异步Kafka生产者实现
type asyncKafkaProducer struct {
	producer sarama.AsyncProducer
	config   *ProducerConfig
	logger   *logrus.Logger
	stopChan chan struct{}
}

// NewAsyncKafkaProducer 创建新的异步Kafka生产者
func NewAsyncKafkaProducer(cfg *ProducerConfig, logger *logrus.Logger) (AsyncKafkaProducer, error) {
	// 配置Sarama
	saramaConfig := sarama.NewConfig()
	saramaConfig.Producer.Return.Successes = true
	saramaConfig.Producer.Return.Errors = true
	saramaConfig.Producer.RequiredAcks = sarama.WaitForAll
	saramaConfig.Producer.Retry.Max = cfg.Retries
	saramaConfig.Producer.Timeout = time.Duration(cfg.Timeout) * time.Millisecond

	// 异步生产者配置
	saramaConfig.Producer.Flush.Frequency = time.Duration(cfg.FlushInterval) * time.Millisecond
	saramaConfig.Producer.Flush.Messages = cfg.FlushMessages
	saramaConfig.Producer.Flush.Bytes = cfg.FlushBytes

	// 创建异步生产者
	producer, err := sarama.NewAsyncProducer(cfg.Brokers, saramaConfig)
	if err != nil {
		return nil, fmt.Errorf("创建异步Kafka生产者失败: %w", err)
	}

	asyncProducer := &asyncKafkaProducer{
		producer: producer,
		config:   cfg,
		logger:   logger,
		stopChan: make(chan struct{}),
	}

	// 启动错误处理器
	asyncProducer.StartErrorHandler()

	logger.WithFields(logrus.Fields{
		"brokers": cfg.Brokers,
	}).Info("异步Kafka生产者创建成功")

	return asyncProducer, nil
}

// SendMessageAsync 异步发送消息
func (akp *asyncKafkaProducer) SendMessageAsync(topic string, key string, value string) error {
	select {
	case <-akp.stopChan:
		return fmt.Errorf("生产者已关闭")
	default:
	}

	// 构建消息
	msg := &sarama.ProducerMessage{
		Topic:     topic,
		Key:       sarama.StringEncoder(key),
		Value:     sarama.StringEncoder(value),
		Timestamp: time.Now(),
	}

	// 添加消息头
	msg.Headers = []sarama.RecordHeader{
		{
			Key:   []byte("producer"),
			Value: []byte("common-kafka-async"),
		},
		{
			Key:   []byte("timestamp"),
			Value: []byte(time.Now().Format(time.RFC3339)),
		},
	}

	// 异步发送消息
	select {
	case akp.producer.Input() <- msg:
		akp.logger.WithFields(logrus.Fields{
			"topic": topic,
			"key":   key,
			"size":  len(value),
		}).Debug("异步Kafka消息已提交")
		return nil
	case <-akp.stopChan:
		return fmt.Errorf("生产者已关闭")
	}
}

// StartErrorHandler 启动错误处理器
func (akp *asyncKafkaProducer) StartErrorHandler() {
	go func() {
		for {
			select {
			case success := <-akp.producer.Successes():
				akp.logger.WithFields(logrus.Fields{
					"topic":     success.Topic,
					"partition": success.Partition,
					"offset":    success.Offset,
				}).Debug("异步Kafka消息发送成功")

			case err := <-akp.producer.Errors():
				akp.logger.WithFields(logrus.Fields{
					"topic": err.Msg.Topic,
					"error": err.Err,
				}).Error("异步Kafka消息发送失败")

			case <-akp.stopChan:
				return
			}
		}
	}()
}

// Close 关闭异步生产者
func (akp *asyncKafkaProducer) Close() error {
	close(akp.stopChan)

	if akp.producer != nil {
		err := akp.producer.Close()
		if err != nil {
			akp.logger.WithError(err).Error("关闭异步Kafka生产者失败")
			return fmt.Errorf("关闭异步Kafka生产者失败: %w", err)
		}
		akp.logger.Info("异步Kafka生产者已关闭")
	}

	return nil
}
