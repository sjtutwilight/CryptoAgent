package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/sirupsen/logrus"
)

// ConsumerConfig Kafka消费者配置
type ConsumerConfig struct {
	Brokers           []string
	GroupID           string
	Topics            []string
	SessionTimeout    int    // milliseconds
	HeartbeatInterval int    // milliseconds
	AutoOffsetReset   string // earliest, latest
}

// DefaultConsumerConfig 默认消费者配置
func DefaultConsumerConfig() *ConsumerConfig {
	return &ConsumerConfig{
		SessionTimeout:    10000, // 10s
		HeartbeatInterval: 3000,  // 3s
		AutoOffsetReset:   "earliest",
	}
}

// MessageHandler 消息处理函数类型
type MessageHandler func(ctx context.Context, message *sarama.ConsumerMessage) error

// KafkaConsumer Kafka消费者接口
type KafkaConsumer interface {
	// Start 启动消费者
	Start(ctx context.Context) error
	// Stop 停止消费者
	Stop() error
	// SetMessageHandler 设置消息处理器
	SetMessageHandler(handler MessageHandler)
}

// kafkaConsumer Kafka消费者实现
type kafkaConsumer struct {
	consumerGroup  sarama.ConsumerGroup
	config         *ConsumerConfig
	logger         *logrus.Logger
	messageHandler MessageHandler
	stopChan       chan struct{}
	wg             sync.WaitGroup
	running        bool
	mu             sync.RWMutex
}

// NewKafkaConsumer 创建新的Kafka消费者
func NewKafkaConsumer(cfg *ConsumerConfig, logger *logrus.Logger) (KafkaConsumer, error) {
	// 配置Sarama消费者
	saramaConfig := sarama.NewConfig()
	saramaConfig.Consumer.Return.Errors = true
	saramaConfig.Consumer.Group.Session.Timeout = time.Duration(cfg.SessionTimeout) * time.Millisecond
	saramaConfig.Consumer.Group.Heartbeat.Interval = time.Duration(cfg.HeartbeatInterval) * time.Millisecond

	// 设置offset策略
	switch cfg.AutoOffsetReset {
	case "earliest":
		saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	case "latest":
		saramaConfig.Consumer.Offsets.Initial = sarama.OffsetNewest
	default:
		saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	}

	// 启用自动提交
	saramaConfig.Consumer.Offsets.AutoCommit.Enable = true
	saramaConfig.Consumer.Offsets.AutoCommit.Interval = 1 * time.Second

	// 消费者组配置
	saramaConfig.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin

	// 创建消费者组
	consumerGroup, err := sarama.NewConsumerGroup(cfg.Brokers, cfg.GroupID, saramaConfig)
	if err != nil {
		return nil, fmt.Errorf("创建Kafka消费者组失败: %w", err)
	}

	logger.WithFields(logrus.Fields{
		"brokers": cfg.Brokers,
		"groupId": cfg.GroupID,
		"topics":  cfg.Topics,
	}).Info("Kafka消费者创建成功")

	return &kafkaConsumer{
		consumerGroup: consumerGroup,
		config:        cfg,
		logger:        logger,
		stopChan:      make(chan struct{}),
	}, nil
}

// SetMessageHandler 设置消息处理器
func (kc *kafkaConsumer) SetMessageHandler(handler MessageHandler) {
	kc.messageHandler = handler
}

// Start 启动消费者
func (kc *kafkaConsumer) Start(ctx context.Context) error {
	kc.mu.Lock()
	if kc.running {
		kc.mu.Unlock()
		return fmt.Errorf("消费者已在运行")
	}
	kc.running = true
	kc.mu.Unlock()

	if kc.messageHandler == nil {
		return fmt.Errorf("消息处理器未设置")
	}

	kc.logger.WithField("topics", kc.config.Topics).Info("启动Kafka消费者")

	// 启动错误处理协程
	kc.wg.Add(1)
	go kc.handleErrors()

	// 启动消费协程
	kc.wg.Add(1)
	go func() {
		defer kc.wg.Done()

		for {
			select {
			case <-ctx.Done():
				kc.logger.Info("收到上下文取消信号，停止消费")
				return
			case <-kc.stopChan:
				kc.logger.Info("收到停止信号，停止消费")
				return
			default:
				// 消费消息
				err := kc.consumerGroup.Consume(ctx, kc.config.Topics, kc)
				if err != nil {
					kc.logger.WithError(err).Error("消费消息时出错")

					// 如果是上下文取消，直接返回
					if ctx.Err() != nil {
						return
					}

					// 其他错误，等待后重试
					time.Sleep(5 * time.Second)
				}
			}
		}
	}()

	return nil
}

// Stop 停止消费者
func (kc *kafkaConsumer) Stop() error {
	kc.mu.Lock()
	if !kc.running {
		kc.mu.Unlock()
		return nil
	}
	kc.running = false
	kc.mu.Unlock()

	kc.logger.Info("停止Kafka消费者")

	// 发送停止信号
	close(kc.stopChan)

	// 关闭消费者组
	if err := kc.consumerGroup.Close(); err != nil {
		kc.logger.WithError(err).Error("关闭消费者组失败")
	}

	// 等待所有协程结束
	kc.wg.Wait()

	kc.logger.Info("Kafka消费者已停止")
	return nil
}

// handleErrors 处理错误
func (kc *kafkaConsumer) handleErrors() {
	defer kc.wg.Done()

	for {
		select {
		case err, ok := <-kc.consumerGroup.Errors():
			if !ok {
				return
			}
			kc.logger.WithError(err).Error("Kafka消费者错误")
		case <-kc.stopChan:
			return
		}
	}
}

// Setup 实现ConsumerGroupHandler接口 - 消费者组启动时调用
func (kc *kafkaConsumer) Setup(sarama.ConsumerGroupSession) error {
	kc.logger.Info("Kafka消费者组会话开始")
	return nil
}

// Cleanup 实现ConsumerGroupHandler接口 - 消费者组停止时调用
func (kc *kafkaConsumer) Cleanup(sarama.ConsumerGroupSession) error {
	kc.logger.Info("Kafka消费者组会话结束")
	return nil
}

// ConsumeClaim 实现ConsumerGroupHandler接口 - 消费消息
func (kc *kafkaConsumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				kc.logger.Info("消息通道已关闭")
				return nil
			}

			// 处理消息
			if err := kc.processMessage(session.Context(), message); err != nil {
				kc.logger.WithFields(logrus.Fields{
					"topic":     message.Topic,
					"partition": message.Partition,
					"offset":    message.Offset,
					"error":     err,
				}).Error("处理消息失败")

				// 继续处理下一条消息，不中断消费
				continue
			}

			// 标记消息已处理
			session.MarkMessage(message, "")

		case <-session.Context().Done():
			kc.logger.Info("消费会话上下文已取消")
			return nil
		}
	}
}

// processMessage 处理单条消息
func (kc *kafkaConsumer) processMessage(ctx context.Context, message *sarama.ConsumerMessage) error {
	startTime := time.Now()

	kc.logger.WithFields(logrus.Fields{
		"topic":     message.Topic,
		"partition": message.Partition,
		"offset":    message.Offset,
		"key":       string(message.Key),
		"size":      len(message.Value),
	}).Debug("收到Kafka消息")

	// 调用消息处理器
	if err := kc.messageHandler(ctx, message); err != nil {
		return err
	}

	duration := time.Since(startTime)
	kc.logger.WithFields(logrus.Fields{
		"topic":    message.Topic,
		"key":      string(message.Key),
		"duration": duration,
	}).Debug("消息处理完成")

	return nil
}

// TaskMessage 任务消息结构
type TaskMessage struct {
	TaskID    string      `json:"taskId"`
	Payload   interface{} `json:"payload"`
	Timestamp time.Time   `json:"timestamp"`
}

// ParseTaskMessage 解析任务消息
func ParseTaskMessage(data []byte) (*TaskMessage, error) {
	var task TaskMessage
	if err := json.Unmarshal(data, &task); err != nil {
		return nil, fmt.Errorf("解析任务消息失败: %w", err)
	}
	return &task, nil
}

// StatusMessage 状态消息结构
type StatusMessage struct {
	TaskID     string    `json:"taskId"`
	Status     string    `json:"status"`
	Message    string    `json:"message"`
	Timestamp  time.Time `json:"timestamp"`
	Duration   int64     `json:"duration"`
	StatusCode int       `json:"statusCode"`
	DataSize   int       `json:"dataSize"`
	RetryCount int       `json:"retryCount"`
}

// ParseStatusMessage 解析状态消息
func ParseStatusMessage(data []byte) (*StatusMessage, error) {
	var status StatusMessage
	if err := json.Unmarshal(data, &status); err != nil {
		return nil, fmt.Errorf("解析状态消息失败: %w", err)
	}
	return &status, nil
}

