package kafka

import (
	"context"
	"fmt"
	"log"

	"unified-worker/internal/config"
	"unified-worker/pkg/types"

	"github.com/IBM/sarama"
)

// Consumer Kafka消费者
type Consumer struct {
	consumer sarama.ConsumerGroup
	config   config.KafkaConfig
	handler  TaskHandler
}

// TaskHandler 任务处理器接口
type TaskHandler interface {
	HandleTask(ctx context.Context, task *types.TaskConfig) error
}

// NewConsumer 创建Kafka消费者
func NewConsumer(cfg config.KafkaConfig, handler TaskHandler) (*Consumer, error) {
	saramaConfig := sarama.NewConfig()
	saramaConfig.Consumer.Return.Errors = true
	saramaConfig.Consumer.Offsets.Initial = sarama.OffsetNewest

	consumer, err := sarama.NewConsumerGroup(cfg.Brokers, cfg.ConsumerGroup, saramaConfig)
	if err != nil {
		return nil, fmt.Errorf("创建Kafka消费者失败: %w", err)
	}

	return &Consumer{
		consumer: consumer,
		config:   cfg,
		handler:  handler,
	}, nil
}

// Start 启动消费者
func (c *Consumer) Start(ctx context.Context) error {
	handler := &consumerGroupHandler{
		taskHandler: c.handler,
	}

	topics := []string{c.config.TaskTopic}

	go func() {
		for {
			if err := c.consumer.Consume(ctx, topics, handler); err != nil {
				log.Printf("Kafka消费错误: %v", err)
			}

			// 检查context是否取消
			if ctx.Err() != nil {
				return
			}
		}
	}()

	return nil
}
