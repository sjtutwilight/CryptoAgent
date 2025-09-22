package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"http-worker/internal/config"
	"http-worker/internal/handler"
	"http-worker/internal/model"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/sirupsen/logrus"
)

// KafkaConsumer Kafka消费者接口
type KafkaConsumer interface {
	// Start 启动消费者
	Start(ctx context.Context) error
	// Stop 停止消费者
	Stop() error
}

// kafkaConsumer Kafka消费者实现
type kafkaConsumer struct {
	consumerGroup   sarama.ConsumerGroup
	taskHandler     handler.TaskHandler
	config          *config.Config
	logger          *logrus.Logger
	stopChan        chan struct{}
	wg              sync.WaitGroup
	running         bool
	mu              sync.RWMutex
}

// NewKafkaConsumer 创建新的Kafka消费者
func NewKafkaConsumer(
	cfg *config.Config,
	taskHandler handler.TaskHandler,
	logger *logrus.Logger,
) (KafkaConsumer, error) {
	// 配置Sarama消费者
	saramaConfig := sarama.NewConfig()
	saramaConfig.Consumer.Return.Errors = true
	saramaConfig.Consumer.Group.Session.Timeout = time.Duration(cfg.Kafka.Consumer.SessionTimeout) * time.Millisecond
	saramaConfig.Consumer.Group.Heartbeat.Interval = time.Duration(cfg.Kafka.Consumer.HeartbeatInterval) * time.Millisecond
	
	// 设置offset策略
	switch cfg.Kafka.Consumer.AutoOffsetReset {
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
	consumerGroup, err := sarama.NewConsumerGroup(cfg.Kafka.Brokers, cfg.Kafka.Consumer.GroupID, saramaConfig)
	if err != nil {
		return nil, fmt.Errorf("创建Kafka消费者组失败: %w", err)
	}
	
	logger.WithFields(logrus.Fields{
		"brokers": cfg.Kafka.Brokers,
		"groupId": cfg.Kafka.Consumer.GroupID,
		"topics":  cfg.Kafka.Consumer.Topics,
	}).Info("Kafka消费者创建成功")
	
	return &kafkaConsumer{
		consumerGroup: consumerGroup,
		taskHandler:   taskHandler,
		config:        cfg,
		logger:        logger,
		stopChan:      make(chan struct{}),
	}, nil
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
	
	// 获取要消费的topics
	topics := []string{kc.config.Kafka.Consumer.Topics["tasks"]}
	
	kc.logger.WithField("topics", topics).Info("启动Kafka消费者")
	
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
				err := kc.consumerGroup.Consume(ctx, topics, kc)
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
	
	// 解析任务
	var task model.HttpTask
	if err := json.Unmarshal(message.Value, &task); err != nil {
		return fmt.Errorf("解析任务JSON失败: %w", err)
	}
	
	// 验证任务
	if err := kc.validateTask(&task); err != nil {
		kc.logger.WithFields(logrus.Fields{
			"taskId": task.TaskID,
			"error":  err,
		}).Warn("任务验证失败")
		
		// 发送失败状态
		return kc.sendTaskFailedStatus(ctx, &task, fmt.Sprintf("Task validation failed: %v", err))
	}
	
	// 处理任务
	if err := kc.taskHandler.HandleTask(ctx, &task); err != nil {
		kc.logger.WithFields(logrus.Fields{
			"taskId": task.TaskID,
			"error":  err,
		}).Error("任务处理失败")
		return err
	}
	
	duration := time.Since(startTime)
	kc.logger.WithFields(logrus.Fields{
		"taskId":   task.TaskID,
		"duration": duration,
	}).Info("任务处理完成")
	
	return nil
}

// validateTask 验证任务
func (kc *kafkaConsumer) validateTask(task *model.HttpTask) error {
	if task.TaskID == "" {
		return fmt.Errorf("任务ID不能为空")
	}
	
	if task.Payload.DataSourceURL == "" {
		return fmt.Errorf("数据源URL不能为空")
	}
	
	if task.Payload.Method == "" {
		task.Payload.Method = "GET" // 设置默认���法
	}
	
	return nil
}

// sendTaskFailedStatus 发送任务失败状态
func (kc *kafkaConsumer) sendTaskFailedStatus(ctx context.Context, task *model.HttpTask, message string) error {
	taskStatus := &model.TaskStatus{
		TaskID:     task.TaskID,
		Status:     "FAILED",
		Message:    message,
		Timestamp:  time.Now(),
		Duration:   0,
		StatusCode: 0,
		DataSize:   0,
		RetryCount: 0,
	}
	
	// 这里应该发送到状态topic，但为了简化，先记录日志
	kc.logger.WithFields(logrus.Fields{
		"taskId":  task.TaskID,
		"status":  taskStatus.Status,
		"message": taskStatus.Message,
	}).Warn("任务验证失败，应发送失败状态")
	
	return nil
}