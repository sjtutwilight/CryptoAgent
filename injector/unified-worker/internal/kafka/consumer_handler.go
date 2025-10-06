package kafka

import (
	"encoding/json"
	"log"

	"unified-worker/pkg/types"

	"github.com/IBM/sarama"
)

// consumerGroupHandler 实现sarama.ConsumerGroupHandler接口
type consumerGroupHandler struct {
	taskHandler TaskHandler
}

// Setup 在开始消费前调用
func (h *consumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

// Cleanup 在停止消费后调用
func (h *consumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim 处理消息
func (h *consumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		// 解析任务
		var task types.TaskConfig
		if err := json.Unmarshal(message.Value, &task); err != nil {
			log.Printf("解析任务失败: %v", err)
			session.MarkMessage(message, "")
			continue
		}

		log.Printf("收到任务: task_id=%s, type=%s, protocol=%s",
			task.TaskID, task.TaskType, task.Protocol)

		// 处理任务
		ctx := session.Context()
		if err := h.taskHandler.HandleTask(ctx, &task); err != nil {
			log.Printf("处理任务失败: task_id=%s, error=%v", task.TaskID, err)
			// 这里可以根据业务需求决定是否标记消息
		}

		// 标记消息已处理
		session.MarkMessage(message, "")
	}

	return nil
}

// Close 关闭消费者
func (c *Consumer) Close() error {
	if c.consumer != nil {
		return c.consumer.Close()
	}
	return nil
}
