package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"unified-worker/internal/kafka"
	"unified-worker/pkg/types"
)

// OutputStage 输出阶段
type OutputStage struct {
	BasePipeline
	producer     *kafka.Producer
	workerID     string
	taskID       string
	dataSourceID string
	outputTopic  string
}

// NewOutputStage 创建输出阶段
func NewOutputStage(producer *kafka.Producer, workerID, taskID, dataSourceID, outputTopic string) *OutputStage {
	return &OutputStage{
		BasePipeline: *NewBasePipeline("OutputStage"),
		producer:     producer,
		workerID:     workerID,
		taskID:       taskID,
		dataSourceID: dataSourceID,
		outputTopic:  outputTopic,
	}
}

// Process 处理数据
func (os *OutputStage) Process(ctx context.Context, data *PipelineData) error {
	// 检查是否应该输出
	if !data.ShouldOutput {
		log.Printf("[%s] 数据已缓冲，跳过输出", os.GetName())
		return nil
	}

	log.Printf("[%s] 输出数据到Kafka: topic=%s, seq=%v", os.GetName(), os.outputTopic, data.Sequence)

	// 构造DataMessage
	dataMessage := &types.DataMessage{
		WorkerID:     os.workerID,
		TaskID:       os.taskID,
		DataSourceID: os.dataSourceID,
		Timestamp:    time.Now().Unix(),
		Sequence:     data.Sequence,
		Data:         json.RawMessage(data.RawData),
		Metadata:     data.Metadata,
	}

	// 发送到Kafka
	key := fmt.Sprintf("%s-%s", os.dataSourceID, os.taskID)
	if err := os.producer.SendData(os.outputTopic, key, dataMessage); err != nil {
		return fmt.Errorf("[%s] 发送Kafka失败: %w", os.GetName(), err)
	}

	log.Printf("[%s] 输出成功", os.GetName())

	// 继续下一个处理器（如果有）
	return os.ProcessNext(ctx, data)
}
