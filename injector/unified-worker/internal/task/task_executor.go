package task

import (
	"context"
	"fmt"
	"log"
	"time"

	"unified-worker/internal/kafka"
	"unified-worker/internal/parser"
	"unified-worker/internal/pipeline"
	"unified-worker/internal/runtime"
	"unified-worker/pkg/types"
)

// TaskExecutor 任务执行器
type TaskExecutor struct {
	config      *types.TaskConfig
	protocol    types.ProtocolHandler
	producer    *kafka.Producer
	rateLimiter runtime.RateLimiter
	workerID    string

	// Pipeline责任链
	pipelineChain *pipeline.PipelineChain
	bufferStage   *pipeline.BufferStage // 保留引用以便刷新缓冲

	// 状态
	lastSequence interface{} // 最后处理的序列号
	retryCount   int
}

// NewTaskExecutor 创建任务执行器
func NewTaskExecutor(
	workerID string,
	config *types.TaskConfig,
	protocol types.ProtocolHandler,
	producer *kafka.Producer,
	rateLimiter runtime.RateLimiter,
) *TaskExecutor {
	// 构建Pipeline责任链
	parserConfig := &parser.ParserConfig{
		DataSourceID:  config.DataSourceID,
		TaskType:      string(config.TaskType),
		SequenceField: config.SequenceField,
	}

	parseStage := pipeline.NewParseStage(parserConfig)
	seqStage := pipeline.NewSequenceStage(config.SequenceField)
	bufferStage := pipeline.NewBufferStage()
	outputStage := pipeline.NewOutputStage(producer, workerID, config.TaskID, config.DataSourceID, config.OutputTopic)

	// 链接Pipeline
	parseStage.SetNext(seqStage)
	seqStage.SetNext(bufferStage)
	bufferStage.SetNext(outputStage)

	pipelineChain := pipeline.NewPipelineChain(parseStage)

	return &TaskExecutor{
		config:        config,
		protocol:      protocol,
		producer:      producer,
		rateLimiter:   rateLimiter,
		workerID:      workerID,
		pipelineChain: pipelineChain,
		bufferStage:   bufferStage,
		retryCount:    0,
	}
}

// Execute 执行任务
func (te *TaskExecutor) Execute(ctx context.Context) error {
	log.Printf("开始执行任务: task_id=%s, type=%s, protocol=%s",
		te.config.TaskID, te.config.TaskType, te.config.Protocol)

	switch te.config.TaskType {
	case types.TaskTypeLongConnection:
		return te.executeLongConnection(ctx)
	case types.TaskTypePolling:
		return te.executePolling(ctx)
	case types.TaskTypeOneTime:
		return te.executeOneTime(ctx)
	default:
		return fmt.Errorf("不支持的任务类型: %s", te.config.TaskType)
	}
}

// handleError 处理错误（本地重试失败后上报）
func (te *TaskExecutor) handleError(ctx context.Context, err error) error {
	te.retryCount++

	if te.retryCount >= te.config.RetryConfig.MaxRetries {
		log.Printf("达到最大重试次数，上报失败: task_id=%s, error=%v",
			te.config.TaskID, err)

		// 如果需要上报控制面
		if te.config.ReportToControlPlane {
			failureReport := &types.FailureReport{
				WorkerID:     te.workerID,
				TaskID:       te.config.TaskID,
				DataSourceID: te.config.DataSourceID,
				Timestamp:    te.getCurrentTimestamp(),
				ErrorType:    "retry_exhausted",
				ErrorMessage: err.Error(),
				RetryCount:   te.retryCount,
				LastSequence: te.lastSequence,
			}

			key := fmt.Sprintf("%s-%s", te.config.DataSourceID, te.config.TaskID)
			if sendErr := te.producer.SendFailure(key, failureReport); sendErr != nil {
				log.Printf("发送失败报告失败: %v", sendErr)
			}
		}

		return fmt.Errorf("任务失败: %w", err)
	}

	// 继续重试
	return nil
}

// getCurrentTimestamp 获取当前时间戳
func (te *TaskExecutor) getCurrentTimestamp() int64 {
	return time.Now().Unix()
}

// flushBuffer 刷新缓冲区中的连续数据
func (te *TaskExecutor) flushBuffer(ctx context.Context) {
	items := te.bufferStage.FlushBuffer()
	for _, item := range items {
		// 重新执行Pipeline处理缓冲数据
		if err := te.pipelineChain.Execute(ctx, item.Data); err != nil {
			log.Printf("处理缓冲数据失败: seq=%v, err=%v", item.Sequence, err)
		}
	}
}
