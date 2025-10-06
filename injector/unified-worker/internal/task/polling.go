package task

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"unified-worker/pkg/types"
)

// PollingConfig是types.PollingConfig的别名
type PollingConfig = types.PollingConfig

// executePolling 执行轮询任务
func (te *TaskExecutor) executePolling(ctx context.Context) error {
	if te.config.TaskSpecificConfig.Polling == nil {
		return fmt.Errorf("缺少轮询配置")
	}

	pollingConfig := te.config.TaskSpecificConfig.Polling
	ticker := time.NewTicker(pollingConfig.Interval)
	defer ticker.Stop()

	log.Printf("开始轮询任务: interval=%v, method=%s",
		pollingConfig.Interval, pollingConfig.Method)

	// 立即执行一次
	if err := te.executeSinglePoll(ctx, pollingConfig); err != nil {
		log.Printf("轮询失败: %v", err)
	}

	for {
		select {
		case <-ctx.Done():
			log.Printf("轮询任务被取消: task_id=%s", te.config.TaskID)
			return ctx.Err()

		case <-ticker.C:
			if err := te.executeSinglePoll(ctx, pollingConfig); err != nil {
				log.Printf("轮询失败: %v", err)
				if err := te.handleError(ctx, err); err != nil {
					return err
				}
			}
		}
	}
}

// executeSinglePoll 执行单次轮询
func (te *TaskExecutor) executeSinglePoll(ctx context.Context, pollingConfig *PollingConfig) error {
	// 限流检查
	if te.rateLimiter != nil {
		if !te.rateLimiter.Allow(ctx) {
			log.Printf("轮询被限流")
			return nil // 限流不算错误
		}
	}

	// 构造JSON-RPC请求
	request := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  pollingConfig.Method,
		"params":  buildParamsArray(pollingConfig.Params),
	}

	reqData, err := json.Marshal(request)
	if err != nil {
		return fmt.Errorf("序列化请求失败: %w", err)
	}

	// 发送HTTP请求
	respData, err := te.protocol.Send(ctx, reqData)
	if err != nil {
		return fmt.Errorf("发送请求失败: %w", err)
	}

	// 使用Pipeline处理响应数据
	if err := te.pipelineChain.Execute(ctx, respData); err != nil {
		return fmt.Errorf("Pipeline处理失败: %w", err)
	}

	// 尝试刷新缓冲区
	te.flushBuffer(ctx)

	return nil
}

// buildParamsArray 构建参数数组
func buildParamsArray(params map[string]interface{}) []interface{} {
	// 从map构建有序参数数组
	// 这里简化处理，实际可能需要根据method定义顺序
	result := make([]interface{}, 0)

	// 常见的参数顺序
	if block, ok := params["block"]; ok {
		result = append(result, block)
	}
	if fullTx, ok := params["full_tx"]; ok {
		result = append(result, fullTx)
	}

	return result
}
