package task

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"
)

// executeOneTime 执行命令式单次调用任务
func (te *TaskExecutor) executeOneTime(ctx context.Context) error {
	if te.config.TaskSpecificConfig.OneTime == nil {
		return fmt.Errorf("缺少命令式配置")
	}

	oneTimeConfig := te.config.TaskSpecificConfig.OneTime

	log.Printf("开始执行命令式任务: method=%s, params=%v",
		oneTimeConfig.Method, oneTimeConfig.Params)

	// 限流检查
	if te.rateLimiter != nil {
		if !te.rateLimiter.Allow(ctx) {
			return fmt.Errorf("任务被限流")
		}
	}

	// 构造JSON-RPC请求
	request := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  oneTimeConfig.Method,
		"params":  buildParamsArray(oneTimeConfig.Params),
	}

	reqData, err := json.Marshal(request)
	if err != nil {
		return fmt.Errorf("序列化请求失败: %w", err)
	}

	// 发送HTTP请求（带重试）
	var respData []byte
	for retryCount := 0; retryCount <= te.config.RetryConfig.MaxRetries; retryCount++ {
		respData, err = te.protocol.Send(ctx, reqData)
		if err == nil {
			break
		}

		log.Printf("命令式任务失败，重试 %d/%d: %v",
			retryCount+1, te.config.RetryConfig.MaxRetries, err)

		// 如果达到最大重试次数，上报失败
		if retryCount == te.config.RetryConfig.MaxRetries {
			if te.config.ReportToControlPlane {
				return te.handleError(ctx, err)
			}
			return err
		}

		// 等待重试
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(te.config.RetryConfig.BackoffBase * time.Duration(retryCount+1)):
			continue
		}
	}

	// 使用Pipeline处理响应数据
	if err := te.pipelineChain.Execute(ctx, respData); err != nil {
		return fmt.Errorf("Pipeline处理失败: %w", err)
	}

	// 尝试刷新缓冲区
	te.flushBuffer(ctx)

	return nil
}
