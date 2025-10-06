package task

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
)

// executeLongConnection 执行长连接订阅任务
func (te *TaskExecutor) executeLongConnection(ctx context.Context) error {
	// 发送订阅请求
	if err := te.subscribe(ctx); err != nil {
		return fmt.Errorf("订阅失败: %w", err)
	}

	// 接收数据
	dataChan, errChan := te.protocol.Receive(ctx)

	for {
		select {
		case <-ctx.Done():
			log.Printf("任务被取消: task_id=%s", te.config.TaskID)
			return ctx.Err()

		case err := <-errChan:
			log.Printf("接收错误: %v", err)
			if err := te.handleError(ctx, err); err != nil {
				return err
			}

		case data := <-dataChan:
			// 使用Pipeline处理数据
			if err := te.pipelineChain.Execute(ctx, data); err != nil {
				log.Printf("Pipeline处理失败: %v", err)
			}

			// 尝试刷新缓冲区
			te.flushBuffer(ctx)
		}
	}
}

// subscribe 发送订阅请求（通用化）
func (te *TaskExecutor) subscribe(ctx context.Context) error {
	if te.config.TaskSpecificConfig.Subscription == nil {
		return fmt.Errorf("缺少订阅配置")
	}

	// 从数据源元数据获取订阅方法和参数（通用化）
	subscribeMethod := "subscribe" // 默认方法
	var params []interface{}

	if te.config.DataSource != nil && te.config.DataSource.Subscription.Supported {
		// 使用数据源配置的订阅方法
		subscribeMethod = te.config.DataSource.Subscription.SubscribeMethod
		params = te.config.DataSource.Subscription.Params
	} else {
		// 兜底：使用任务配置中的topics
		params = make([]interface{}, len(te.config.TaskSpecificConfig.Subscription.Topics))
		for i, topic := range te.config.TaskSpecificConfig.Subscription.Topics {
			params[i] = topic
		}
	}

	// 构造订阅请求（通用JSON-RPC格式）
	subscribeReq := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  subscribeMethod,
		"params":  params,
	}

	reqData, err := json.Marshal(subscribeReq)
	if err != nil {
		return fmt.Errorf("序列化订阅请求失败: %w", err)
	}

	log.Printf("发送订阅请求: method=%s, params=%v", subscribeMethod, params)

	_, err = te.protocol.Send(ctx, reqData)
	if err != nil {
		return fmt.Errorf("发送订阅请求失败: %w", err)
	}

	log.Printf("订阅成功: method=%s", subscribeMethod)
	return nil
}
