package task_v2

import (
	"context"
	"log"
	"time"
)

// PollingTask 轮询任务
type PollingTask struct {
	ctx      *TaskContext
	config   map[string]interface{}
	interval time.Duration
	stopChan chan struct{}
}

// NewPollingTask 创建轮询任务
func NewPollingTask(ctx *TaskContext, config map[string]interface{}) *PollingTask {
	interval := 60 * time.Second
	if iv, ok := config["interval_seconds"].(int); ok {
		interval = time.Duration(iv) * time.Second
	}
	if iv, ok := config["interval_seconds"].(float64); ok {
		interval = time.Duration(iv) * time.Second
	}

	return &PollingTask{
		ctx:      ctx,
		config:   config,
		interval: interval,
		stopChan: make(chan struct{}),
	}
}

// Type 返回任务类型
func (pt *PollingTask) Type() string {
	return "polling"
}

// Start 启动轮询
func (pt *PollingTask) Start(ctx context.Context) error {
	log.Printf("[PollingTask] 启动轮询任务, 间隔: %v", pt.interval)

	ticker := time.NewTicker(pt.interval)
	defer ticker.Stop()

	// 立即执行一次
	if err := pt.poll(ctx); err != nil {
		log.Printf("[PollingTask] 轮询失败: %v", err)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-pt.stopChan:
			return nil
		case <-ticker.C:
			if err := pt.poll(ctx); err != nil {
				log.Printf("[PollingTask] 轮询失败: %v", err)
			}
		}
	}
}

// poll 执行一次轮询
func (pt *PollingTask) poll(ctx context.Context) error {
	log.Printf("[PollingTask] 开始轮询")
	// 1. 获取限流token
	if pt.ctx.Resource != nil {
		if err := pt.ctx.Resource.AcquireRateLimit(ctx); err != nil {
			return err
		}
	}
	// 2. 使用Fetcher获取数据
	var data []byte
	var err error

	if pt.ctx.Fetcher != nil {
		data, err = pt.ctx.Fetcher.Fetch(ctx, pt.config)
		if err != nil {
			return err
		}
	}
	log.Printf("[PollingTask] 获取数据: %s", string(data))
	// 3. 通过Handler链处理
	if pt.ctx.Handler != nil {
		_, err = pt.ctx.Handler.Handle(ctx, data)
		if err != nil {
			return err
		}
	}

	return nil
}

// Stop 停止轮询
func (pt *PollingTask) Stop() error {
	close(pt.stopChan)
	return nil
}
