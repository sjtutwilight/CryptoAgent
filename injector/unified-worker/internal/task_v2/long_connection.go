package task_v2

import (
	"context"
	"log"
)

// LongConnectionTask 长连接任务
type LongConnectionTask struct {
	ctx      *TaskContext
	config   map[string]interface{}
	stopChan chan struct{}
}

// NewLongConnectionTask 创建长连接任务
func NewLongConnectionTask(ctx *TaskContext, config map[string]interface{}) *LongConnectionTask {
	return &LongConnectionTask{
		ctx:      ctx,
		config:   config,
		stopChan: make(chan struct{}),
	}
}

// Type 返回任务类型
func (lct *LongConnectionTask) Type() string {
	return "long_connection"
}

// Start 启动长连接
func (lct *LongConnectionTask) Start(ctx context.Context) error {
	log.Printf("[LongConnectionTask] 启动长连接任务")
	
	// 订阅数据流
	dataChan, errChan := lct.ctx.Protocol.Receive(ctx)
	
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-lct.stopChan:
			return nil
		case data := <-dataChan:
			// 处理数据
			if lct.ctx.Handler != nil {
				if _, err := lct.ctx.Handler.Handle(ctx, data); err != nil {
					log.Printf("[LongConnectionTask] 处理数据失败: %v", err)
				}
			}
		case err := <-errChan:
			log.Printf("[LongConnectionTask] 接收错误: %v", err)
			// 不返回错误，让Protocol内部处理重连
		}
	}
}

// Stop 停止长连接
func (lct *LongConnectionTask) Stop() error {
	close(lct.stopChan)
	return nil
}
