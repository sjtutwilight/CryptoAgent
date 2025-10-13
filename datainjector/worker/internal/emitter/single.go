package emitter

import (
	"context"
	"time"
)

// Single 一次性订阅触发器：从配置读取订阅信息，第一次触发订阅，后续周期性触发以拉取消息
// 用于websocket订阅等场景，订阅信息直接来自配置
type Single struct {
	Params       map[string]any // 订阅参数
	PollInterval time.Duration  // 轮询间隔，用于拉取缓存的消息
}

// Start 第一次触发订阅，然后周期性触发以拉取消息
func (s *Single) Start(ctx context.Context, fire func(args map[string]any)) error {
	// 设置默认轮询间隔
	if s.PollInterval <= 0 {
		s.PollInterval = 1 * time.Second
	}

	// 第一次触发订阅，将配置参数传递给caller
	fire(s.Params)

	// 周期性触发以拉取缓存的消息
	ticker := time.NewTicker(s.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			// 周期性触发，从caller拉取缓存的消息
			fire(nil)
		}
	}
}
