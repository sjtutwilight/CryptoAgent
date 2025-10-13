package emitter

import (
    "context"
    "time"
)

// Polling 触发器：固定间隔触发一次 fire(args)
type Polling struct {
    Interval time.Duration
}

// Start 会阻塞直到 ctx 结束
func (p *Polling) Start(ctx context.Context, fire func(args map[string]any)) error {
    if p.Interval <= 0 {
        p.Interval = time.Second
    }
    ticker := time.NewTicker(p.Interval)
    defer ticker.Stop()
    // 立即触发一次
    fire(nil)
    for {
        select {
        case <-ctx.Done():
            return nil
        case <-ticker.C:
            fire(nil)
        }
    }
}

