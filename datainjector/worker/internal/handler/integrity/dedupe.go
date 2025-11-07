package integrity

import (
	"fmt"
	"sync"
	"time"
)

type dedupeEntry struct {
	expire time.Time // 过期时间
}

type deduper struct {
	ttl time.Duration // 去重窗口
	mu  sync.Mutex
	m   map[string]dedupeEntry // 简单内存索引
}

func newDeduper(ttl time.Duration) *deduper {
	if ttl <= 0 {
		return nil
	}
	return &deduper{
		ttl: ttl,
		m:   make(map[string]dedupeEntry),
	}
}

func (d *deduper) ShouldDrop(evt *Event) bool {
	if d == nil || evt == nil {
		return false
	}
	if evt.MessageID == "" && evt.StreamKey == "" {
		return false
	}
	// 优先使用业务自带幂等键；否则退化为 stream+seq。
	key := evt.MessageID
	if key == "" {
		key = fmt.Sprintf("%s:%d", evt.StreamKey, evt.Seq)
	}
	now := time.Now()
	d.mu.Lock()
	defer d.mu.Unlock()
	if entry, ok := d.m[key]; ok && entry.expire.After(now) {
		return true
	}
	d.m[key] = dedupeEntry{expire: now.Add(d.ttl)}
	// 顺带清理过期键，避免 map 无界增长。
	for k, v := range d.m {
		if v.expire.Before(now) {
			delete(d.m, k)
		}
	}
	return false
}
