package integrity

import (
	"sync"

	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/types"
)

// Scheduler 统一补数调度接口。
type Scheduler interface {
	Schedule(cmd types.BackfillCmd) bool
	RegisterTarget(name string, target Target)
}

// Target 表示补数结果注入主链路的方式。
type Target interface {
	Handle(cmd types.BackfillCmd) bool
}

type simpleScheduler struct {
	mu      sync.RWMutex
	targets map[string]Target // name → target，支持多通道兜底
}

func newScheduler() *simpleScheduler {
	return &simpleScheduler{
		targets: map[string]Target{},
	}
}

func (s *simpleScheduler) RegisterTarget(name string, target Target) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if target == nil {
		delete(s.targets, name)
		return
	}
	s.targets[name] = target
}

func (s *simpleScheduler) Schedule(cmd types.BackfillCmd) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.targets) == 0 {
		return false
	}
	// 先按类型偏好选择目标，不命中再遍历全部。
	order := s.preferredOrder(cmd)
	for _, name := range order {
		if target, ok := s.targets[name]; ok {
			if target.Handle(cmd) {
				return true
			}
		}
	}
	for name, target := range s.targets {
		if contains(order, name) {
			continue
		}
		if target.Handle(cmd) {
			return true
		}
	}
	return false
}

func (s *simpleScheduler) preferredOrder(cmd types.BackfillCmd) []string {
	switch cmd.Type {
	case types.BackfillTypeSnapshot:
		return []string{"snapshot"}
	case types.BackfillTypeRange:
		return []string{"diff", "queue"}
	// BackfillTypeReplay 暂未定义，预留
	// case types.BackfillTypeReplay:
	// 	return []string{"replay"}
	default:
		return nil
	}
}

func contains(order []string, name string) bool {
	for _, v := range order {
		if v == name {
			return true
		}
	}
	return false
}

// ChannelTarget 简单地将 BackfillCmd 写入 channel。
type ChannelTarget struct {
	Ch     chan<- types.BackfillCmd     // 底层链路
	Filter func(types.BackfillCmd) bool // 可选过滤器，用于拆分 diff/snapshot
}

func (t *ChannelTarget) Handle(cmd types.BackfillCmd) bool {
	if t == nil || t.Ch == nil {
		return false
	}
	if t.Filter != nil && !t.Filter(cmd) {
		return false
	}
	select {
	case t.Ch <- cmd:
		return true
	default:
	}
	return false
}
