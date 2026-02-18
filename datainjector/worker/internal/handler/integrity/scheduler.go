package integrity

import (
	"errors"
	"sync"
	"time"

	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/observability/metrics"
	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/types"
)

// Scheduler 统一补数调度接口。
type Scheduler interface {
	Schedule(cmd types.BackfillCmd) error
	RegisterTarget(name string, target Target)
	OnResult(result types.BackfillResult)
}

// Target 表示补数结果注入主链路的方式。
type Target interface {
	Handle(cmd types.BackfillCmd) error
}

type simpleScheduler struct {
	mu       sync.RWMutex
	targets  map[string]Target // name → target，支持多通道兜底
	inflight map[string]types.BackfillCmd
}

func newScheduler() *simpleScheduler {
	return &simpleScheduler{
		targets:  map[string]Target{},
		inflight: map[string]types.BackfillCmd{},
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

var errTargetFiltered = errors.New("backfill target filtered")

func (s *simpleScheduler) Schedule(cmd types.BackfillCmd) error {
	cmd.EnsureDefaults(time.Now())
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.targets) == 0 {
		return types.ErrBackfillNoTarget
	}
	if _, ok := s.inflight[cmd.Key]; ok {
		metrics.RecordBackfillScheduleDedup(cmd.RoleID, cmd.StreamKey, cmd.Type)
		return nil
	}
	var lastErr error
	// 先按类型偏好选择目标，不命中再遍历全部。
	order := s.preferredOrder(cmd)
	for _, name := range order {
		if target, ok := s.targets[name]; ok {
			err := target.Handle(cmd)
			if err == nil {
				s.inflight[cmd.Key] = cmd
				return nil
			}
			if errors.Is(err, errTargetFiltered) {
				continue
			}
			lastErr = err
		}
	}
	for name, target := range s.targets {
		if contains(order, name) {
			continue
		}
		err := target.Handle(cmd)
		if err == nil {
			s.inflight[cmd.Key] = cmd
			return nil
		}
		if errors.Is(err, errTargetFiltered) {
			continue
		}
		lastErr = err
	}
	if lastErr != nil {
		return lastErr
	}
	return types.ErrBackfillNoTarget
}

func (s *simpleScheduler) OnResult(result types.BackfillResult) {
	key := result.Key
	if key == "" {
		key = types.BackfillSessionKey(result.RoleID, result.StreamKey, result.Type)
	}
	if key == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.inflight, key)
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

// ChannelTarget 将 BackfillCmd 写入 channel，支持阻塞超时与过滤。
type ChannelTarget struct {
	Ch     chan<- types.BackfillCmd     // 底层链路
	Filter func(types.BackfillCmd) bool // 可选过滤器，用于拆分 diff/snapshot
	// EnqueueTimeout 表示阻塞写超时；=0 使用默认 200ms，<0 表示仅尝试一次非阻塞写。
	EnqueueTimeout time.Duration
}

func (t *ChannelTarget) Handle(cmd types.BackfillCmd) error {
	start := time.Now()
	if t == nil || t.Ch == nil {
		metrics.RecordBackfillEnqueue(cmd.RoleID, cmd.Type, "no_target", time.Since(start))
		return types.ErrBackfillNoTarget
	}
	if t.Filter != nil && !t.Filter(cmd) {
		return errTargetFiltered
	}
	timeout := t.EnqueueTimeout
	if timeout == 0 {
		timeout = 200 * time.Millisecond
	}
	select {
	case t.Ch <- cmd:
		metrics.RecordBackfillEnqueue(cmd.RoleID, cmd.Type, "success", time.Since(start))
		return nil
	default:
	}
	if t.EnqueueTimeout < 0 {
		metrics.RecordBackfillEnqueue(cmd.RoleID, cmd.Type, "queue_full", time.Since(start))
		return types.ErrBackfillQueueFull
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case t.Ch <- cmd:
		metrics.RecordBackfillEnqueue(cmd.RoleID, cmd.Type, "success", time.Since(start))
		return nil
	case <-timer.C:
		metrics.RecordBackfillEnqueue(cmd.RoleID, cmd.Type, "enqueue_timeout", time.Since(start))
		return types.ErrBackfillEnqueueTimeout
	}
}
