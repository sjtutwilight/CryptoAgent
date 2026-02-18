package integrity

import (
	"fmt"
	"strings"
	"time"

	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/types"
)

// IntegrityHandler 将策略、序列控制、幂等、阀门串联起来。
type IntegrityHandler struct {
	cfg       Config
	engine    *SequenceEngine
	scheduler Scheduler
}

func NewIntegrityHandler(cfg Config) (*IntegrityHandler, error) {
	cfg.Normalise()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	scheduler := newScheduler()
	rangeEval := buildRangeEvaluator(cfg.Profile, cfg.Keys.RangeStartField)
	deduper := newDeduper(cfg.Dedupe.TTL)
	if !cfg.Dedupe.Enabled {
		deduper = nil
	}
	var gate Gate
	switch strings.ToLower(cfg.Gate.Mode) {
	case "snapshot_hold":
		gate = &snapshotHoldGate{}
	case "finality":
		gate = newFinalityGate(cfg.Gate.FinalityBlocks)
	default:
		gate = &noopGate{}
	}
	engine := NewSequenceEngine(cfg, rangeEval, scheduler, gate, deduper, cfg.Keys.StreamKeyField)
	return &IntegrityHandler{
		cfg:       cfg,
		engine:    engine,
		scheduler: scheduler,
	}, nil
}

func (h *IntegrityHandler) SetBackfillTarget(name string, ch chan<- types.BackfillCmd) {
	timeout := h.cfg.Backfill.EnqueueTimeout
	if timeout <= 0 {
		timeout = 200 * time.Millisecond
	}
	h.scheduler.RegisterTarget(name, &ChannelTarget{Ch: ch, EnqueueTimeout: timeout})
}

func (h *IntegrityHandler) Handle(msg *types.Message) ([]*types.Message, error) {
	if msg == nil {
		return nil, nil
	}
	evt, err := h.buildEvent(msg)
	if err != nil {
		return nil, err
	}

	decision := h.engine.Handle(evt)
	return decision.Deliver, nil
}

func (h *IntegrityHandler) OnSnapshotApplied(lastSeq uint64) []*types.Message {
	// Gate 接口已改为控制面，不再返回消息
	// 所有消息释放由 engine.OnSnapshotApplied 处理
	decision := h.engine.OnSnapshotApplied(lastSeq)
	return decision.Deliver
}

func (h *IntegrityHandler) OnBackfillResult(result types.BackfillResult) {
	if h == nil || h.engine == nil {
		return
	}
	h.engine.OnBackfillResult(result)
}

func (h *IntegrityHandler) buildEvent(msg *types.Message) (*Event, error) {
	if msg.Metadata == nil {
		return nil, fmt.Errorf("integrity: metadata 缺失")
	}
	seqVal, ok := msg.Metadata[h.cfg.Keys.SeqField]
	if !ok {
		return nil, fmt.Errorf("integrity: metadata.%s 缺失", h.cfg.Keys.SeqField)
	}
	seq, err := toUint64(seqVal)
	if err != nil {
		return nil, fmt.Errorf("integrity: metadata.%s 非整数: %v", h.cfg.Keys.SeqField, err)
	}
	evt := &Event{
		Seq:     seq,
		Message: msg,
		Arrival: time.Now(),
	}

	if field := h.cfg.Keys.RangeStartField; field != "" {
		if v, ok := msg.Metadata[field]; ok {
			if rv, err := toUint64(v); err == nil {
				evt.RangeStart = rv
				evt.HasRange = true
			}
		}
	}
	if field := h.cfg.Keys.StreamKeyField; field != "" {
		if v, ok := msg.Metadata[field]; ok {
			evt.StreamKey = fmt.Sprintf("%v", v)
		}
	}
	if len(h.cfg.Keys.MessageIDFields) > 0 {
		var parts []string
		for _, f := range h.cfg.Keys.MessageIDFields {
			if v, ok := msg.Metadata[f]; ok {
				parts = append(parts, fmt.Sprintf("%v", v))
			}
		}
		if len(parts) > 0 {
			evt.MessageID = strings.Join(parts, "|")
		}
	}
	return evt, nil
}

func buildRangeEvaluator(profile, rangeField string) RangeEvaluator {
	switch strings.ToLower(profile) {
	case "binance_depth":
		return rangeEvalFunc(func(expected uint64, evt *Event) bool {
			if evt == nil {
				return false
			}
			// Primary path: standard U/u range covers expected sequence.
			if evt.HasRange && evt.RangeStart <= expected && evt.Seq >= expected {
				return true
			}
			// Fallback path for streams where U may not be contiguous:
			// trust pu continuity when it matches previous expected boundary.
			if evt.Message == nil || evt.Message.Metadata == nil {
				return false
			}
			prevRaw, ok := evt.Message.Metadata["prev_final_update_id"]
			if !ok {
				return false
			}
			prev, err := toUint64(prevRaw)
			if err != nil || prev == ^uint64(0) {
				return false
			}
			return prev+1 == expected
		})
	default:
		return rangeEvalFunc(func(expected uint64, evt *Event) bool {
			if evt == nil {
				return false
			}
			return evt.Seq == expected
		})
	}
}
