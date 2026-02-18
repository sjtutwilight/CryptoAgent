package integrity

import (
	"testing"
	"time"

	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/types"
)

type captureScheduler struct {
	calls   []types.BackfillCmd
	results []types.BackfillResult
}

func (s *captureScheduler) Schedule(cmd types.BackfillCmd) error {
	s.calls = append(s.calls, cmd)
	return nil
}

func (s *captureScheduler) RegisterTarget(name string, target Target) {}

func (s *captureScheduler) OnResult(result types.BackfillResult) {
	s.results = append(s.results, result)
}

func newResultDrivenEngineForTest(s Scheduler) *SequenceEngine {
	cfg := Config{}
	cfg.Keys.SeqField = "seq"
	cfg.Backfill.ResultDrivenEnabled = true
	cfg.Backfill.MaxFailures = 1
	cfg.Backfill.ExhaustCooldown = 50 * time.Millisecond
	cfg.Backfill.Options = []types.BackfillOption{{
		Transport: types.BackfillTransportHTTP,
		RPCMethod: "eth_getBlockByNumber",
	}}
	cfg.Normalise()
	engine := NewSequenceEngine(cfg, nil, s, nil, nil, "stream-a")
	engine.roleID = "role-a"
	engine.streamName = "stream-a"
	return engine
}

func TestSequenceEnginePendingDedupAndMergedIntent(t *testing.T) {
	sched := &captureScheduler{}
	engine := newResultDrivenEngineForTest(sched)
	now := time.Now()

	if ok := engine.triggerBackfill(10, 12, now); !ok {
		t.Fatalf("expected first trigger success")
	}
	if ok := engine.triggerBackfill(11, 15, now.Add(10*time.Millisecond)); ok {
		t.Fatalf("expected second trigger merged into pending")
	}
	if got := len(sched.calls); got != 1 {
		t.Fatalf("expected one scheduled cmd before result, got %d", got)
	}
	first := sched.calls[0]

	engine.OnBackfillResult(types.BackfillResult{
		CmdID:      first.CmdID,
		SessionID:  first.SessionID,
		Key:        first.Key,
		RoleID:     first.RoleID,
		StreamKey:  first.StreamKey,
		Type:       first.Type,
		Status:     types.BackfillResultSuccess,
		FinishedAt: now.Add(30 * time.Millisecond),
	})

	if got := len(sched.calls); got != 2 {
		t.Fatalf("expected merged intent scheduled after success, got %d calls", got)
	}
	second := sched.calls[1]
	if second.Start != 11 || second.End != 15 {
		t.Fatalf("unexpected merged range: start=%d end=%d", second.Start, second.End)
	}
}

func TestSequenceEngineIgnoresOutOfOrderResult(t *testing.T) {
	sched := &captureScheduler{}
	engine := newResultDrivenEngineForTest(sched)
	now := time.Now()

	if ok := engine.triggerBackfill(1, 2, now); !ok {
		t.Fatalf("expected first trigger success")
	}
	first := sched.calls[0]

	engine.OnBackfillResult(types.BackfillResult{
		Key:        first.Key,
		SessionID:  "stale-session",
		Status:     types.BackfillResultSuccess,
		FinishedAt: now.Add(20 * time.Millisecond),
	})
	if ok := engine.triggerBackfill(3, 4, now.Add(25*time.Millisecond)); ok {
		t.Fatalf("expected pending session to reject new trigger before valid result")
	}
	if got := len(sched.calls); got != 1 {
		t.Fatalf("unexpected new schedule before valid result: %d", got)
	}

	engine.OnBackfillResult(types.BackfillResult{
		Key:        first.Key,
		SessionID:  first.SessionID,
		CmdID:      first.CmdID,
		Status:     types.BackfillResultSuccess,
		FinishedAt: now.Add(40 * time.Millisecond),
	})
	if got := len(sched.calls); got != 2 {
		t.Fatalf("expected new schedule after valid result, got %d", got)
	}
	if ok := engine.triggerBackfill(5, 6, now.Add(50*time.Millisecond)); ok {
		t.Fatalf("expected trigger merged into pending session")
	}
	if got := len(sched.calls); got != 2 {
		t.Fatalf("expected no extra schedule while pending, got %d", got)
	}
}

func TestSequenceEngineCooldownRecovery(t *testing.T) {
	sched := &captureScheduler{}
	engine := newResultDrivenEngineForTest(sched)
	now := time.Now()

	if ok := engine.triggerBackfill(20, 22, now); !ok {
		t.Fatalf("expected first trigger success")
	}
	first := sched.calls[0]
	engine.OnBackfillResult(types.BackfillResult{
		Key:        first.Key,
		CmdID:      first.CmdID,
		SessionID:  first.SessionID,
		Status:     types.BackfillResultFail,
		ErrorClass: "unknown",
		FinishedAt: now.Add(10 * time.Millisecond),
	})

	if ok := engine.triggerBackfill(23, 24, now.Add(20*time.Millisecond)); ok {
		t.Fatalf("expected trigger blocked by cooldown")
	}
	if got := len(sched.calls); got != 1 {
		t.Fatalf("unexpected schedule during cooldown, got %d", got)
	}

	if ok := engine.triggerBackfill(23, 24, now.Add(80*time.Millisecond)); !ok {
		t.Fatalf("expected trigger success after cooldown")
	}
	if got := len(sched.calls); got != 2 {
		t.Fatalf("expected second schedule after cooldown, got %d", got)
	}
}

func TestSequenceEngineOrderbookSideChannelDoesNotBlockDiff(t *testing.T) {
	sched := &captureScheduler{}
	cfg := Config{}
	cfg.Profile = "binance_depth"
	cfg.Keys.SeqField = "seq"
	cfg.Backfill.OrderbookMode = "snapshot_sidechannel"
	cfg.Backfill.Options = []types.BackfillOption{{
		Transport: types.BackfillTransportHTTP,
		Params: map[string]any{
			"method": "GET",
		},
	}}
	cfg.Sequence.EagerGap = 1
	cfg.Normalise()

	engine := NewSequenceEngine(cfg, nil, sched, nil, nil, "stream-ob")
	engine.roleID = "role-ob"

	// 首条 diff 不应进入 snapshot gate 阻塞。
	first := engine.Handle(&Event{
		Seq:     100,
		Arrival: time.Now(),
		Message: &types.Message{Metadata: map[string]any{"seq": uint64(100)}, Payload: []byte(`{}`)},
	})
	if len(first.Deliver) != 1 {
		t.Fatalf("expected first diff delivered, got %d", len(first.Deliver))
	}
	if engine.state.AwaitingSnapshot {
		t.Fatalf("sidechannel mode should not await snapshot on first diff")
	}

	// gap 触发 snapshot backfill，但 diff 主流仍不进入 snapshot gate。
	_ = engine.Handle(&Event{
		Seq:     103,
		Arrival: time.Now().Add(10 * time.Millisecond),
		Message: &types.Message{Metadata: map[string]any{"seq": uint64(103)}, Payload: []byte(`{}`)},
	})
	if len(sched.calls) != 1 {
		t.Fatalf("expected one backfill cmd, got %d", len(sched.calls))
	}
	cmd := sched.calls[0]
	if cmd.Type != types.BackfillTypeSnapshot {
		t.Fatalf("expected snapshot backfill, got %s", cmd.Type)
	}
	if cmd.SnapshotSource != "backfill" {
		t.Fatalf("unexpected snapshot source: %s", cmd.SnapshotSource)
	}
	if cmd.SnapshotReason != "gap" {
		t.Fatalf("unexpected snapshot reason: %s", cmd.SnapshotReason)
	}
	if engine.state.AwaitingSnapshot {
		t.Fatalf("sidechannel mode should not await snapshot after scheduling backfill")
	}

	// sidechannel snapshot 只旁路透传，不参与 diff 放行状态机。
	snapshot := engine.Handle(&Event{
		Seq:     999,
		Arrival: time.Now().Add(20 * time.Millisecond),
		Message: &types.Message{Metadata: map[string]any{"seq": uint64(999), "snapshot": true}, Payload: []byte(`{}`)},
	})
	if len(snapshot.Deliver) != 1 {
		t.Fatalf("expected snapshot pass-through, got %d", len(snapshot.Deliver))
	}
	if engine.state.AwaitingSnapshot {
		t.Fatalf("sidechannel snapshot should not switch engine to awaiting state")
	}
}
