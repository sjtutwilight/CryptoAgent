package integrity

import (
	"errors"
	"testing"
	"time"

	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/types"
)

type mockScheduler struct {
	err error
}

func (m *mockScheduler) Schedule(cmd types.BackfillCmd) error      { return m.err }
func (m *mockScheduler) RegisterTarget(name string, target Target) {}
func (m *mockScheduler) OnResult(result types.BackfillResult)      {}

func TestCompensationQueuePersistAndReplay(t *testing.T) {
	tmp := t.TempDir() + "/comp.json"
	s := &mockScheduler{err: errors.New("full")}
	cfg := Config{}
	cfg.Keys.SeqField = "seq"
	cfg.Backfill.PersistentCompensation = true
	cfg.Backfill.CompensationFile = tmp
	cfg.Backfill.ReplayInterval = time.Millisecond
	cfg.Backfill.CompensationMaxPending = 16
	cfg.Normalise()

	engine := NewSequenceEngine(cfg, nil, s, nil, nil, "stream-a")
	engine.roleID = "role-a"
	cmd := types.BackfillCmd{
		Type:      types.BackfillTypeRange,
		RoleID:    "role-a",
		StreamKey: "stream-a",
		Start:     1,
		End:       2,
	}

	engine.enqueueCompensation(cmd, types.ErrBackfillEnqueueTimeout, time.Now())
	if got := len(engine.compQueue); got != 1 {
		t.Fatalf("expected 1 compensation item, got %d", got)
	}

	s.err = nil
	engine.replayCompensations(time.Now().Add(2 * time.Millisecond))
	if got := len(engine.compQueue); got != 0 {
		t.Fatalf("expected compensation queue drained, got %d", got)
	}
}

func TestCompensationReplaySkippedWhenSessionPending(t *testing.T) {
	sched := &captureScheduler{}
	cfg := Config{}
	cfg.Keys.SeqField = "seq"
	cfg.Backfill.ResultDrivenEnabled = true
	cfg.Backfill.PersistentCompensation = true
	cfg.Backfill.CompensationFile = t.TempDir() + "/comp.json"
	cfg.Backfill.ReplayInterval = time.Millisecond
	cfg.Backfill.MaxFailures = 1
	cfg.Backfill.ExhaustCooldown = 50 * time.Millisecond
	cfg.Backfill.Options = []types.BackfillOption{{
		Transport: types.BackfillTransportHTTP,
		RPCMethod: "eth_getBlockByNumber",
	}}
	cfg.Normalise()

	engine := NewSequenceEngine(cfg, nil, sched, nil, nil, "stream-a")
	engine.roleID = "role-a"
	engine.streamName = "stream-a"

	now := time.Now()
	if ok := engine.triggerBackfill(1, 2, now); !ok {
		t.Fatalf("expected triggerBackfill success")
	}
	if got := len(sched.calls); got != 1 {
		t.Fatalf("expected one scheduled command, got %d", got)
	}

	engine.enqueueCompensation(sched.calls[0], types.ErrBackfillEnqueueTimeout, now)
	if got := len(engine.compQueue); got != 1 {
		t.Fatalf("expected one compensation item, got %d", got)
	}

	engine.replayCompensations(now.Add(2 * time.Millisecond))
	if got := len(engine.compQueue); got != 1 {
		t.Fatalf("expected compensation item retained while pending, got %d", got)
	}
	if got := len(sched.calls); got != 1 {
		t.Fatalf("expected no extra schedule while pending, got %d", got)
	}
}
