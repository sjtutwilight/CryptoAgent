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
