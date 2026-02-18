package role

import (
	"context"
	"testing"
	"time"

	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/handler"
	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/types"
)

type captureBackfillResultAware struct {
	results []types.BackfillResult
}

func (c *captureBackfillResultAware) OnBackfillResult(result types.BackfillResult) {
	c.results = append(c.results, result)
}

func TestHandleBackfillCmdEmitFailWhenNoAttempts(t *testing.T) {
	sink := &captureBackfillResultAware{}
	r := &Role{
		ID:             "role-test",
		backfillStates: map[string]backfillState{},
		backfillResult: []handler.BackfillResultAware{sink},
	}
	cmd := types.BackfillCmd{
		Type:      types.BackfillTypeRange,
		RoleID:    "role-test",
		StreamKey: "stream-a",
		Start:     1,
		End:       2,
	}

	r.handleBackfillCmd(context.Background(), 0, cmd)
	if len(sink.results) != 1 {
		t.Fatalf("expected one result callback, got %d", len(sink.results))
	}
	if sink.results[0].Status != types.BackfillResultFail {
		t.Fatalf("expected fail result, got %+v", sink.results[0])
	}
	if sink.results[0].Key == "" || sink.results[0].CmdID == "" || sink.results[0].SessionID == "" {
		t.Fatalf("expected compatibility defaults filled, got %+v", sink.results[0])
	}
}

func TestHandleBackfillCmdEmitTimeoutWhenCooldown(t *testing.T) {
	sink := &captureBackfillResultAware{}
	r := &Role{
		ID:             "role-test",
		backfillStates: map[string]backfillState{},
		backfillResult: []handler.BackfillResultAware{sink},
	}
	cmd := types.BackfillCmd{
		Type:      types.BackfillTypeRange,
		RoleID:    "role-test",
		StreamKey: "stream-a",
		Start:     1,
		End:       2,
	}
	stateKey := r.backfillStateKey(cmd)
	r.backfillStates[stateKey] = backfillState{
		Status:        "cooldown",
		CooldownUntil: time.Now().Add(3 * time.Second),
	}

	r.handleBackfillCmd(context.Background(), 0, cmd)
	if len(sink.results) != 1 {
		t.Fatalf("expected one result callback, got %d", len(sink.results))
	}
	if sink.results[0].Status != types.BackfillResultTimeout {
		t.Fatalf("expected timeout result, got %+v", sink.results[0])
	}
	if sink.results[0].ErrorClass != "timeout" {
		t.Fatalf("expected timeout error class, got %+v", sink.results[0])
	}
}
