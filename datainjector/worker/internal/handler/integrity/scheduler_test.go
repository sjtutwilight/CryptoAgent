package integrity

import (
	"errors"
	"testing"
	"time"

	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/types"
)

func TestSchedulerNoTargetReturnsError(t *testing.T) {
	s := newScheduler()
	err := s.Schedule(types.BackfillCmd{Type: types.BackfillTypeRange})
	if !errors.Is(err, types.ErrBackfillNoTarget) {
		t.Fatalf("expected ErrBackfillNoTarget, got %v", err)
	}
}

func TestChannelTargetTimeout(t *testing.T) {
	ch := make(chan types.BackfillCmd, 1)
	ch <- types.BackfillCmd{Type: types.BackfillTypeRange}
	target := &ChannelTarget{
		Ch:             ch,
		EnqueueTimeout: 10 * time.Millisecond,
	}
	err := target.Handle(types.BackfillCmd{Type: types.BackfillTypeRange})
	if !errors.Is(err, types.ErrBackfillEnqueueTimeout) {
		t.Fatalf("expected ErrBackfillEnqueueTimeout, got %v", err)
	}
}

func TestChannelTargetQueueFull(t *testing.T) {
	ch := make(chan types.BackfillCmd, 1)
	ch <- types.BackfillCmd{Type: types.BackfillTypeRange}
	target := &ChannelTarget{
		Ch:             ch,
		EnqueueTimeout: -1,
	}
	err := target.Handle(types.BackfillCmd{Type: types.BackfillTypeRange})
	if !errors.Is(err, types.ErrBackfillQueueFull) {
		t.Fatalf("expected ErrBackfillQueueFull, got %v", err)
	}
}

func TestSchedulerDedupByKeyUntilResult(t *testing.T) {
	s := newScheduler()
	ch := make(chan types.BackfillCmd, 4)
	s.RegisterTarget("diff", &ChannelTarget{Ch: ch, EnqueueTimeout: 50 * time.Millisecond})

	cmd1 := types.BackfillCmd{
		Type:      types.BackfillTypeRange,
		RoleID:    "role-a",
		StreamKey: "stream-a",
		Start:     1,
		End:       3,
	}
	if err := s.Schedule(cmd1); err != nil {
		t.Fatalf("first schedule failed: %v", err)
	}
	if got := len(ch); got != 1 {
		t.Fatalf("expected first enqueue, got queue len=%d", got)
	}

	cmd2 := cmd1
	cmd2.Start = 2
	cmd2.End = 4
	if err := s.Schedule(cmd2); err != nil {
		t.Fatalf("second schedule failed: %v", err)
	}
	if got := len(ch); got != 1 {
		t.Fatalf("expected dedup not enqueue second command, got queue len=%d", got)
	}

	s.OnResult(types.BackfillResult{
		RoleID:    "role-a",
		StreamKey: "stream-a",
		Type:      types.BackfillTypeRange,
	})
	if err := s.Schedule(cmd2); err != nil {
		t.Fatalf("third schedule after result failed: %v", err)
	}
	if got := len(ch); got != 2 {
		t.Fatalf("expected enqueue after result release, got queue len=%d", got)
	}
}
