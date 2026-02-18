package role

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/observability/status"
	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/types"
)

type mockStatusReporter struct {
	mu     sync.Mutex
	events []status.Event
}

func (m *mockStatusReporter) Report(_ context.Context, event status.Event) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events = append(m.events, event)
	return nil
}

func (m *mockStatusReporter) Close() error { return nil }

func (m *mockStatusReporter) snapshot() []status.Event {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]status.Event, len(m.events))
	copy(out, m.events)
	return out
}

func TestFinalizeQueuedMessageSuccessAfterAllPendingDone(t *testing.T) {
	reporter := &mockStatusReporter{}
	r := &Role{
		ID:             "role-a",
		statusReporter: reporter,
		tasks:          map[string]*queueTaskState{},
	}
	start := time.Now().Add(-2 * time.Second)
	r.registerQueuedTask("task-1", "run-1", 2, start)

	msg := &types.Message{Metadata: map[string]any{
		"task_id": "task-1",
		"run_id":  "run-1",
	}}
	r.finalizeQueuedMessage(context.Background(), msg, "run-1", nil, 0, "")
	if len(reporter.snapshot()) != 0 {
		t.Fatalf("expected no final event after first message, got %d", len(reporter.snapshot()))
	}

	r.finalizeQueuedMessage(context.Background(), msg, "run-1", nil, 0, "")
	foundSuccess := false
	for _, evt := range reporter.snapshot() {
		if evt.Status == "SUCCESS" && evt.Stage == "final_succeeded" {
			foundSuccess = true
			break
		}
	}
	if !foundSuccess {
		t.Fatalf("expected final success event, got %+v", reporter.snapshot())
	}
}

func TestFinalizeQueuedMessageFailure(t *testing.T) {
	reporter := &mockStatusReporter{}
	r := &Role{
		ID:             "role-b",
		statusReporter: reporter,
		tasks:          map[string]*queueTaskState{},
	}
	r.registerQueuedTask("task-2", "run-2", 1, time.Now().Add(-time.Second))

	msg := &types.Message{Metadata: map[string]any{
		"task_id": "task-2",
		"run_id":  "run-2",
	}}
	r.finalizeQueuedMessage(context.Background(), msg, "run-2", errors.New("sink failed"), 3, "")

	foundFailed := false
	for _, evt := range reporter.snapshot() {
		if evt.Status == "FAILED" && evt.Stage == "final_failed" {
			foundFailed = true
			break
		}
	}
	if !foundFailed {
		t.Fatalf("expected final failed event, got %+v", reporter.snapshot())
	}
}
