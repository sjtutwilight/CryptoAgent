package role

import (
	"context"
	"errors"
	"testing"
	"time"

	handlerpkg "github.com/twilight-labs/dataplatform/datainjector/worker/internal/handler"
	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/queue"
	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/types"
)

type alwaysFailSink struct{}

func (s *alwaysFailSink) Write(_ *types.Message) error {
	return errors.New("sink failure")
}

func TestQueuePipelineSinkFailureMustNotReportSuccess(t *testing.T) {
	reporter := &mockStatusReporter{}
	r := &Role{
		ID:             "role-test",
		q:              queue.NewBounded[*types.Message](16),
		handlers:       []handlerpkg.Handler{&handlerpkg.NoopHandler{}},
		sink:           &alwaysFailSink{},
		statusReporter: reporter,
		tasks:          map[string]*queueTaskState{},
		queueRetries:   1,
		taskTTL:        5 * time.Minute,
		maxTrackedTask: 100,
		strictFinalize: true,
	}

	taskID := "task-integration"
	runID := "run-integration"
	r.registerQueuedTask(taskID, runID, 1, time.Now())
	msg := &types.Message{
		Metadata: map[string]any{
			"task_id": taskID,
			"run_id":  runID,
		},
		Payload: []byte(`{"k":"v"}`),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go r.consume(ctx)
	if err := r.q.Enqueue(ctx, msg); err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		events := reporter.snapshot()
		hasFinalFailed := false
		hasSuccess := false
		for _, evt := range events {
			if evt.Stage == "final_failed" {
				hasFinalFailed = true
			}
			if evt.Status == "SUCCESS" || evt.Stage == "final_succeeded" {
				hasSuccess = true
			}
		}
		if hasFinalFailed {
			if hasSuccess {
				t.Fatalf("unexpected success events after sink failure: %+v", events)
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("expected final_failed event, got %+v", reporter.snapshot())
}
