package status

import (
	"encoding/json"
	"testing"
	"time"
)

type legacyEvent struct {
	TaskID     string    `json:"taskId"`
	Status     string    `json:"status"`
	StatusCode int       `json:"statusCode,omitempty"`
	Message    string    `json:"message,omitempty"`
	DurationMs int64     `json:"durationMs,omitempty"`
	DataSize   int       `json:"dataSize,omitempty"`
	Retryable  bool      `json:"retryable,omitempty"`
	Timestamp  time.Time `json:"timestamp"`
}

func TestEventIsBackwardCompatibleWithLegacyConsumer(t *testing.T) {
	now := time.Now().UTC()
	evt := Event{
		TaskID:     "task-1",
		Status:     "SUCCESS",
		StatusCode: 200,
		Stage:      "final_succeeded",
		RoleID:     "role-a",
		RunID:      "run-1",
		Timestamp:  now,
	}
	b, err := json.Marshal(evt)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	var legacy legacyEvent
	if err := json.Unmarshal(b, &legacy); err != nil {
		t.Fatalf("legacy unmarshal failed: %v", err)
	}
	if legacy.TaskID != evt.TaskID || legacy.Status != evt.Status || !legacy.Timestamp.Equal(now) {
		t.Fatalf("legacy decode mismatch: %+v", legacy)
	}
}
