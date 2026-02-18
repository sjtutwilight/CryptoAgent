package types

import (
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestBackfillCmdEnsureDefaultsBackwardCompatible(t *testing.T) {
	now := time.Unix(1730000000, 1234)
	cmd := BackfillCmd{
		Type:      BackfillTypeSnapshot,
		RoleID:    "role-a",
		StreamKey: "stream-a",
	}
	cmd.EnsureDefaults(now)

	if cmd.Key != "role-a|stream-a|snapshot" {
		t.Fatalf("unexpected key: %s", cmd.Key)
	}
	if cmd.Attempt != 1 {
		t.Fatalf("expected attempt=1, got %d", cmd.Attempt)
	}
	if cmd.SessionID == "" || cmd.CmdID == "" {
		t.Fatalf("expected session/cmd id generated, session=%q cmd=%q", cmd.SessionID, cmd.CmdID)
	}
}

func TestBackfillResultEnsureDefaultsAndSerialize(t *testing.T) {
	now := time.Unix(1730000001, 0)
	cmd := BackfillCmd{
		Type:      BackfillTypeRange,
		RoleID:    "role-b",
		StreamKey: "stream-b",
		Start:     10,
		End:       20,
	}
	cmd.EnsureDefaults(now)
	result := BackfillResult{
		Status:     BackfillResultFail,
		ErrorClass: "ENQUEUE_TIMEOUT",
	}
	result.EnsureDefaultsFromCmd(cmd, now)

	if result.ErrorClass != "enqueue_timeout" {
		t.Fatalf("unexpected error class: %s", result.ErrorClass)
	}
	if result.Key != cmd.Key || result.CmdID != cmd.CmdID || result.SessionID != cmd.SessionID {
		t.Fatalf("result defaults not copied from cmd: %+v cmd=%+v", result, cmd)
	}

	raw, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	var decoded BackfillResult
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if decoded.CmdID != cmd.CmdID || decoded.ErrorClass != "enqueue_timeout" {
		t.Fatalf("decoded result mismatch: %+v", decoded)
	}
}

func TestBackfillErrorClassMapping(t *testing.T) {
	cases := []struct {
		err  error
		want string
	}{
		{nil, ""},
		{ErrBackfillNoTarget, "no_target"},
		{ErrBackfillQueueFull, "queue_full"},
		{ErrBackfillEnqueueTimeout, "enqueue_timeout"},
		{errors.New("other"), "unknown"},
	}
	for _, tc := range cases {
		if got := BackfillErrorClass(tc.err); got != tc.want {
			t.Fatalf("BackfillErrorClass(%v)=%q, want %q", tc.err, got, tc.want)
		}
	}
}
