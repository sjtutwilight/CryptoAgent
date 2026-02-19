package integrity

import (
	"testing"
	"time"
)

func TestStepCoreTable(t *testing.T) {
	now := time.Now()
	tests := []struct {
		name   string
		state  coreState
		input  coreInput
		assert func(t *testing.T, next coreState, actions []coreAction)
	}{
		{
			name: "equal produces drain action",
			state: coreState{
				ExpectedNext: 10,
			},
			input: coreInput{Kind: coreInputEqual, Arrival: now},
			assert: func(t *testing.T, next coreState, actions []coreAction) {
				if next.ExpectedNext != 11 {
					t.Fatalf("expected next=11, got %d", next.ExpectedNext)
				}
				if len(actions) != 1 || actions[0].Kind != coreActionDrainFrom || actions[0].From != 11 {
					t.Fatalf("unexpected actions: %#v", actions)
				}
			},
		},
		{
			name:  "cover cleans up and drains",
			state: coreState{ExpectedNext: 20},
			input: coreInput{Kind: coreInputCover, Seq: 25, Arrival: now},
			assert: func(t *testing.T, next coreState, actions []coreAction) {
				if next.ExpectedNext != 26 {
					t.Fatalf("expected next=26, got %d", next.ExpectedNext)
				}
				if len(actions) != 2 || actions[0].Kind != coreActionCleanupLE || actions[0].LE != 25 ||
					actions[1].Kind != coreActionDrainFrom || actions[1].From != 26 {
					t.Fatalf("unexpected actions: %#v", actions)
				}
			},
		},
		{
			name: "gap eager triggers backfill",
			state: coreState{
				ExpectedNext: 100,
				Initialized:  true,
			},
			input: coreInput{Kind: coreInputGap, Seq: 120, Arrival: now, EagerGap: 3, MaxRange: 10},
			assert: func(t *testing.T, next coreState, actions []coreAction) {
				if next.WaitStart.IsZero() {
					t.Fatalf("expected wait start to be set")
				}
				if len(actions) != 1 || actions[0].Kind != coreActionTriggerBackfill {
					t.Fatalf("unexpected actions: %#v", actions)
				}
				if actions[0].Start != 100 || actions[0].End != 109 {
					t.Fatalf("unexpected backfill range: %#v", actions[0])
				}
			},
		},
		{
			name:  "budget triggers advance",
			state: coreState{ExpectedNext: 10, SeenMax: 30},
			input: coreInput{Kind: coreInputBudget, MaxGap: 8},
			assert: func(t *testing.T, next coreState, actions []coreAction) {
				if len(actions) != 1 || actions[0].Kind != coreActionAdvanceExpected || actions[0].Target != 22 {
					t.Fatalf("unexpected actions: %#v", actions)
				}
			},
		},
		{
			name: "hard timeout takes precedence over soft timeout",
			state: coreState{
				ExpectedNext: 200,
				SeenMax:      260,
				WaitStart:    now.Add(-5 * time.Second),
			},
			input: coreInput{
				Kind:        coreInputTimeout,
				Now:         now,
				HardTimeout: 3 * time.Second,
				MaxDelay:    100 * time.Millisecond,
				MaxRange:    5,
			},
			assert: func(t *testing.T, next coreState, actions []coreAction) {
				if len(actions) != 1 || actions[0].Kind != coreActionAdvanceExpected || actions[0].Reason != "hard-timeout" {
					t.Fatalf("unexpected actions: %#v", actions)
				}
				if actions[0].Target != 260 {
					t.Fatalf("unexpected hard-timeout target: %d", actions[0].Target)
				}
			},
		},
		{
			name:  "advance emits cleanup and updates state",
			state: coreState{ExpectedNext: 50},
			input: coreInput{Kind: coreInputAdvance, Target: 70, Now: now},
			assert: func(t *testing.T, next coreState, actions []coreAction) {
				if next.ExpectedNext != 70 {
					t.Fatalf("expected next=70, got %d", next.ExpectedNext)
				}
				if len(actions) != 1 || actions[0].Kind != coreActionCleanupLE || actions[0].LE != 69 {
					t.Fatalf("unexpected actions: %#v", actions)
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			next, actions := stepCore(tc.state, tc.input)
			tc.assert(t, next, actions)
		})
	}
}
