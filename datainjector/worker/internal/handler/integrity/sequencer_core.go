package integrity

import "time"

type coreInputKind string

const (
	coreInputEqual   coreInputKind = "equal"
	coreInputCover   coreInputKind = "cover"
	coreInputGap     coreInputKind = "gap"
	coreInputTimeout coreInputKind = "timeout"
	coreInputBudget  coreInputKind = "budget"
	coreInputAdvance coreInputKind = "advance"
)

type coreActionKind string

const (
	coreActionCleanupLE       coreActionKind = "cleanup_le"
	coreActionDrainFrom       coreActionKind = "drain_from"
	coreActionTriggerBackfill coreActionKind = "trigger_backfill"
	coreActionAdvanceExpected coreActionKind = "advance_expected"
)

type coreState struct {
	ExpectedNext     uint64
	SeenMax          uint64
	Initialized      bool
	AwaitingSnapshot bool
	WaitStart        time.Time
}

type coreInput struct {
	Kind coreInputKind

	Seq     uint64
	Now     time.Time
	Arrival time.Time
	Target  uint64

	EagerGap    uint64
	MaxRange    uint64
	MaxDelay    time.Duration
	HardTimeout time.Duration
	MaxGap      uint64
}

type coreAction struct {
	Kind coreActionKind

	LE     uint64
	From   uint64
	Start  uint64
	End    uint64
	Target uint64
	Reason string
}

func stepCore(state coreState, in coreInput) (coreState, []coreAction) {
	next := state
	actions := make([]coreAction, 0, 2)
	switch in.Kind {
	case coreInputEqual:
		next.WaitStart = in.Arrival
		next.ExpectedNext++
		actions = append(actions, coreAction{
			Kind: coreActionDrainFrom,
			From: next.ExpectedNext,
		})
	case coreInputCover:
		actions = append(actions, coreAction{
			Kind: coreActionCleanupLE,
			LE:   in.Seq,
		})
		next.ExpectedNext = in.Seq + 1
		next.WaitStart = in.Arrival
		actions = append(actions, coreAction{
			Kind: coreActionDrainFrom,
			From: next.ExpectedNext,
		})
	case coreInputGap:
		if next.Initialized && next.WaitStart.IsZero() {
			next.WaitStart = in.Arrival
		}
		if in.EagerGap > 0 && in.Seq > next.ExpectedNext {
			gap := in.Seq - next.ExpectedNext
			if gap > in.EagerGap {
				end := in.Seq - 1
				if in.MaxRange > 0 && end-next.ExpectedNext+1 > in.MaxRange {
					end = next.ExpectedNext + in.MaxRange - 1
				}
				actions = append(actions, coreAction{
					Kind:   coreActionTriggerBackfill,
					Start:  next.ExpectedNext,
					End:    end,
					Reason: "gap",
				})
			}
		}
	case coreInputTimeout:
		if next.WaitStart.IsZero() {
			return next, actions
		}
		elapsed := in.Now.Sub(next.WaitStart)
		if in.HardTimeout > 0 && elapsed > in.HardTimeout {
			target := next.SeenMax
			if target <= next.ExpectedNext {
				target = next.ExpectedNext + 1
			}
			actions = append(actions, coreAction{
				Kind:   coreActionAdvanceExpected,
				Target: target,
				Reason: "hard-timeout",
			})
			return next, actions
		}
		if in.MaxDelay > 0 && elapsed > in.MaxDelay {
			end := next.SeenMax
			if in.MaxRange > 0 {
				end = minUint64(next.ExpectedNext+in.MaxRange-1, next.SeenMax)
			}
			actions = append(actions, coreAction{
				Kind:   coreActionTriggerBackfill,
				Start:  next.ExpectedNext,
				End:    end,
				Reason: "timeout",
			})
			return next, actions
		}
	case coreInputBudget:
		if in.MaxGap == 0 || next.SeenMax <= next.ExpectedNext {
			return next, actions
		}
		if diff := next.SeenMax - next.ExpectedNext; diff > in.MaxGap {
			actions = append(actions, coreAction{
				Kind:   coreActionAdvanceExpected,
				Target: next.SeenMax - in.MaxGap,
				Reason: "max-gap",
			})
		}
	case coreInputAdvance:
		if in.Target <= next.ExpectedNext {
			return next, actions
		}
		if in.Target > 0 {
			actions = append(actions, coreAction{
				Kind: coreActionCleanupLE,
				LE:   in.Target - 1,
			})
		}
		next.ExpectedNext = in.Target
		next.WaitStart = in.Now
	}
	return next, actions
}
