package integrity

import "time"

// coreInputKind 描述纯状态机可接收的输入事件类型。
type coreInputKind string

const (
	coreInputEqual   coreInputKind = "equal"
	coreInputCover   coreInputKind = "cover"
	coreInputGap     coreInputKind = "gap"
	coreInputTimeout coreInputKind = "timeout"
	coreInputBudget  coreInputKind = "budget"
	coreInputAdvance coreInputKind = "advance"
)

// coreActionKind 描述状态机输出给外层执行层的动作。
type coreActionKind string

const (
	coreActionCleanupLE       coreActionKind = "cleanup_le"
	coreActionDrainFrom       coreActionKind = "drain_from"
	coreActionTriggerBackfill coreActionKind = "trigger_backfill"
	coreActionAdvanceExpected coreActionKind = "advance_expected"
)

// coreState 仅保留与顺序推进直接相关的最小状态。
type coreState struct {
	ExpectedNext     uint64
	SeenMax          uint64
	Initialized      bool
	AwaitingSnapshot bool
	WaitStart        time.Time
}

// coreInput 封装 stepCore 所需上下文，便于表驱动测试覆盖分支。
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

// coreAction 是纯状态机输出，由 SequenceEngine 解释并执行副作用。
type coreAction struct {
	Kind coreActionKind

	LE     uint64
	From   uint64
	Start  uint64
	End    uint64
	Target uint64
	Reason string
}

// stepCore 是纯函数状态机：输入当前状态与事件，输出下一状态与待执行动作。
// 约束：不访问 buffer、scheduler、metrics，所有副作用通过 action 交给外层。
func stepCore(state coreState, in coreInput) (coreState, []coreAction) {
	next := state
	actions := make([]coreAction, 0, 2)
	switch in.Kind {
	case coreInputEqual:
		// 命中 expected，推进 expected，并尝试从新 expected 开始连续 drain。
		next.WaitStart = in.Arrival
		next.ExpectedNext++
		actions = append(actions, coreAction{
			Kind: coreActionDrainFrom,
			From: next.ExpectedNext,
		})
	case coreInputCover:
		// 覆盖 expected（如 Binance U/u），清理 <=Seq 的缓存后直接跳到 Seq+1。
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
		// 发现 gap 时只记录等待窗口；是否立即补数由 EagerGap / MaxRange 决定。
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
		// hard timeout 优先：宁可前跳也避免长期卡死。
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
		// soft timeout：优先触发补数，保持顺序语义。
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
		// SeenMax 与 ExpectedNext 差距过大时执行预算前跳，避免缓存无界增长。
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
		// 外部强制推进（例如 hard-timeout 已决策），同步清理旧缓存。
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
