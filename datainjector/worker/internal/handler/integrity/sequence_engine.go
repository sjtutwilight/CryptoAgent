package integrity

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/observability/logging"
	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/observability/metrics"
	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/types"
	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/util"
)

// RangeEvaluator 判断一个事件是否覆盖期望序列。
type RangeEvaluator interface {
	Covers(expected uint64, evt *Event) bool
}

type rangeEvalFunc func(expected uint64, evt *Event) bool

func (f rangeEvalFunc) Covers(expected uint64, evt *Event) bool {
	return f(expected, evt)
}

// SequenceEngine 负责顺序性控制与补数决策。
type SequenceEngine struct {
	cfg        Config         // 全局配置
	rangeEval  RangeEvaluator // 范围覆盖策略
	buffer     *reorderBuffer // 乱序缓存
	backfill   Scheduler      // 补数调度器
	state      engineState    // 运行时状态
	dedupe     *deduper       // 幂等过滤器
	gate       Gate           // 下游放行阀门
	streamName string         // 流标识，用于日志
	roleID     string         // role_id（来自消息 metadata）
	sessionMu  sync.Mutex
	sessions   map[string]*backfillSession
	compMu     sync.Mutex
	compLoaded bool
	compQueue  []compensationItem
}

type compensationItem struct {
	Cmd        types.BackfillCmd `json:"cmd"`
	ErrorClass string            `json:"error_class"`
	RetryCount int               `json:"retry_count"`
	UpdatedAt  time.Time         `json:"updated_at"`
}

type engineState struct {
	ExpectedNext     uint64         // 当前期望的序列号
	SeenMax          uint64         // 已见到的最大序列
	Initialized      bool           // 是否已初始化
	AwaitingSnapshot bool           // 是否等待快照确认
	WaitStart        time.Time      // 当前等待窗口的起点时间
	LastSweep        time.Time      // 上次清理时间
	LastCompReplay   time.Time      // 上次补偿重放时间
	LastBackfill     backfillRecord // 最近一次补数记录
	LastPressureFill time.Time      // 背压状态下上次触发补数时间
}

type backfillSessionState string

const (
	sessionIdle     backfillSessionState = "idle"
	sessionPending  backfillSessionState = "pending"
	sessionCooldown backfillSessionState = "cooldown"
)

type backfillSession struct {
	Key           string
	Type          string
	RoleID        string
	StreamKey     string
	State         backfillSessionState
	SessionID     string
	CmdID         string
	Attempt       int
	PendingSince  time.Time
	CooldownUntil time.Time
	Failures      int
	CurrentStart  uint64
	CurrentEnd    uint64
	IntentStart   uint64
	IntentEnd     uint64
	HasIntent     bool
}

type Decision struct {
	Deliver []*types.Message // 可以直接下发的消息
}

func NewSequenceEngine(cfg Config, eval RangeEvaluator, sched Scheduler, gate Gate, dedupe *deduper, stream string) *SequenceEngine {
	cfg.Normalise()
	if eval == nil {
		eval = rangeEvalFunc(func(expected uint64, evt *Event) bool {
			if !evt.HasRange {
				return false
			}
			return evt.RangeStart <= expected && evt.Seq >= expected
		})
	}
	if gate == nil {
		gate = &noopGate{}
	}
	return &SequenceEngine{
		cfg:        cfg,
		rangeEval:  eval,
		buffer:     newReorderBuffer(cfg),
		backfill:   sched,
		dedupe:     dedupe,
		gate:       gate,
		streamName: stream,
		sessions:   make(map[string]*backfillSession),
	}
}

func (e *SequenceEngine) Handle(evt *Event) Decision {
	if evt == nil || evt.Message == nil {
		return Decision{}
	}
	e.captureIdentity(evt)
	if evt.Arrival.IsZero() {
		evt.Arrival = time.Now()
	}
	e.replayCompensations(evt.Arrival)
	isSnapshot := isSnapshotEvent(evt)
	sideChannel := e.cfg.SnapshotSideChannelEnabled()

	if !e.state.Initialized {
		// 首条消息直接作为初始 expected，避免空补数
		e.bootstrap(evt)
		if isSnapshot {
			if sideChannel {
				return e.deliver([]*Event{evt})
			}
			e.state.AwaitingSnapshot = true
			return e.deliver([]*Event{evt})
		}
		// Snapshot-gated 流在首条 diff 到来时即进入等待快照状态，
		// 避免持续下发 diff 但下游因缺快照无法应用，导致长时间无产出。
		if e.snapshotGateEnabled() {
			e.state.AwaitingSnapshot = true
			e.buffer.add(evt)
			e.ensureWait(evt.Arrival)
			e.triggerBackfillWithReason(e.state.ExpectedNext, e.state.ExpectedNext, evt.Arrival, "init")
			return Decision{}
		}
		return e.deliver([]*Event{evt})
	}

	if isSnapshot {
		if sideChannel {
			return e.deliver([]*Event{evt})
		}
		e.state.AwaitingSnapshot = true
		if evt.Seq > e.state.SeenMax {
			e.state.SeenMax = evt.Seq
		}
		e.state.WaitStart = evt.Arrival
		return e.deliver([]*Event{evt})
	}

	if evt.Seq > e.state.SeenMax {
		e.state.SeenMax = evt.Seq
	}

	if e.state.AwaitingSnapshot {
		e.buffer.add(evt)
		e.ensureWait(evt.Arrival)
		e.retrySnapshotBackfill(evt.Arrival)
		return Decision{}
	}

	switch {
	case evt.Seq == e.state.ExpectedNext:
		return e.onEqual(evt)
	case evt.Seq < e.state.ExpectedNext:
		return Decision{}
	case e.rangeEval.Covers(e.state.ExpectedNext, evt):
		return e.onCover(evt)
	default:
		return e.onGap(evt)
	}
}

func (e *SequenceEngine) OnSnapshotApplied(lastSeq uint64) Decision {
	now := time.Now()
	e.replayCompensations(now)
	if e.cfg.SnapshotSideChannelEnabled() {
		// sidechannel 模式下不会依赖快照回调放行 diff。
		return Decision{}
	}
	e.state.AwaitingSnapshot = false
	e.state.Initialized = true
	if lastSeq > e.state.SeenMax {
		e.state.SeenMax = lastSeq
	}
	if lastSeq != math.MaxUint64 {
		e.state.ExpectedNext = lastSeq + 1
		e.buffer.cleanup(lastSeq)
	}
	e.state.WaitStart = now

	// 通知 Gate 快照已应用
	shouldReleaseAll := e.gate.OnSnapshotApplied(lastSeq)

	if shouldReleaseAll {
		// Gate 要求释放所有缓冲消息（如 snapshotHoldGate）
		events, next := e.buffer.drain(e.state.ExpectedNext)
		e.state.ExpectedNext = next
		return e.deliver(events)
	}

	// 否则正常处理（如 finalityGate 可能仍需要等待确认）
	return Decision{}
}

func (e *SequenceEngine) bootstrap(evt *Event) {
	e.state.Initialized = true
	if e.snapshotGateEnabled() {
		// For snapshot-gated streams (e.g. Binance depth), keep ExpectedNext at current seq
		// so the next out-of-order observation naturally triggers snapshot backfill flow.
		e.state.ExpectedNext = evt.Seq
	} else {
		if evt.Seq == math.MaxUint64 {
			e.state.ExpectedNext = math.MaxUint64
		} else {
			e.state.ExpectedNext = evt.Seq + 1
		}
	}
	e.state.SeenMax = evt.Seq
	e.state.WaitStart = evt.Arrival
}

func (e *SequenceEngine) onEqual(evt *Event) Decision {
	e.state.WaitStart = evt.Arrival
	e.state.ExpectedNext++
	events, next := e.buffer.drain(e.state.ExpectedNext)
	e.state.ExpectedNext = next
	return e.deliver(append([]*Event{evt}, events...))
}

func (e *SequenceEngine) onCover(evt *Event) Decision {
	e.buffer.cleanup(evt.Seq)
	e.state.ExpectedNext = evt.Seq + 1
	e.state.WaitStart = evt.Arrival
	events, next := e.buffer.drain(e.state.ExpectedNext)
	e.state.ExpectedNext = next
	return e.deliver(append([]*Event{evt}, events...))
}

func (e *SequenceEngine) onGap(evt *Event) Decision {
	e.buffer.add(evt)
	e.ensureWait(evt.Arrival)
	gap := evt.Seq - e.state.ExpectedNext
	metrics.RecordIntegrityGap(e.roleID, e.streamName)
	logging.Info(context.Background(), logging.EventIntegrityGapDetected, "sequence gap detected", logging.Fields{
		"role_id":      e.roleID,
		"stream_key":   e.streamName,
		"expected_seq": e.state.ExpectedNext,
		"seen_seq":     evt.Seq,
		"gap":          gap,
	})
	if e.cfg.Sequence.EagerGap > 0 && gap > e.cfg.Sequence.EagerGap {
		end := evt.Seq - 1
		if e.cfg.Sequence.MaxRange > 0 && end-e.state.ExpectedNext+1 > e.cfg.Sequence.MaxRange {
			end = e.state.ExpectedNext + e.cfg.Sequence.MaxRange - 1
		}
		e.triggerBackfillForEvent(e.state.ExpectedNext, end, evt.Arrival, evt)
	}
	if !e.checkTimeout(evt.Arrival, evt) {
		e.checkBudget(evt.Arrival)
	}
	e.runSweep(evt.Arrival)
	return Decision{}
}

func (e *SequenceEngine) ensureWait(now time.Time) {
	if !e.state.Initialized {
		return
	}
	if e.state.WaitStart.IsZero() {
		e.state.WaitStart = now
	}
}

func (e *SequenceEngine) retrySnapshotBackfill(now time.Time) {
	if !e.state.AwaitingSnapshot || !e.snapshotGateEnabled() {
		return
	}
	e.resolvePendingTimeout(now)
	if e.state.WaitStart.IsZero() {
		return
	}
	timeout := e.cfg.Sequence.HardTimeout
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	if now.Sub(e.state.WaitStart) < timeout {
		return
	}
	start := e.state.ExpectedNext
	end := e.state.SeenMax
	if end < start {
		end = start
	}
	logging.Warn(context.Background(), logging.EventIntegrityBackfillRetry, "snapshot wait timeout, retry backfill", logging.Fields{
		"role_id":      e.roleID,
		"stream_key":   e.streamName,
		"expected_seq": e.state.ExpectedNext,
		"seen_max":     e.state.SeenMax,
		"elapsed_ms":   now.Sub(e.state.WaitStart).Milliseconds(),
	})
	// 以重试时间作为新的等待起点，避免高流量下频繁触发。
	e.state.WaitStart = now
	e.triggerBackfillWithReason(start, end, now, "timeout")
}

func (e *SequenceEngine) checkTimeout(now time.Time, evt *Event) bool {
	e.resolvePendingTimeout(now)
	if e.cfg.Sequence.MaxDelay <= 0 && e.cfg.Sequence.HardTimeout <= 0 {
		return false
	}
	if e.state.WaitStart.IsZero() {
		return false
	}
	elapsed := now.Sub(e.state.WaitStart)
	if e.cfg.Sequence.MaxDelay > 0 && elapsed > e.cfg.Sequence.MaxDelay {
		end := e.state.SeenMax
		if e.cfg.Sequence.MaxRange > 0 {
			end = minUint64(e.state.ExpectedNext+e.cfg.Sequence.MaxRange-1, e.state.SeenMax)
		}
		e.triggerBackfillForEvent(e.state.ExpectedNext, end, now, evt)
		return true
	}
	if e.cfg.Sequence.HardTimeout > 0 && elapsed > e.cfg.Sequence.HardTimeout {
		target := e.state.SeenMax
		if target <= e.state.ExpectedNext {
			target = e.state.ExpectedNext + 1
		}
		e.advance(target, now, "hard-timeout")
		return true
	}
	return false
}

func (e *SequenceEngine) triggerBackfillForEvent(start, end uint64, now time.Time, evt *Event) bool {
	if e.shouldThrottleBackpressureBackfill(now, evt) {
		logging.Warn(context.Background(), logging.EventIntegrityBackfillSkipped, "skip backfill under backpressure cooldown", logging.Fields{
			"role_id":            e.roleID,
			"stream_key":         e.streamName,
			"start":              start,
			"end":                end,
			"backpressure_wait":  e.cfg.Backfill.BackpressureGapCooldown.Milliseconds(),
			"backpressure_since": e.state.LastPressureFill.UnixMilli(),
		})
		return false
	}
	ok := e.triggerBackfillWithReason(start, end, now, e.backfillReasonFromEvent(evt))
	if ok && isBackpressureEvent(evt) {
		e.state.LastPressureFill = now
	}
	return ok
}

func (e *SequenceEngine) shouldThrottleBackpressureBackfill(now time.Time, evt *Event) bool {
	if !isBackpressureEvent(evt) {
		return false
	}
	window := e.cfg.Backfill.BackpressureGapCooldown
	if window <= 0 {
		return false
	}
	if e.state.LastPressureFill.IsZero() {
		return false
	}
	return now.Sub(e.state.LastPressureFill) < window
}

func (e *SequenceEngine) checkBudget(now time.Time) {
	if e.cfg.Sequence.MaxGap == 0 || e.state.SeenMax <= e.state.ExpectedNext {
		return
	}
	if diff := e.state.SeenMax - e.state.ExpectedNext; diff > e.cfg.Sequence.MaxGap {
		target := e.state.SeenMax - e.cfg.Sequence.MaxGap
		e.advance(target, now, "max-gap")
	}
}

func (e *SequenceEngine) triggerBackfill(start, end uint64, now time.Time) bool {
	return e.triggerBackfillWithReason(start, end, now, "gap")
}

func (e *SequenceEngine) triggerBackfillWithReason(start, end uint64, now time.Time, reason string) bool {
	if e.backfill == nil || len(e.cfg.Backfill.Options) == 0 {
		return false
	}
	if end < start {
		return false
	}
	if e.cfg.Backfill.ResultDrivenEnabled {
		return e.triggerBackfillWithSession(start, end, now, reason)
	}

	// 冷却检查：相同范围且在冷却期内，跳过
	if e.state.LastBackfill.start == start && e.state.LastBackfill.end == end &&
		now.Sub(e.state.LastBackfill.at) < e.cfg.Backfill.Cooldown {
		return false
	}

	cmd, ok := e.buildBackfillCmd(start, end, "", 1, now, reason)
	if !ok {
		return false
	}
	if len(cmd.Attempts) == 0 {
		logging.Warn(context.Background(), logging.EventIntegrityBackfillSkipped, "backfill request skipped: no executable attempts", logging.Fields{
			"role_id":     e.roleID,
			"stream_key":  e.streamName,
			"start":       start,
			"end":         end,
			"attempts":    0,
			"snapshot":    e.cfg.Backfill.SnapshotBased,
			"backfill_on": len(e.cfg.Backfill.Options),
		})
		return false
	}

	if err := e.backfill.Schedule(cmd); err == nil {
		e.state.LastBackfill = backfillRecord{start: start, end: end, at: now}
		if e.snapshotGateEnabled() {
			e.state.AwaitingSnapshot = true
		}
		return true
	} else {
		logging.Warn(context.Background(), logging.EventIntegrityBackfillEnqueue, "backfill schedule failed", logging.Fields{
			"role_id":       e.roleID,
			"stream_key":    e.streamName,
			"backfill_type": cmd.Type,
			"start":         cmd.Start,
			"end":           cmd.End,
			"error":         err.Error(),
			"error_class":   types.BackfillErrorClass(err),
		})
		if e.cfg.Backfill.PersistentCompensation {
			e.enqueueCompensation(cmd, err, now)
		}
	}
	return false
}

func (e *SequenceEngine) triggerBackfillWithSession(start, end uint64, now time.Time, reason string) bool {
	kind := e.backfillType()
	key := types.BackfillSessionKey(e.roleID, e.streamName, kind)
	session := e.getOrCreateSession(key, kind)

	e.sessionMu.Lock()
	defer e.sessionMu.Unlock()
	session.RoleID = e.roleID
	session.StreamKey = e.streamName
	session.Type = kind
	session.Key = key

	if session.State == sessionCooldown {
		if now.Before(session.CooldownUntil) {
			logging.Warn(context.Background(), logging.EventIntegrityBackfillSkipped, "backfill blocked by session cooldown", logging.Fields{
				"role_id":       e.roleID,
				"stream_key":    e.streamName,
				"backfill_type": kind,
				"session_key":   key,
				"cooldown_ms":   session.CooldownUntil.Sub(now).Milliseconds(),
			})
			return false
		}
		session.State = sessionIdle
		session.CooldownUntil = time.Time{}
	}
	if session.State == sessionPending {
		e.mergeSessionIntent(session, start, end)
		metrics.RecordBackfillScheduleDedup(e.roleID, e.streamName, kind)
		logging.Info(context.Background(), logging.EventIntegrityBackfillDedup, "backfill deduplicated into pending session", logging.Fields{
			"role_id":       e.roleID,
			"stream_key":    e.streamName,
			"backfill_type": kind,
			"session_key":   key,
			"intent_start":  session.IntentStart,
			"intent_end":    session.IntentEnd,
		})
		return false
	}

	attempt := session.Attempt + 1
	sessionID := fmt.Sprintf("%s#%d", key, now.UnixNano())
	cmd, ok := e.buildBackfillCmd(start, end, sessionID, attempt, now, reason)
	if !ok {
		return false
	}
	if len(cmd.Attempts) == 0 {
		logging.Warn(context.Background(), logging.EventIntegrityBackfillSkipped, "backfill request skipped: no executable attempts", logging.Fields{
			"role_id":     e.roleID,
			"stream_key":  e.streamName,
			"start":       start,
			"end":         end,
			"attempts":    0,
			"snapshot":    e.cfg.Backfill.SnapshotBased,
			"backfill_on": len(e.cfg.Backfill.Options),
		})
		return false
	}
	if err := e.backfill.Schedule(cmd); err != nil {
		logging.Warn(context.Background(), logging.EventIntegrityBackfillEnqueue, "backfill schedule failed", logging.Fields{
			"role_id":       e.roleID,
			"stream_key":    e.streamName,
			"backfill_type": cmd.Type,
			"start":         cmd.Start,
			"end":           cmd.End,
			"cmd_id":        cmd.CmdID,
			"session_id":    cmd.SessionID,
			"session_key":   cmd.Key,
			"error":         err.Error(),
			"error_class":   types.BackfillErrorClass(err),
		})
		if e.cfg.Backfill.PersistentCompensation {
			e.enqueueCompensation(cmd, err, now)
		}
		return false
	}

	e.state.LastBackfill = backfillRecord{start: start, end: end, at: now}
	if e.snapshotGateEnabled() {
		e.state.AwaitingSnapshot = true
	}
	session.State = sessionPending
	session.Attempt = attempt
	session.SessionID = cmd.SessionID
	session.CmdID = cmd.CmdID
	session.PendingSince = now
	session.CurrentStart = start
	session.CurrentEnd = end
	session.HasIntent = false
	metrics.SetBackfillSessionsInflight(e.roleID, e.streamName, kind, 1)
	logging.Info(context.Background(), logging.EventIntegrityBackfillSession, "backfill session entered pending", logging.Fields{
		"role_id":       e.roleID,
		"stream_key":    e.streamName,
		"backfill_type": kind,
		"session_key":   key,
		"session_id":    cmd.SessionID,
		"cmd_id":        cmd.CmdID,
		"attempt":       attempt,
		"start":         cmd.Start,
		"end":           cmd.End,
	})
	return true
}

func (e *SequenceEngine) buildBackfillCmd(start, end uint64, sessionID string, attempt int, now time.Time, reason string) (types.BackfillCmd, bool) {
	var cmd types.BackfillCmd
	var attempts []types.BackfillAttempt
	if e.cfg.Backfill.SnapshotBased {
		snapshotReason := normalizeSnapshotReason(reason)
		logging.Info(context.Background(), logging.EventIntegrityBackfillTrigger, "trigger snapshot backfill", logging.Fields{
			"role_id":       e.roleID,
			"stream_key":    e.streamName,
			"backfill_type": types.BackfillTypeSnapshot,
			"start":         start,
			"end":           end,
			"reason":        snapshotReason,
		})
		attempts = e.buildBackfillAttempts(types.BackfillTypeSnapshot, 0, 0, snapshotReason)
		cmd = types.BackfillCmd{
			Type:           types.BackfillTypeSnapshot,
			RoleID:         e.roleID,
			StreamKey:      e.streamName,
			Start:          0,
			End:            0,
			SnapshotSource: "backfill",
			SnapshotReason: snapshotReason,
			Attempts:       attempts,
		}
	} else {
		if e.cfg.Sequence.MaxRange > 0 {
			limit := start + e.cfg.Sequence.MaxRange - 1
			if limit < start {
				limit = math.MaxUint64
			}
			if end > limit {
				end = limit
			}
		}
		if start > math.MaxInt64 || end > math.MaxInt64 {
			return types.BackfillCmd{}, false
		}
		startInt := int64(start)
		endInt := int64(end)
		logging.Info(context.Background(), logging.EventIntegrityBackfillTrigger, "trigger range backfill", logging.Fields{
			"role_id":       e.roleID,
			"stream_key":    e.streamName,
			"backfill_type": types.BackfillTypeRange,
			"start":         startInt,
			"end":           endInt,
		})
		attempts = e.buildBackfillAttempts(types.BackfillTypeRange, startInt, endInt, reason)
		cmd = types.BackfillCmd{
			Type:      types.BackfillTypeRange,
			RoleID:    e.roleID,
			StreamKey: e.streamName,
			Start:     startInt,
			End:       endInt,
			Attempts:  attempts,
		}
	}
	if attempt > 0 {
		cmd.Attempt = attempt
	}
	if sessionID != "" {
		cmd.SessionID = sessionID
	}
	cmd.EnsureDefaults(now)
	return cmd, true
}

func (e *SequenceEngine) backfillType() string {
	if e.cfg.Backfill.SnapshotBased {
		return types.BackfillTypeSnapshot
	}
	return types.BackfillTypeRange
}

func (e *SequenceEngine) getOrCreateSession(key, kind string) *backfillSession {
	e.sessionMu.Lock()
	defer e.sessionMu.Unlock()
	if s, ok := e.sessions[key]; ok {
		return s
	}
	s := &backfillSession{
		Key:       key,
		Type:      kind,
		RoleID:    e.roleID,
		StreamKey: e.streamName,
		State:     sessionIdle,
	}
	e.sessions[key] = s
	return s
}

func (e *SequenceEngine) mergeSessionIntent(session *backfillSession, start, end uint64) {
	if session == nil {
		return
	}
	if !session.HasIntent {
		session.IntentStart = start
		session.IntentEnd = end
		session.HasIntent = true
		return
	}
	if start < session.IntentStart {
		session.IntentStart = start
	}
	if end > session.IntentEnd {
		session.IntentEnd = end
	}
}

func (e *SequenceEngine) OnBackfillResult(result types.BackfillResult) {
	now := time.Now()
	if result.FinishedAt.IsZero() {
		result.FinishedAt = now
	}
	result.ErrorClass = types.NormalizeBackfillErrorClass(result.ErrorClass)
	if result.Type == "" {
		result.Type = e.backfillType()
	}
	if result.RoleID == "" {
		result.RoleID = e.roleID
	}
	if result.StreamKey == "" {
		result.StreamKey = e.streamName
	}
	if result.Key == "" {
		result.Key = types.BackfillSessionKey(result.RoleID, result.StreamKey, result.Type)
	}
	if e.backfill != nil {
		e.backfill.OnResult(result)
	}
	if !e.cfg.Backfill.ResultDrivenEnabled {
		return
	}

	var (
		triggerNext bool
		nextStart   uint64
		nextEnd     uint64
		pendingFor  time.Duration
		status      = strings.ToLower(strings.TrimSpace(result.Status))
	)
	if status == "" {
		status = types.BackfillResultFail
	}

	e.sessionMu.Lock()
	session, ok := e.sessions[result.Key]
	if !ok {
		e.sessionMu.Unlock()
		return
	}
	if session.State != sessionPending {
		e.sessionMu.Unlock()
		return
	}
	if result.SessionID != "" && session.SessionID != "" && result.SessionID != session.SessionID {
		e.sessionMu.Unlock()
		return
	}

	if !session.PendingSince.IsZero() {
		pendingFor = result.FinishedAt.Sub(session.PendingSince)
		if pendingFor < 0 {
			pendingFor = 0
		}
	}
	session.PendingSince = time.Time{}
	metrics.SetBackfillSessionsInflight(result.RoleID, result.StreamKey, result.Type, 0)

	switch status {
	case types.BackfillResultSuccess:
		session.Failures = 0
		session.State = sessionIdle
	case types.BackfillResultTimeout, types.BackfillResultFail:
		session.Failures++
		threshold := e.cfg.Backfill.MaxFailures
		if threshold <= 0 {
			threshold = 1
		}
		if session.Failures >= threshold {
			session.State = sessionCooldown
			cooldown := e.cfg.Backfill.ExhaustCooldown
			if cooldown <= 0 {
				cooldown = e.cfg.Backfill.Cooldown
			}
			session.CooldownUntil = result.FinishedAt.Add(cooldown)
			session.Failures = 0
		} else {
			session.State = sessionIdle
		}
	default:
		session.State = sessionIdle
	}

	logging.Info(context.Background(), logging.EventIntegrityBackfillResult, "backfill result applied to session", logging.Fields{
		"role_id":       result.RoleID,
		"stream_key":    result.StreamKey,
		"backfill_type": result.Type,
		"session_key":   result.Key,
		"session_id":    result.SessionID,
		"cmd_id":        result.CmdID,
		"status":        status,
		"error_class":   result.ErrorClass,
		"pending_ms":    pendingFor.Milliseconds(),
		"state":         session.State,
	})

	if session.State == sessionIdle && session.HasIntent {
		triggerNext = true
		nextStart = session.IntentStart
		nextEnd = session.IntentEnd
		session.HasIntent = false
	}
	e.sessionMu.Unlock()

	metrics.RecordBackfillResult(result.RoleID, result.Type, status, result.ErrorClass)
	if pendingFor > 0 {
		metrics.RecordBackfillPendingDuration(result.RoleID, result.StreamKey, result.Type, status, pendingFor)
	}
	if triggerNext {
		e.triggerBackfillWithReason(nextStart, nextEnd, result.FinishedAt, "session_intent")
	}
}

func (e *SequenceEngine) resolvePendingTimeout(now time.Time) {
	if !e.cfg.Backfill.ResultDrivenEnabled {
		return
	}
	timeout := e.cfg.Sequence.HardTimeout
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	expired := make([]types.BackfillResult, 0)
	e.sessionMu.Lock()
	for _, session := range e.sessions {
		if session == nil || session.State != sessionPending || session.PendingSince.IsZero() {
			continue
		}
		if now.Sub(session.PendingSince) < timeout {
			continue
		}
		expired = append(expired, types.BackfillResult{
			CmdID:      session.CmdID,
			SessionID:  session.SessionID,
			Key:        session.Key,
			RoleID:     session.RoleID,
			StreamKey:  session.StreamKey,
			Type:       session.Type,
			Attempt:    session.Attempt,
			Status:     types.BackfillResultTimeout,
			ErrorClass: "timeout",
			FinishedAt: now,
		})
	}
	e.sessionMu.Unlock()

	for _, result := range expired {
		e.OnBackfillResult(result)
	}
}

func (e *SequenceEngine) advance(target uint64, now time.Time, reason string) {
	if target <= e.state.ExpectedNext {
		return
	}
	if target > 0 {
		e.buffer.cleanup(target - 1)
	}
	logging.Info(context.Background(), logging.EventIntegrityAdvance, "advance expected sequence", logging.Fields{
		"role_id":       e.roleID,
		"stream_key":    e.streamName,
		"expected_prev": e.state.ExpectedNext,
		"expected_new":  target,
		"reason":        reason,
	})
	e.state.ExpectedNext = target
	e.state.WaitStart = now
}

func (e *SequenceEngine) captureIdentity(evt *Event) {
	if evt == nil {
		return
	}
	if e.roleID == "" && evt.Message != nil && evt.Message.Metadata != nil {
		if v, ok := evt.Message.Metadata["role_id"]; ok && v != nil {
			e.roleID = fmt.Sprintf("%v", v)
		}
	}
	if evt.StreamKey != "" {
		e.streamName = evt.StreamKey
	}
}

func (e *SequenceEngine) runSweep(now time.Time) {
	e.state.LastSweep = now
	removed := e.buffer.sweep(now)
	if len(removed) == 0 {
		return
	}
	for _, seq := range removed {
		if seq < e.state.ExpectedNext {
			continue
		}
		e.triggerBackfillWithReason(seq, seq, now, "buffer_sweep")
	}
}

func (e *SequenceEngine) snapshotGateEnabled() bool {
	if e.cfg.SnapshotSideChannelEnabled() {
		return false
	}
	return e.cfg.Backfill.SnapshotBased || strings.EqualFold(e.cfg.Gate.Mode, "snapshot_hold")
}

func (e *SequenceEngine) backfillReasonFromEvent(evt *Event) string {
	if isBackpressureEvent(evt) {
		return "backpressure"
	}
	return "gap"
}

func (e *SequenceEngine) replayCompensations(now time.Time) {
	if !e.cfg.Backfill.PersistentCompensation || e.backfill == nil {
		return
	}
	e.compMu.Lock()
	defer e.compMu.Unlock()

	if !e.compLoaded {
		e.loadCompensationsLocked()
		e.compLoaded = true
	}
	if len(e.compQueue) == 0 {
		metrics.SetBackfillCompensationBacklog(e.roleID, 0)
		return
	}
	if !e.state.LastCompReplay.IsZero() && now.Sub(e.state.LastCompReplay) < e.cfg.Backfill.ReplayInterval {
		metrics.SetBackfillCompensationBacklog(e.roleID, len(e.compQueue))
		return
	}

	remaining := make([]compensationItem, 0, len(e.compQueue))
	for _, item := range e.compQueue {
		if err := e.backfill.Schedule(item.Cmd); err != nil {
			item.RetryCount++
			item.UpdatedAt = now
			remaining = append(remaining, item)
			continue
		}
		logging.Info(context.Background(), logging.EventIntegrityBackfillSuccess, "replayed compensated backfill", logging.Fields{
			"role_id":       e.roleID,
			"stream_key":    item.Cmd.StreamKey,
			"backfill_type": item.Cmd.Type,
			"start":         item.Cmd.Start,
			"end":           item.Cmd.End,
			"retry_count":   item.RetryCount,
		})
	}
	e.compQueue = remaining
	e.state.LastCompReplay = now
	metrics.SetBackfillCompensationBacklog(e.roleID, len(e.compQueue))
	e.persistCompensationsLocked()
}

func (e *SequenceEngine) enqueueCompensation(cmd types.BackfillCmd, err error, now time.Time) {
	e.compMu.Lock()
	defer e.compMu.Unlock()

	if !e.compLoaded {
		e.loadCompensationsLocked()
		e.compLoaded = true
	}
	key := compensationKey(cmd)
	for i := range e.compQueue {
		if compensationKey(e.compQueue[i].Cmd) == key {
			e.compQueue[i].ErrorClass = types.BackfillErrorClass(err)
			e.compQueue[i].RetryCount++
			e.compQueue[i].UpdatedAt = now
			e.persistCompensationsLocked()
			metrics.SetBackfillCompensationBacklog(e.roleID, len(e.compQueue))
			return
		}
	}
	item := compensationItem{
		Cmd:        cmd,
		ErrorClass: types.BackfillErrorClass(err),
		RetryCount: 0,
		UpdatedAt:  now,
	}
	e.compQueue = append(e.compQueue, item)
	maxPending := e.cfg.Backfill.CompensationMaxPending
	if maxPending > 0 && len(e.compQueue) > maxPending {
		dropped := len(e.compQueue) - maxPending
		e.compQueue = e.compQueue[dropped:]
		logging.Warn(context.Background(), logging.EventIntegrityBackfillSkipped, "compensation queue trimmed by max_pending", logging.Fields{
			"role_id":      e.roleID,
			"dropped":      dropped,
			"max_pending":  maxPending,
			"queue_length": len(e.compQueue),
		})
	}
	e.persistCompensationsLocked()
	metrics.SetBackfillCompensationBacklog(e.roleID, len(e.compQueue))
}

func (e *SequenceEngine) loadCompensationsLocked() {
	path := strings.TrimSpace(e.cfg.Backfill.CompensationFile)
	if path == "" {
		return
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return
	}
	var items []compensationItem
	if err := json.Unmarshal(data, &items); err != nil {
		logging.Warn(context.Background(), logging.EventIntegrityBackfillSkipped, "failed to decode compensation file", logging.Fields{
			"role_id": e.roleID,
			"path":    path,
			"error":   err.Error(),
		})
		return
	}
	e.compQueue = items
}

func (e *SequenceEngine) persistCompensationsLocked() {
	path := strings.TrimSpace(e.cfg.Backfill.CompensationFile)
	if path == "" {
		return
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return
	}
	payload, err := json.MarshalIndent(e.compQueue, "", "  ")
	if err != nil {
		return
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, payload, 0o644); err != nil {
		return
	}
	_ = os.Rename(tmp, path)
}

func compensationKey(cmd types.BackfillCmd) string {
	if cmd.Key != "" {
		return cmd.Key
	}
	return fmt.Sprintf("%s|%s|%s|%d|%d", cmd.RoleID, cmd.Type, cmd.StreamKey, cmd.Start, cmd.End)
}

func (e *SequenceEngine) deliver(events []*Event) Decision {
	if len(events) == 0 {
		return Decision{}
	}
	var out []*types.Message
	for _, evt := range events {
		if evt == nil || evt.Message == nil {
			continue
		}
		if e.dedupe != nil && e.dedupe.ShouldDrop(evt) {
			// 幂等器判定重复，直接跳过后续流程
			continue
		}
		// Gate 作为控制面，判断是否应该下发
		if e.gate.ShouldPass(evt) {
			out = append(out, evt.Message)
			// 通知 Gate 消息已下发，更新内部状态
			e.gate.OnDelivered(evt)
		}
	}
	return Decision{Deliver: out}
}

func isSnapshotEvent(evt *Event) bool {
	if evt == nil || evt.Message == nil || evt.Message.Metadata == nil {
		return false
	}
	if snapshot, ok := evt.Message.Metadata["snapshot"].(bool); ok && snapshot {
		return true
	}
	return false
}

func isBackpressureEvent(evt *Event) bool {
	if evt == nil || evt.Message == nil || evt.Message.Metadata == nil {
		return false
	}
	if backpressure, ok := evt.Message.Metadata["ws_backpressure"].(bool); ok {
		return backpressure
	}
	return false
}

func (e *SequenceEngine) buildBackfillAttempts(kind string, start, end int64, reason string) []types.BackfillAttempt {
	if len(e.cfg.Backfill.Options) == 0 {
		return nil
	}
	attempts := make([]types.BackfillAttempt, 0, len(e.cfg.Backfill.Options))
	for _, opt := range e.cfg.Backfill.Options {
		requests := buildRequestsForOption(opt, kind, start, end, e, reason)
		if len(requests) == 0 {
			continue
		}
		name := strings.ToLower(opt.Transport)
		if name == "" {
			name = "default"
		}
		attempts = append(attempts, types.BackfillAttempt{
			Name:     name,
			Requests: requests,
		})
	}
	return attempts
}

func buildRequestsForOption(opt types.BackfillOption, kind string, start, end int64, engine *SequenceEngine, reason string) []types.BackfillRequest {
	switch kind {
	case types.BackfillTypeSnapshot:
		return buildSnapshotRequests(opt, engine, reason)
	case types.BackfillTypeRange:
		return buildRangeRequests(opt, start, end, engine)
	default:
		return nil
	}
}

func buildRangeRequests(opt types.BackfillOption, start, end int64, engine *SequenceEngine) []types.BackfillRequest {
	if end < start {
		return nil
	}
	transport := strings.ToLower(opt.Transport)
	method := opt.RPCMethod
	if method == "" {
		method = util.GetString(opt.Params, "method", "")
	}
	if method == "" {
		method = "eth_getBlockByNumber"
	}
	includeFullTx := util.GetBool(opt.Params, "include_full_tx", false)
	jsonrpc := util.GetString(opt.Params, "jsonrpc", "2.0")
	transportOverride := strings.ToLower(util.GetString(opt.Params, "transport", ""))
	endpoint := util.GetString(opt.Params, "endpoint", "")
	chainID := util.GetString(opt.Params, "chain_id", "")
	source := util.GetString(opt.Params, "source", "")
	useRangeWindow := util.GetBool(opt.Params, "range_window", false)
	if useRangeWindow && (transport == types.BackfillTransportHTTP || transportOverride == "rest" || isHTTPMethod(method)) {
		return buildRangeWindowRequests(opt, start, end, engine, transport, method, transportOverride, endpoint, chainID, source, jsonrpc, includeFullTx)
	}

	requests := make([]types.BackfillRequest, 0, int(end-start+1))
	for blk := start; blk <= end; blk++ {
		params := []any{fmt.Sprintf("0x%x", blk)}
		if strings.EqualFold(method, "eth_getBlockByNumber") {
			params = append(params, includeFullTx)
		}

		metadata := map[string]any{
			"is_backfill":   true,
			"backfill_type": types.BackfillTypeRange,
			"block_query":   blk,
		}
		if includeFullTx {
			metadata["include_full_tx"] = true
		}
		if chainID != "" {
			metadata["chain_id"] = chainID
		}
		if source != "" {
			metadata["source"] = source
		}
		if opt.Params != nil {
			if extraMeta, ok := opt.Params["metadata"]; ok {
				if metaMap, ok := extraMeta.(map[string]any); ok {
					for k, v := range metaMap {
						metadata[k] = v
					}
				}
			}
		}
		if engine != nil && engine.cfg.Backfill.Cooldown > 0 {
			metadata["backfill_cooldown_ms"] = engine.cfg.Backfill.Cooldown.Milliseconds()
			metadata["backfill_max_failures"] = engine.cfg.Backfill.MaxFailures
			metadata["backfill_exhaust_cooldown_ms"] = engine.cfg.Backfill.ExhaustCooldown.Milliseconds()
			metadata["backfill_retry_backoff_ms"] = engine.cfg.Backfill.RetryBackoff.Milliseconds()
		}

		args := map[string]any{
			"method":   method,
			"params":   params,
			"jsonrpc":  jsonrpc,
			"metadata": metadata,
		}
		if endpoint != "" {
			args["url"] = endpoint
		}
		if chainID != "" {
			args["chain_id"] = chainID
		}
		if opt.Params != nil {
			if headers, ok := opt.Params["headers"]; ok {
				args["headers"] = headers
			}
			if query, ok := opt.Params["query"]; ok {
				args["query"] = query
			}
			if body, ok := opt.Params["body"]; ok {
				args["body"] = body
			}
		}

		switch {
		case transportOverride != "":
			args["transport"] = transportOverride
		case transport == types.BackfillTransportHTTP:
			if isHTTPMethod(method) {
				args["transport"] = "rest"
			} else {
				args["transport"] = "rpc"
			}
		case transport == types.BackfillTransportWebSocket:
			args["transport"] = "rpc"
			args["rpc"] = true
		default:
			if transport != "" {
				args["transport"] = transport
			}
		}

		requests = append(requests, types.BackfillRequest{
			Transport: transport,
			Args:      args,
		})
	}
	return requests
}

func buildRangeWindowRequests(
	opt types.BackfillOption,
	start, end int64,
	engine *SequenceEngine,
	transport, method, transportOverride, endpoint, chainID, source, jsonrpc string,
	includeFullTx bool,
) []types.BackfillRequest {
	if end < start {
		return nil
	}
	metadata := map[string]any{
		"is_backfill":   true,
		"backfill_type": types.BackfillTypeRange,
		"range_start":   start,
		"range_end":     end,
	}
	if includeFullTx {
		metadata["include_full_tx"] = true
	}
	if chainID != "" {
		metadata["chain_id"] = chainID
	}
	if source != "" {
		metadata["source"] = source
	}
	if opt.Params != nil {
		if extraMeta, ok := opt.Params["metadata"]; ok {
			if metaMap, ok := extraMeta.(map[string]any); ok {
				for k, v := range metaMap {
					metadata[k] = v
				}
			}
		}
	}
	if engine != nil && engine.cfg.Backfill.Cooldown > 0 {
		metadata["backfill_cooldown_ms"] = engine.cfg.Backfill.Cooldown.Milliseconds()
		metadata["backfill_max_failures"] = engine.cfg.Backfill.MaxFailures
		metadata["backfill_exhaust_cooldown_ms"] = engine.cfg.Backfill.ExhaustCooldown.Milliseconds()
		metadata["backfill_retry_backoff_ms"] = engine.cfg.Backfill.RetryBackoff.Milliseconds()
	}

	args := map[string]any{
		"method":   method,
		"jsonrpc":  jsonrpc,
		"metadata": metadata,
	}
	if strings.EqualFold(method, "eth_getBlockByNumber") {
		args["params"] = []any{fmt.Sprintf("0x%x", start), includeFullTx}
	}
	if endpoint != "" {
		args["url"] = endpoint
	}
	if chainID != "" {
		args["chain_id"] = chainID
	}
	if opt.Params != nil {
		if headers, ok := opt.Params["headers"]; ok {
			args["headers"] = headers
		}
		if body, ok := opt.Params["body"]; ok {
			args["body"] = body
		}
	}

	query := cloneStringAnyMap(toMapStringAny(opt.Params["query"]))
	startKey := util.GetString(opt.Params, "range_start_param", "fromId")
	if startKey != "" {
		query[startKey] = start
	}
	endKey := util.GetString(opt.Params, "range_end_param", "")
	if endKey != "" {
		query[endKey] = end
	}
	limitKey := util.GetString(opt.Params, "range_limit_param", "limit")
	if limitKey != "" {
		count := end - start + 1
		if maxLimit := util.GetInt(opt.Params, "range_max_limit", 0); maxLimit > 0 && count > int64(maxLimit) {
			count = int64(maxLimit)
		}
		if count > 0 {
			query[limitKey] = count
		}
	}
	if len(query) > 0 {
		args["query"] = query
	}

	switch {
	case transportOverride != "":
		args["transport"] = transportOverride
	case transport == types.BackfillTransportHTTP:
		args["transport"] = "rest"
	case transport == types.BackfillTransportWebSocket:
		args["transport"] = "rpc"
		args["rpc"] = true
	default:
		if isHTTPMethod(method) {
			args["transport"] = "rest"
		} else if transport != "" {
			args["transport"] = transport
		}
	}

	return []types.BackfillRequest{{
		Transport: transport,
		Args:      args,
	}}
}

func buildSnapshotRequests(opt types.BackfillOption, engine *SequenceEngine, reason string) []types.BackfillRequest {
	transport := strings.ToLower(opt.Transport)
	snapshotReason := normalizeSnapshotReason(reason)
	metadata := map[string]any{
		"is_backfill":     true,
		"backfill_type":   types.BackfillTypeSnapshot,
		"snapshot":        true,
		"snapshot_source": "backfill",
		"snapshot_reason": snapshotReason,
	}
	if opt.Params != nil {
		if extraMeta, ok := opt.Params["metadata"]; ok {
			if metaMap, ok := extraMeta.(map[string]any); ok {
				for k, v := range metaMap {
					metadata[k] = v
				}
			}
		}
	}
	if engine != nil && engine.cfg.Backfill.Cooldown > 0 {
		metadata["backfill_cooldown_ms"] = engine.cfg.Backfill.Cooldown.Milliseconds()
		metadata["backfill_max_failures"] = engine.cfg.Backfill.MaxFailures
		metadata["backfill_exhaust_cooldown_ms"] = engine.cfg.Backfill.ExhaustCooldown.Milliseconds()
		metadata["backfill_retry_backoff_ms"] = engine.cfg.Backfill.RetryBackoff.Milliseconds()
	}

	args := map[string]any{
		"metadata": metadata,
	}
	if endpoint := util.GetString(opt.Params, "endpoint", ""); endpoint != "" {
		args["url"] = endpoint
	}
	if chainID := util.GetString(opt.Params, "chain_id", ""); chainID != "" {
		args["chain_id"] = chainID
		metadata["chain_id"] = chainID
	}
	if source := util.GetString(opt.Params, "source", ""); source != "" {
		metadata["source"] = source
	}

	method := util.GetString(opt.Params, "method", "")
	if method != "" {
		if isHTTPMethod(method) {
			args["method"] = strings.ToUpper(method)
		} else {
			args["method"] = method
		}
	}
	if opt.Params != nil {
		if headers, ok := opt.Params["headers"]; ok {
			args["headers"] = headers
		}
		if query, ok := opt.Params["query"]; ok {
			args["query"] = query
		}
		if body, ok := opt.Params["body"]; ok {
			args["body"] = body
		}
	}

	transportOverride := strings.ToLower(util.GetString(opt.Params, "transport", ""))
	switch {
	case transportOverride != "":
		args["transport"] = transportOverride
	case method != "" && isHTTPMethod(method):
		args["transport"] = "rest"
	case transport == types.BackfillTransportWebSocket:
		args["transport"] = "rpc"
		args["rpc"] = true
	case transport == types.BackfillTransportHTTP:
		if method != "" && !isHTTPMethod(method) {
			args["transport"] = "rpc"
		} else {
			args["transport"] = "rest"
		}
	default:
		if transport != "" {
			args["transport"] = transport
		}
	}

	return []types.BackfillRequest{{
		Transport: transport,
		Args:      args,
	}}
}

func isHTTPMethod(method string) bool {
	switch strings.ToUpper(method) {
	case "GET", "POST", "PUT", "DELETE", "PATCH", "HEAD":
		return true
	default:
		return false
	}
}

func toMapStringAny(raw any) map[string]any {
	switch v := raw.(type) {
	case map[string]any:
		return v
	case map[any]any:
		out := make(map[string]any, len(v))
		for k, val := range v {
			out[fmt.Sprintf("%v", k)] = val
		}
		return out
	default:
		return nil
	}
}

func cloneStringAnyMap(src map[string]any) map[string]any {
	if len(src) == 0 {
		return map[string]any{}
	}
	dst := make(map[string]any, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func normalizeSnapshotReason(reason string) string {
	normalized := strings.ToLower(strings.TrimSpace(reason))
	if normalized == "" {
		return "gap"
	}
	return normalized
}

type backfillRecord struct {
	start uint64
	end   uint64
	at    time.Time
}

func minUint64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}
