package integrity

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/observability/logging"
	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/observability/metrics"
	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/types"
)

type BackfillOrchestrator struct {
	sessionMu sync.Mutex
	sessions  map[string]*backfillSession
}

func newBackfillOrchestrator() *BackfillOrchestrator {
	return &BackfillOrchestrator{
		sessions: make(map[string]*backfillSession),
	}
}

func (o *BackfillOrchestrator) triggerWithSession(e *SequenceEngine, start, end uint64, now time.Time, reason string) bool {
	kind := e.backfillType()
	key := types.BackfillSessionKey(e.roleID, e.streamName, kind)
	session := o.getOrCreateSession(key, kind, e.roleID, e.streamName)

	o.sessionMu.Lock()
	defer o.sessionMu.Unlock()
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
		prevState := session.State
		session.State = sessionIdle
		session.CooldownUntil = time.Time{}
		e.logSessionTransition(session, prevState, session.State, "cooldown_elapsed", now)
	}
	if session.State == sessionPending {
		o.mergeSessionIntent(session, start, end)
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
	prevState := session.State
	session.State = sessionPending
	session.Attempt = attempt
	session.SessionID = cmd.SessionID
	session.CmdID = cmd.CmdID
	session.PendingSince = now
	session.HasIntent = false
	metrics.SetBackfillSessionsInflight(e.roleID, e.streamName, kind, 1)
	e.logSessionTransition(session, prevState, session.State, "schedule_success", now)
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

func (o *BackfillOrchestrator) onBackfillResult(e *SequenceEngine, result types.BackfillResult) {
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

	o.sessionMu.Lock()
	session, ok := o.sessions[result.Key]
	if !ok {
		o.sessionMu.Unlock()
		return
	}
	if session.State != sessionPending {
		o.sessionMu.Unlock()
		return
	}
	if result.SessionID != "" && session.SessionID != "" && result.SessionID != session.SessionID {
		o.sessionMu.Unlock()
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

	prevState := session.State
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
	e.logSessionTransition(session, prevState, session.State, status, result.FinishedAt)

	if session.State == sessionIdle && session.HasIntent {
		triggerNext = true
		nextStart = session.IntentStart
		nextEnd = session.IntentEnd
		session.HasIntent = false
	}
	o.sessionMu.Unlock()

	metrics.RecordBackfillResult(result.RoleID, result.Type, status, result.ErrorClass)
	if pendingFor > 0 {
		metrics.RecordBackfillPendingDuration(result.RoleID, result.StreamKey, result.Type, status, pendingFor)
	}
	if triggerNext {
		e.triggerBackfillWithReason(nextStart, nextEnd, result.FinishedAt, "session_intent")
	}
}

func (o *BackfillOrchestrator) resolvePendingTimeout(e *SequenceEngine, now time.Time) {
	if !e.cfg.Backfill.ResultDrivenEnabled {
		return
	}
	timeout := e.cfg.Sequence.HardTimeout
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	expired := make([]types.BackfillResult, 0)
	o.sessionMu.Lock()
	for _, session := range o.sessions {
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
	o.sessionMu.Unlock()

	for _, result := range expired {
		o.onBackfillResult(e, result)
	}
}

func (o *BackfillOrchestrator) getOrCreateSession(key, kind, roleID, streamName string) *backfillSession {
	o.sessionMu.Lock()
	defer o.sessionMu.Unlock()
	if s, ok := o.sessions[key]; ok {
		return s
	}
	s := &backfillSession{
		Key:       key,
		Type:      kind,
		RoleID:    roleID,
		StreamKey: streamName,
		State:     sessionIdle,
	}
	o.sessions[key] = s
	return s
}

func (o *BackfillOrchestrator) mergeSessionIntent(session *backfillSession, start, end uint64) {
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

func (o *BackfillOrchestrator) isPending(cmd types.BackfillCmd) bool {
	if o == nil {
		return false
	}
	key := cmd.Key
	if key == "" {
		key = types.BackfillSessionKey(cmd.RoleID, cmd.StreamKey, cmd.Type)
	}
	if key == "" {
		return false
	}
	o.sessionMu.Lock()
	defer o.sessionMu.Unlock()
	session, ok := o.sessions[key]
	return ok && session != nil && session.State == sessionPending
}
