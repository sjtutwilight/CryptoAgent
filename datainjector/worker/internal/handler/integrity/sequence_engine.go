package integrity

import (
	"fmt"
	"log"
	"math"
	"strings"
	"time"

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
}

type engineState struct {
	ExpectedNext     uint64         // 当前期望的序列号
	SeenMax          uint64         // 已见到的最大序列
	Initialized      bool           // 是否已初始化
	AwaitingSnapshot bool           // 是否等待快照确认
	WaitStart        time.Time      // 当前等待窗口的起点时间
	LastSweep        time.Time      // 上次清理时间
	LastBackfill     backfillRecord // 最近一次补数记录
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
	}
}

func (e *SequenceEngine) Handle(evt *Event) Decision {
	if evt == nil || evt.Message == nil {
		return Decision{}
	}
	if evt.Arrival.IsZero() {
		evt.Arrival = time.Now()
	}
	isSnapshot := isSnapshotEvent(evt)

	if !e.state.Initialized {
		// 首条消息直接作为初始 expected，避免空补数
		e.bootstrap(evt)
		if isSnapshot {
			e.state.AwaitingSnapshot = true
		}
		return e.deliver([]*Event{evt})
	}

	if isSnapshot {
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
	e.state.ExpectedNext = evt.Seq
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
	if e.cfg.Sequence.EagerGap > 0 && gap > e.cfg.Sequence.EagerGap {
		end := evt.Seq - 1
		if e.cfg.Sequence.MaxRange > 0 && end-e.state.ExpectedNext+1 > e.cfg.Sequence.MaxRange {
			end = e.state.ExpectedNext + e.cfg.Sequence.MaxRange - 1
		}
		e.triggerBackfill(e.state.ExpectedNext, end, evt.Arrival)
	}
	if !e.checkTimeout(evt.Arrival) {
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

func (e *SequenceEngine) checkTimeout(now time.Time) bool {
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
		e.triggerBackfill(e.state.ExpectedNext, end, now)
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

func (e *SequenceEngine) checkBudget(now time.Time) {
	if e.cfg.Sequence.MaxGap == 0 || e.state.SeenMax <= e.state.ExpectedNext {
		return
	}
	if diff := e.state.SeenMax - e.state.ExpectedNext; diff > e.cfg.Sequence.MaxGap {
		target := e.state.SeenMax - e.cfg.Sequence.MaxGap
		e.advance(target, now, "max-gap")
	}
}

func (e *SequenceEngine) triggerBackfill(start, end uint64, now time.Time) {
	if e.backfill == nil || len(e.cfg.Backfill.Options) == 0 {
		return
	}
	if end < start {
		return
	}

	// 冷却检查：相同范围且在冷却期内，跳过
	if e.state.LastBackfill.start == start && e.state.LastBackfill.end == end &&
		now.Sub(e.state.LastBackfill.at) < e.cfg.Backfill.Cooldown {
		return
	}

	var cmd types.BackfillCmd
	var attempts []types.BackfillAttempt
	if e.cfg.Backfill.SnapshotBased {
		// 快照模式：如 Binance 订单簿，触发全量快照请求
		log.Printf("[integrity] stream=%s trigger SNAPSHOT backfill (gap %d->%d)", e.streamName, start, end)
		attempts = e.buildBackfillAttempts(types.BackfillTypeSnapshot, 0, 0)
		cmd = types.BackfillCmd{
			Type:     types.BackfillTypeSnapshot,
			Start:    0,
			End:      0,
			Attempts: attempts,
		}
	} else {
		// 范围模式：如区块链数据，请求 [start, end] 范围
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
			return
		}
		startInt := int64(start)
		endInt := int64(end)
		log.Printf("[integrity] stream=%s trigger RANGE backfill [%d, %d]", e.streamName, startInt, endInt)
		attempts = e.buildBackfillAttempts(types.BackfillTypeRange, startInt, endInt)
		cmd = types.BackfillCmd{
			Type:     types.BackfillTypeRange,
			Start:    startInt,
			End:      endInt,
			Attempts: attempts,
		}
	}

	if len(cmd.Attempts) == 0 {
		log.Printf("[integrity] stream=%s backfill request skipped: no executable attempts", e.streamName)
		return
	}

	if e.backfill.Schedule(cmd) {
		e.state.LastBackfill = backfillRecord{start: start, end: end, at: now}
		if e.cfg.Backfill.SnapshotBased {
			e.state.AwaitingSnapshot = true
		}
	}
}

func (e *SequenceEngine) advance(target uint64, now time.Time, reason string) {
	if target <= e.state.ExpectedNext {
		return
	}
	if target > 0 {
		e.buffer.cleanup(target - 1)
	}
	log.Printf("[integrity] stream=%s advance expected %d -> %d (%s)", e.streamName, e.state.ExpectedNext, target, reason)
	e.state.ExpectedNext = target
	e.state.WaitStart = now
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
		e.triggerBackfill(seq, seq, now)
	}
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

func (e *SequenceEngine) buildBackfillAttempts(kind string, start, end int64) []types.BackfillAttempt {
	if len(e.cfg.Backfill.Options) == 0 {
		return nil
	}
	attempts := make([]types.BackfillAttempt, 0, len(e.cfg.Backfill.Options))
	for _, opt := range e.cfg.Backfill.Options {
		requests := buildRequestsForOption(opt, kind, start, end, e)
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

func buildRequestsForOption(opt types.BackfillOption, kind string, start, end int64, engine *SequenceEngine) []types.BackfillRequest {
	switch kind {
	case types.BackfillTypeSnapshot:
		return buildSnapshotRequests(opt, engine)
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
		method = "eth_getBlockByNumber"
	}
	includeFullTx := util.GetBool(opt.Params, "include_full_tx", false)
	jsonrpc := util.GetString(opt.Params, "jsonrpc", "2.0")
	transportOverride := strings.ToLower(util.GetString(opt.Params, "transport", ""))
	endpoint := util.GetString(opt.Params, "endpoint", "")
	chainID := util.GetString(opt.Params, "chain_id", "")
	source := util.GetString(opt.Params, "source", "")

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

func buildSnapshotRequests(opt types.BackfillOption, engine *SequenceEngine) []types.BackfillRequest {
	transport := strings.ToLower(opt.Transport)
	metadata := map[string]any{
		"is_backfill":   true,
		"backfill_type": types.BackfillTypeSnapshot,
		"snapshot":      true,
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
