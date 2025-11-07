package role

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/caller"
	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/config"
	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/emitter"
	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/handler"
	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/queue"
	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/sink"
	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/status"
	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/types"
)

type Role struct {
	ID             string
	emitterType    string // "polling" | "single"
	pollingEmitter *emitter.Polling
	singleEmitter  *emitter.Single
	kafkaEmitter   *emitter.KafkaCommand
	caller         caller.Caller
	httpCaller     *caller.HTTPCall
	q              *queue.BoundedQueue[*types.Message]
	handlers       []handler.Handler
	sink           sink.Sink
	closers        []io.Closer
	backfillCh     chan types.BackfillCmd
	statusReporter status.Reporter
	lastSnapshotAt time.Time
	backfillMu     sync.Mutex
}

func Build(rc config.RoleConfig) (*Role, error) {
	// 构造传入 caller 的参数：native_call 需要区分 caller_config / caller_params，其他 caller 直接使用 CallerParams
	var paramsForCaller map[string]any
	switch rc.Caller {
	case "native_call":
		cfg := map[string]any{}
		if rc.CallerConfig != nil {
			cfg["caller_config"] = rc.CallerConfig
		}
		if rc.CallerParams != nil {
			cfg["caller_params"] = rc.CallerParams
		}
		paramsForCaller = cfg
	default:
		if rc.CallerParams != nil {
			paramsForCaller = rc.CallerParams
		} else {
			paramsForCaller = map[string]any{}
		}
	}

	// 3. 创建 caller
	cl, err := caller.New(rc.Caller, rc.CallerClass, paramsForCaller)
	if err != nil {
		return nil, err
	}
	handlers := make([]handler.Handler, 0, len(rc.Handlers))
	backfillAware := make([]handler.BackfillCommandAware, 0)
	var closers []io.Closer
	if len(rc.Handlers) == 0 {
		h, _ := handler.New("noop", nil)
		handlers = append(handlers, h)
		if closer, ok := h.(io.Closer); ok {
			closers = append(closers, closer)
		}
	} else {
		for _, hc := range rc.Handlers {
			h, err := handler.New(hc.Type, hc.With)
			if err != nil {
				return nil, err
			}
			handlers = append(handlers, h)
			if closer, ok := h.(io.Closer); ok {
				closers = append(closers, closer)
			}
			if aware, ok := h.(handler.BackfillCommandAware); ok {
				backfillAware = append(backfillAware, aware)
			}
		}
	}

	var snapshotListeners []handler.SnapshotListener
	for _, h := range handlers {
		if listener, ok := h.(handler.SnapshotListener); ok {
			snapshotListeners = append(snapshotListeners, listener)
		}
		if aware, ok := h.(handler.SnapshotListenerAware); ok {
			for _, listener := range snapshotListeners {
				aware.SetSnapshotListener(listener)
			}
		}
	}

	sk, err := sink.New(rc.Sink.Type, rc.Sink.With)
	if err != nil {
		return nil, err
	}

	r := &Role{
		ID:             rc.RoleID,
		emitterType:    rc.Emitter,
		caller:         cl,
		q:              queue.NewBounded[*types.Message](rc.Queue.Size),
		handlers:       handlers,
		sink:           sk,
		closers:        closers,
		statusReporter: status.Get(),
	}

	if len(backfillAware) > 0 {
		ch := make(chan types.BackfillCmd, 256) // 增加缓冲容量：16 -> 256
		r.backfillCh = ch
		for _, aware := range backfillAware {
			aware.SetBackfillChannel(ch)
		}
	}

	// 根据emitter类型初始化对应的emitter
	switch rc.Emitter {
	case "polling":
		r.pollingEmitter = &emitter.Polling{Interval: rc.PollingDuration()}
	case "single":
		// single emitter 将订阅参数传递给 caller，同时支持自定义轮询间隔
		var pollInterval time.Duration = time.Second
		paramsCopy := map[string]any{}
		if rc.CallerParams != nil {
			for k, v := range rc.CallerParams {
				if k == "poll_interval_ms" {
					switch vv := v.(type) {
					case int:
						if vv > 0 {
							pollInterval = time.Duration(vv) * time.Millisecond
						}
					case int64:
						if vv > 0 {
							pollInterval = time.Duration(vv) * time.Millisecond
						}
					case float64:
						if vv > 0 {
							pollInterval = time.Duration(vv) * time.Millisecond
						}
					default:
					}
					continue
				}
				paramsCopy[k] = v
			}
		}
		r.singleEmitter = &emitter.Single{Params: paramsCopy, PollInterval: pollInterval}
	case "kafka_command":
		cfg := emitter.KafkaCommandConfig{}
		if rc.EmitterConfig != nil {
			cfg.Brokers = toStringSlice(rc.EmitterConfig["brokers"])
			if v, ok := rc.EmitterConfig["topic"].(string); ok {
				cfg.Topic = v
			}
			if v, ok := rc.EmitterConfig["group_id"].(string); ok {
				cfg.GroupID = v
			}
			cfg.MinBytes = toInt(rc.EmitterConfig["min_bytes"], 0)
			cfg.MaxBytes = toInt(rc.EmitterConfig["max_bytes"], 0)
		}
		kEmitter, err := emitter.NewKafkaCommand(cfg)
		if err != nil {
			return nil, err
		}
		r.kafkaEmitter = kEmitter
	}

	return r, nil
}

func extractTaskID(args map[string]any) string {
	if args == nil {
		return ""
	}
	if v, ok := args["taskId"]; ok {
		return stringify(v)
	}
	if v, ok := args["task_id"]; ok {
		return stringify(v)
	}
	if meta, ok := args["metadata"].(map[string]any); ok {
		if v, ok := meta["taskId"]; ok {
			return stringify(v)
		}
		if v, ok := meta["task_id"]; ok {
			return stringify(v)
		}
	}
	return ""
}

func (r *Role) reportFailure(ctx context.Context, taskID string, err error, duration time.Duration, retryable bool, statusCode int, reason string) {
	if r.statusReporter == nil || taskID == "" {
		return
	}
	evt := status.Event{
		TaskID:     taskID,
		Status:     "FAILED",
		StatusCode: statusCode,
		Message:    buildStatusMessage(reason, err),
		Retryable:  retryable,
		DurationMs: duration.Milliseconds(),
		Timestamp:  time.Now().UTC(),
	}
	if !retryable {
		evt.Status = "FAILED"
	} else {
		evt.Status = "RETRY"
	}
	if err := r.statusReporter.Report(ctx, evt); err != nil {
		log.Printf("role %s: status report failure: %v", r.ID, err)
	}
}

func (r *Role) reportSuccess(ctx context.Context, taskID string, duration time.Duration) {
	if r.statusReporter == nil || taskID == "" {
		return
	}
	evt := status.Event{
		TaskID:     taskID,
		Status:     "SUCCESS",
		StatusCode: 200,
		DurationMs: duration.Milliseconds(),
		Retryable:  false,
		Timestamp:  time.Now().UTC(),
	}
	if err := r.statusReporter.Report(ctx, evt); err != nil {
		log.Printf("role %s: status report success failed: %v", r.ID, err)
	}
}

func buildStatusMessage(reason string, err error) string {
	if err == nil {
		return reason
	}
	if reason == "" {
		return err.Error()
	}
	return fmt.Sprintf("%s: %v", reason, err)
}

func stringify(v interface{}) string {
	switch vv := v.(type) {
	case string:
		return vv
	case fmt.Stringer:
		return vv.String()
	case []byte:
		return string(vv)
	default:
		return fmt.Sprintf("%v", vv)
	}
}

func toStringSlice(v interface{}) []string {
	if v == nil {
		return nil
	}
	switch vv := v.(type) {
	case []string:
		return vv
	case []any:
		res := make([]string, 0, len(vv))
		for _, item := range vv {
			if s, ok := item.(string); ok {
				res = append(res, s)
			}
		}
		return res
	default:
		return nil
	}
}

func toInt(v interface{}, def int) int {
	switch vv := v.(type) {
	case int:
		return vv
	case int64:
		return int(vv)
	case float64:
		return int(vv)
	case string:
		if i, err := strconv.Atoi(vv); err == nil {
			return i
		}
	default:
	}
	return def
}

func getStringValue(m map[string]any, key, def string) string {
	if m == nil {
		return def
	}
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return def
}

func (r *Role) Start(ctx context.Context) error {
	if closer, ok := r.sink.(io.Closer); ok {
		defer closer.Close()
	}
	for _, h := range r.closers {
		defer h.Close()
	}
	// 如果caller实现了io.Closer，也需要在退出时关闭
	if closer, ok := r.caller.(io.Closer); ok {
		defer closer.Close()
	}

	// 消费者
	go r.consume(ctx)
	if r.backfillCh != nil {
		go r.runBackfill(ctx)
	}

	// 触发器：每次触发调用caller，并把返回消息入队
	fireFunc := func(args map[string]any) {
		taskID := extractTaskID(args)
		start := time.Now()

		// 调用 caller
		msgs, err := r.caller.CallOnce(ctx, args)
		if err != nil {
			retryable, statusCode := true, 0
			var callErr *caller.CallError
			if errors.As(err, &callErr) {
				retryable = callErr.Retryable
				statusCode = callErr.StatusCode
			}
			r.reportFailure(ctx, taskID, err, time.Since(start), retryable, statusCode, "caller error")
			log.Printf("role %s: caller error: %v", r.ID, err)
			return
		}
		// 消息入队
		for _, m := range msgs {
			if m == nil {
				continue
			}
			if err := r.q.Enqueue(ctx, m); err != nil {
				log.Printf("role %s: enqueue error: %v", r.ID, err)
				r.reportFailure(ctx, taskID, err, time.Since(start), true, 0, "enqueue error")
				return
			}
		}

		r.reportSuccess(ctx, taskID, time.Since(start))
	}

	// 根据emitter类型启动
	switch r.emitterType {
	case "polling":
		return r.pollingEmitter.Start(ctx, fireFunc)
	case "single":
		return r.singleEmitter.Start(ctx, fireFunc)
	case "kafka_command":
		return r.kafkaEmitter.Start(ctx, fireFunc)
	default:
		return nil
	}
}

func (r *Role) runBackfill(ctx context.Context) {
	const numWorkers = 1
	for i := 0; i < numWorkers; i++ {
		go func(workerID int) {
			for {
				select {
				case <-ctx.Done():
					return
				case cmd := <-r.backfillCh:
					r.handleBackfillCmd(ctx, workerID, cmd)
				}
			}
		}(i)
	}
	<-ctx.Done()
}

func (r *Role) handleBackfillCmd(ctx context.Context, workerID int, cmd types.BackfillCmd) {
	if len(cmd.Attempts) == 0 {
		log.Printf("role %s worker-%d: backfill %s skipped (no attempts)", r.ID, workerID, cmd.Type)
		return
	}
	for _, attempt := range cmd.Attempts {
		if r.executeBackfillAttempt(ctx, workerID, cmd, attempt) {
			switch cmd.Type {
			case types.BackfillTypeSnapshot:
				log.Printf("role %s worker-%d: snapshot backfill succeeded via %s", r.ID, workerID, attempt.Name)
			case types.BackfillTypeRange:
				log.Printf("role %s worker-%d: range backfill [%d, %d] succeeded via %s", r.ID, workerID, cmd.Start, cmd.End, attempt.Name)
			default:
				log.Printf("role %s worker-%d: backfill %s succeeded via %s", r.ID, workerID, cmd.Type, attempt.Name)
			}
			return
		}
		log.Printf("role %s worker-%d: backfill attempt %s failed for %s", r.ID, workerID, attempt.Name, cmd.Type)
	}
	log.Printf("role %s worker-%d: backfill %s exhausted (start=%d end=%d)", r.ID, workerID, cmd.Type, cmd.Start, cmd.End)
}

func (r *Role) executeBackfillAttempt(ctx context.Context, workerID int, cmd types.BackfillCmd, attempt types.BackfillAttempt) bool {
	if len(attempt.Requests) == 0 {
		return false
	}
	var cooldown time.Duration
	if meta := extractMetadata(attempt.Requests[0].Args); meta != nil {
		if v, ok := meta["backfill_cooldown_ms"]; ok {
			switch vv := v.(type) {
			case int64:
				cooldown = time.Duration(vv) * time.Millisecond
			case int:
				cooldown = time.Duration(vv) * time.Millisecond
			case float64:
				cooldown = time.Duration(int64(vv)) * time.Millisecond
			case string:
				if n, err := strconv.ParseInt(vv, 10, 64); err == nil {
					cooldown = time.Duration(n) * time.Millisecond
				}
			}
		}
	}
	if cmd.Type == types.BackfillTypeSnapshot && cooldown > 0 {
		for {
			select {
			case <-ctx.Done():
				return false
			default:
			}
			r.backfillMu.Lock()
			wait := time.Until(r.lastSnapshotAt.Add(cooldown))
			if wait <= 0 {
				r.lastSnapshotAt = time.Now()
				r.backfillMu.Unlock()
				break
			}
			r.backfillMu.Unlock()
			timer := time.NewTimer(wait)
			select {
			case <-ctx.Done():
				timer.Stop()
				return false
			case <-timer.C:
			}
		}
	}
	for idx, req := range attempt.Requests {
		select {
		case <-ctx.Done():
			return false
		default:
		}

		args := cloneArgs(req.Args)
		if args == nil {
			args = map[string]any{}
		}
		if _, ok := args["transport"]; !ok && req.Transport != "" {
			args["transport"] = req.Transport
		}

		meta := ensureMetadata(args)
		if _, ok := meta["is_backfill"]; !ok {
			meta["is_backfill"] = true
		}
		if _, ok := meta["backfill_type"]; !ok {
			meta["backfill_type"] = cmd.Type
		}
		if attempt.Name != "" {
			meta["backfill_attempt"] = attempt.Name
		}
		meta["backfill_step"] = idx + 1
		meta["backfill_steps"] = len(attempt.Requests)
		if cmd.Type == types.BackfillTypeSnapshot {
			meta["snapshot"] = true
		}
		args["metadata"] = meta

		msgs, err := r.executeBackfillRequest(ctx, strings.ToLower(req.Transport), args)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return false
			}
			var callErr *caller.CallError
			if errors.As(err, &callErr) {
				log.Printf("role %s worker-%d: attempt %s request %d failed (status=%d retryable=%v): %v", r.ID, workerID, attempt.Name, idx+1, callErr.StatusCode, callErr.Retryable, callErr.Err)
			} else {
				log.Printf("role %s worker-%d: attempt %s request %d failed: %v", r.ID, workerID, attempt.Name, idx+1, err)
			}
			return false
		}

		for _, msg := range msgs {
			if msg == nil {
				continue
			}
			if msg.Metadata == nil {
				msg.Metadata = map[string]any{}
			}
			if attempt.Name != "" {
				msg.Metadata["backfill_attempt"] = attempt.Name
			}
			if _, ok := msg.Metadata["backfill_type"]; !ok {
				msg.Metadata["backfill_type"] = cmd.Type
			}
			if cmd.Type == types.BackfillTypeSnapshot {
				msg.Metadata["snapshot"] = true
			}
			if err := r.q.Enqueue(ctx, msg); err != nil {
				log.Printf("role %s worker-%d: enqueue backfill message failed: %v", r.ID, workerID, err)
				return false
			}
		}
	}
	return true
}

func cloneArgs(src map[string]any) map[string]any {
	if len(src) == 0 {
		return map[string]any{}
	}
	dst := make(map[string]any, len(src))
	for k, v := range src {
		dst[k] = cloneValue(v)
	}
	return dst
}

func cloneValue(v interface{}) interface{} {
	switch vv := v.(type) {
	case map[string]any:
		return cloneArgs(vv)
	case map[interface{}]interface{}:
		converted := make(map[string]any, len(vv))
		for k, val := range vv {
			converted[fmt.Sprint(k)] = cloneValue(val)
		}
		return converted
	case []interface{}:
		out := make([]interface{}, len(vv))
		for i := range vv {
			out[i] = cloneValue(vv[i])
		}
		return out
	default:
		return vv
	}
}

func extractMetadata(args map[string]any) map[string]any {
	if args == nil {
		return nil
	}
	if meta, ok := args["metadata"].(map[string]any); ok {
		return meta
	}
	return nil
}

func ensureMetadata(args map[string]any) map[string]any {
	if args == nil {
		args = map[string]any{}
	}
	if meta, ok := args["metadata"].(map[string]any); ok && meta != nil {
		return meta
	}
	meta := map[string]any{}
	args["metadata"] = meta
	return meta
}

func (r *Role) executeBackfillRequest(ctx context.Context, transport string, args map[string]any) ([]*types.Message, error) {
	mode := strings.ToLower(transport)
	if mode == "" || mode == "default" {
		if raw, ok := args["transport"].(string); ok {
			mode = strings.ToLower(raw)
		}
	}
	switch mode {
	case "rest", "http", "https":
		if err := r.ensureHTTPCaller(); err != nil {
			return nil, err
		}
		if mode == types.BackfillTransportHTTP || mode == "http" || mode == "https" {
			// default to REST when not explicitly specified
			if _, ok := args["transport"].(string); !ok {
				args["transport"] = "rest"
			}
		}
		return r.httpCaller.CallOnce(ctx, args)
	case "websocket", "rpc":
		if args == nil {
			args = map[string]any{}
		}
		if _, ok := args["rpc"]; !ok {
			args["rpc"] = true
		}
		if _, ok := args["transport"].(string); !ok {
			args["transport"] = "rpc"
		}
		return r.caller.CallOnce(ctx, args)
	default:
		return r.caller.CallOnce(ctx, args)
	}
}

func (r *Role) ensureHTTPCaller() error {
	if r.httpCaller != nil {
		return nil
	}
	cfg := map[string]any{
		"timeout_ms": 5000,
		"rate_limit": map[string]any{
			"capacity":    1,
			"refill_rate": 0.066, // roughly one snapshot every 15s
		},
	}
	call, err := caller.NewHTTPCall(cfg, map[string]any{})
	if err != nil {
		return fmt.Errorf("create http backfill caller: %w", err)
	}
	r.httpCaller = call
	return nil
}

func (r *Role) consume(ctx context.Context) {
	for {
		msg, err := r.q.Dequeue(ctx)
		if err != nil {
			// 正常退出
			return
		}
		curMsgs := []*types.Message{msg}
		handlerErr := error(nil)

		for _, h := range r.handlers {
			next := make([]*types.Message, 0, len(curMsgs))
			for _, m := range curMsgs {
				if m == nil {
					continue
				}
				outs, err := h.Handle(m)
				if err != nil {
					handlerErr = err
					log.Printf("role %s: handler error: %v", r.ID, err)
					break
				}
				next = append(next, outs...)
			}
			if handlerErr != nil {
				curMsgs = nil
				break
			}
			curMsgs = next
			if len(curMsgs) == 0 {
				break
			}
		}
		if handlerErr != nil || len(curMsgs) == 0 {
			continue
		}
		wrote := false
		for _, out := range curMsgs {
			if out == nil {
				continue
			}
			if err := r.sink.Write(out); err != nil {
				log.Printf("role %s: sink error: %v", r.ID, err)
			}
			wrote = true
		}
		if !wrote {
			continue
		}
		// 小休以避免日志刷屏（示例）
		select {
		case <-ctx.Done():
			return
		case <-time.After(10 * time.Millisecond):
		}
	}
}
