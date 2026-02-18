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
	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/observability/logging"
	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/observability/metrics"
	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/observability/status"
	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/observability/tracing"
	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/queue"
	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/sink"
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
	pipelineMode   string
	queueMode      string
	q              *queue.BoundedQueue[*types.Message]
	handlers       []handler.Handler
	sink           sink.Sink
	closers        []io.Closer
	backfillCh     chan types.BackfillCmd
	backfillDiffCh chan types.BackfillCmd
	backfillSnapCh chan types.BackfillCmd
	backfillResult []handler.BackfillResultAware
	statusReporter status.Reporter
	lastSnapshotAt time.Time
	backfillMu     sync.Mutex
	backfillStates map[string]backfillState
	taskMu         sync.Mutex
	tasks          map[string]*queueTaskState
	queueRetries   int
	taskTTL        time.Duration
	maxTrackedTask int
	strictFinalize bool
	dlqMu          sync.Mutex
	dlqBuffer      []*types.Message
}

type backfillState struct {
	ConsecutiveFailures int
	CooldownUntil       time.Time
	Status              string
}

type queueTaskState struct {
	TaskID      string
	RunID       string
	StartedAt   time.Time
	Pending     int
	Failed      bool
	Finalized   bool
	LastError   string
	LastAttempt int
}

func (r *Role) RoleID() string {
	return r.ID
}

func Build(rc config.RoleConfig) (*Role, error) {
	wsBoundedBuffer := rc.WSBoundedBufferEnabled()
	backfillPersistentComp := rc.BackfillPersistentCompEnabled()
	taskTTL := time.Duration(rc.Queue.TaskTTLSeconds) * time.Second

	// 构造传入 caller 的参数：native_call 和 batch_file 需要区分 caller_config / caller_params，其他 caller 直接使用 CallerParams
	var paramsForCaller map[string]any
	switch rc.Caller {
	case "native_call", "batch_file":
		cfg := map[string]any{}
		if rc.CallerConfig != nil {
			cfg["caller_config"] = rc.CallerConfig
		}
		if rc.CallerParams != nil {
			callerParamsCopy := cloneArgs(rc.CallerParams)
			if callerParamsCopy == nil {
				callerParamsCopy = map[string]any{}
			}
			callerParamsCopy["role_id"] = rc.RoleID
			callerParamsCopy["ws_bounded_buffer"] = wsBoundedBuffer
			cfg["caller_params"] = callerParamsCopy
		} else {
			cfg["caller_params"] = map[string]any{
				"role_id":           rc.RoleID,
				"ws_bounded_buffer": wsBoundedBuffer,
			}
		}
		paramsForCaller = cfg
	default:
		if rc.CallerParams != nil {
			paramsForCaller = cloneArgs(rc.CallerParams)
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
	backfillResultAware := make([]handler.BackfillResultAware, 0)
	var closers []io.Closer
	if len(rc.Handlers) == 0 {
		h, _ := handler.New("noop", nil)
		handlers = append(handlers, h)
		if closer, ok := h.(io.Closer); ok {
			closers = append(closers, closer)
		}
	} else {
		for _, hc := range rc.Handlers {
			with := cloneArgs(hc.With)
			if with == nil {
				with = map[string]any{}
			}
			switch strings.ToLower(hc.Type) {
			case "integrity", "missing_detector":
				if _, ok := with["backfill_result_driven_enabled"]; !ok {
					with["backfill_result_driven_enabled"] = rc.BackfillResultDrivenEnabled()
				}
				if _, ok := with["backfill_enqueue_timeout_ms"]; !ok {
					with["backfill_enqueue_timeout_ms"] = rc.BackfillEnqueueTimeoutMs
				}
				if _, ok := with["backfill_persistent_compensation"]; !ok {
					with["backfill_persistent_compensation"] = backfillPersistentComp
				}
				if _, ok := with["backfill_compensation_file"]; !ok {
					if rc.BackfillCompensationFile != "" {
						with["backfill_compensation_file"] = rc.BackfillCompensationFile
					} else {
						with["backfill_compensation_file"] = fmt.Sprintf("runtime/data/backfill_compensation_%s.json", rc.RoleID)
					}
				}
				if _, ok := with["backfill_replay_interval_ms"]; !ok && rc.BackfillReplayIntervalMs > 0 {
					with["backfill_replay_interval_ms"] = rc.BackfillReplayIntervalMs
				}
				if _, ok := with["backfill_compensation_max_pending"]; !ok && rc.BackfillCompensationMaxPend > 0 {
					with["backfill_compensation_max_pending"] = rc.BackfillCompensationMaxPend
				}
			}
			h, err := handler.New(hc.Type, with)
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
			if aware, ok := h.(handler.BackfillResultAware); ok {
				backfillResultAware = append(backfillResultAware, aware)
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

	pipelineMode := strings.ToLower(rc.PipelineMode)
	if pipelineMode == "" {
		pipelineMode = "queue"
	}
	queueMode := strings.ToLower(rc.Queue.Mode)
	if queueMode == "" {
		queueMode = "bounded"
	}
	queueEnabled := pipelineMode == "queue" && queueMode != "none"

	if !queueEnabled && len(backfillAware) > 0 {
		return nil, fmt.Errorf("role %s: backfill handlers require queue mode", rc.RoleID)
	}

	r := &Role{
		ID:             rc.RoleID,
		emitterType:    rc.Emitter,
		caller:         cl,
		pipelineMode:   pipelineMode,
		queueMode:      queueMode,
		handlers:       handlers,
		sink:           sk,
		closers:        closers,
		statusReporter: status.Get(),
		backfillStates: make(map[string]backfillState),
		tasks:          make(map[string]*queueTaskState),
		queueRetries:   3,
		taskTTL:        taskTTL,
		maxTrackedTask: rc.Queue.MaxTrackedTasks,
		strictFinalize: rc.StrictTaskFinalizationEnabled(),
		backfillResult: backfillResultAware,
	}
	if queueEnabled {
		r.q = queue.NewBounded[*types.Message](rc.Queue.Size)
	}

	if queueEnabled && len(backfillAware) > 0 {
		capacity := rc.Queue.BackfillQueueCap
		if capacity <= 0 {
			capacity = 256
		}
		defaultCh := make(chan types.BackfillCmd, capacity)
		snapshotCh := make(chan types.BackfillCmd, capacity)
		diffCh := make(chan types.BackfillCmd, capacity)
		r.backfillCh = defaultCh
		r.backfillSnapCh = snapshotCh
		r.backfillDiffCh = diffCh
		for _, aware := range backfillAware {
			if targetAware, ok := aware.(handler.BackfillTargetAware); ok {
				targetAware.SetBackfillTargets(defaultCh, snapshotCh, diffCh)
				continue
			}
			aware.SetBackfillChannel(defaultCh)
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
		// 如果没有配置 group_id，使用 worker.<role_id> 作为默认值
		if cfg.GroupID == "" {
			cfg.GroupID = fmt.Sprintf("worker.%s", rc.RoleID)
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

func extractRunID(args map[string]any) string {
	if args == nil {
		return ""
	}
	if v, ok := args["run_id"]; ok {
		return stringify(v)
	}
	if meta, ok := args["metadata"].(map[string]any); ok {
		return extractRunIDFromMeta(meta)
	}
	return ""
}

func extractRunIDFromMeta(meta map[string]any) string {
	if meta == nil {
		return ""
	}
	if v, ok := meta["run_id"]; ok {
		return stringify(v)
	}
	return ""
}

func extractTaskIDFromMeta(meta map[string]any) string {
	if meta == nil {
		return ""
	}
	if v, ok := meta["task_id"]; ok {
		return stringify(v)
	}
	if v, ok := meta["taskId"]; ok {
		return stringify(v)
	}
	return ""
}

func (r *Role) reportFailure(ctx context.Context, taskID, runID string, err error, duration time.Duration, retryable bool, statusCode int, reason string) {
	if r.statusReporter == nil || taskID == "" {
		return
	}
	stage := "final_failed"
	if retryable {
		stage = "pipeline_retry"
	}
	evt := status.Event{
		TaskID:     taskID,
		Status:     "FAILED",
		StatusCode: statusCode,
		Message:    buildStatusMessage(reason, err),
		Retryable:  retryable,
		DurationMs: duration.Milliseconds(),
		Stage:      stage,
		ErrorClass: classifyQueueError(err),
		RoleID:     r.ID,
		RunID:      runID,
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
	metrics.RecordTaskStage(r.ID, evt.Stage, "failed")
}

func (r *Role) reportSuccess(ctx context.Context, taskID, runID string, duration time.Duration) {
	if r.statusReporter == nil || taskID == "" {
		return
	}
	evt := status.Event{
		TaskID:     taskID,
		Status:     "SUCCESS",
		StatusCode: 200,
		DurationMs: duration.Milliseconds(),
		Retryable:  false,
		Stage:      "final_succeeded",
		RoleID:     r.ID,
		RunID:      runID,
		Timestamp:  time.Now().UTC(),
	}
	if err := r.statusReporter.Report(ctx, evt); err != nil {
		log.Printf("role %s: status report success failed: %v", r.ID, err)
	}
	metrics.RecordTaskStage(r.ID, evt.Stage, "success")
}

func (r *Role) reportStage(ctx context.Context, taskID, runID, stage string, err error, attempt int) {
	if r.statusReporter == nil || taskID == "" || stage == "" {
		return
	}
	evt := status.Event{
		TaskID:     taskID,
		Status:     strings.ToUpper(stage),
		StatusCode: 202,
		Message:    buildStatusMessage(stage, err),
		Retryable:  false,
		Stage:      stage,
		ErrorClass: classifyQueueError(err),
		Attempt:    attempt,
		RoleID:     r.ID,
		RunID:      runID,
		Timestamp:  time.Now().UTC(),
	}
	if err := r.statusReporter.Report(ctx, evt); err != nil {
		log.Printf("role %s: status report stage failed: %v", r.ID, err)
	}
	metrics.RecordTaskStage(r.ID, stage, "event")
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

func handlerName(h handler.Handler) string {
	if h == nil {
		return "unknown"
	}
	return fmt.Sprintf("%T", h)
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
	logging.Info(ctx, logging.EventRoleStart, "role started", logging.Fields{
		"role_id":       r.ID,
		"emitter":       r.emitterType,
		"caller":        fmt.Sprintf("%T", r.caller),
		"pipeline_mode": r.pipelineMode,
		"queue_mode":    r.queueMode,
	})
	defer logging.Info(ctx, logging.EventRoleStop, "role stopped", logging.Fields{
		"role_id": r.ID,
	})

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
	if r.pipelineMode == "queue" && r.q != nil {
		go r.consume(ctx)
		if r.backfillCh != nil {
			go r.runBackfill(ctx)
		}
	}

	// 触发器：每次触发调用caller，并把返回消息入队
	fireFunc := func(args map[string]any) {
		if args == nil {
			args = map[string]any{}
		}
		taskID := extractTaskID(args)
		runID := extractRunID(args)
		meta := ensureMetadata(args)
		if runID != "" {
			if _, ok := meta["run_id"]; !ok {
				meta["run_id"] = runID
			}
		}
		traceCtx, ok := tracing.ExtractMetadata(meta)
		if !ok {
			traceCtx = tracing.NewRoot(tracing.ShouldSample(runID, r.ID))
		}
		if runID != "" {
			traceCtx = tracing.WithBaggage(traceCtx, "run_id", runID)
		}
		tracing.InjectMetadata(meta, traceCtx)
		args["metadata"] = meta
		fireCtx := tracing.ContextWithTrace(ctx, traceCtx)

		logging.Info(fireCtx, logging.EventEmitterFire, "emitter fired", logging.Fields{
			"role_id": r.ID,
			"task_id": taskID,
			"run_id":  runID,
		})

		start := time.Now()

		// 调用 caller
		callSpan := tracing.NewChild(traceCtx)
		callCtx := tracing.ContextWithTrace(ctx, callSpan)
		logging.Info(callCtx, logging.EventCallerRequest, "caller request", logging.Fields{
			"role_id": r.ID,
			"task_id": taskID,
			"run_id":  runID,
		})
		msgs, err := r.caller.CallOnce(callCtx, args)
		if err != nil {
			retryable, statusCode := true, 0
			var callErr *caller.CallError
			if errors.As(err, &callErr) {
				retryable = callErr.Retryable
				statusCode = callErr.StatusCode
			}
			r.reportFailure(ctx, taskID, runID, err, time.Since(start), retryable, statusCode, "caller error")
			logging.Error(callCtx, logging.EventCallerError, "caller error", err, logging.Fields{
				"role_id":     r.ID,
				"task_id":     taskID,
				"run_id":      runID,
				"retryable":   retryable,
				"status_code": statusCode,
			})
			log.Printf("role %s: caller error: %v", r.ID, err)
			return
		}
		runIDForLog := runID
		if runIDForLog == "" {
			for _, msg := range msgs {
				if msg == nil {
					continue
				}
				if candidate := extractRunIDFromMeta(msg.Metadata); candidate != "" {
					runIDForLog = candidate
					break
				}
			}
		}
		logging.Info(callCtx, logging.EventCallerResponse, "caller response", logging.Fields{
			"role_id":    r.ID,
			"task_id":    taskID,
			"run_id":     runIDForLog,
			"elapsed_ms": time.Since(start).Milliseconds(),
			"msg_count":  len(msgs),
			"pipeline":   r.pipelineMode,
			"queue_mode": r.queueMode,
		})
		if taskID != "" {
			r.reportStage(callCtx, taskID, runIDForLog, "caller_accepted", nil, 0)
		}

		for _, msg := range msgs {
			if msg == nil {
				continue
			}
			if msg.Metadata == nil {
				msg.Metadata = map[string]any{}
			}
			if _, ok := msg.Metadata["role_id"]; !ok {
				msg.Metadata["role_id"] = r.ID
			}
			if taskID != "" {
				if _, ok := msg.Metadata["task_id"]; !ok {
					msg.Metadata["task_id"] = taskID
				}
			}
			if runIDForLog != "" {
				if _, ok := msg.Metadata["run_id"]; !ok {
					msg.Metadata["run_id"] = runIDForLog
				}
			}
			tracing.InjectMetadata(msg.Metadata, callSpan)
		}

		if r.pipelineMode == "direct" || r.q == nil {
			if err := r.handleDirect(callCtx, msgs); err != nil {
				r.reportFailure(ctx, taskID, runIDForLog, err, time.Since(start), true, 0, "direct pipeline error")
				logging.Error(callCtx, logging.EventPipelineError, "direct pipeline error", err, logging.Fields{
					"role_id": r.ID,
					"task_id": taskID,
					"run_id":  runIDForLog,
				})
				log.Printf("role %s: direct pipeline error: %v", r.ID, err)
				return
			}
			r.reportSuccess(ctx, taskID, runIDForLog, time.Since(start))
		} else {
			// 消息入队
			enqueued := 0
			for _, m := range msgs {
				if m == nil {
					continue
				}
				if err := r.q.Enqueue(ctx, m); err != nil {
					if enqueued > 0 {
						r.registerQueuedTask(taskID, runIDForLog, enqueued, start)
						r.markQueuedTaskFailed(taskID, runIDForLog, err)
						r.reportStage(callCtx, taskID, runIDForLog, "queue_enqueued", nil, 0)
					}
					log.Printf("role %s: enqueue error: %v", r.ID, err)
					r.reportFailure(ctx, taskID, runIDForLog, err, time.Since(start), true, 0, "enqueue error")
					logging.Error(callCtx, logging.EventQueueEnqueue, "enqueue error", err, logging.Fields{
						"role_id": r.ID,
						"task_id": taskID,
						"run_id":  runIDForLog,
					})
					return
				}
				enqueued++
			}
			if enqueued == 0 {
				r.reportSuccess(ctx, taskID, runIDForLog, time.Since(start))
				return
			}
			if !r.strictFinalize {
				r.reportStage(callCtx, taskID, runIDForLog, "queue_enqueued", nil, 0)
				r.reportSuccess(ctx, taskID, runIDForLog, time.Since(start))
				return
			}
			r.registerQueuedTask(taskID, runIDForLog, enqueued, start)
			r.reportStage(callCtx, taskID, runIDForLog, "queue_enqueued", nil, 0)
		}
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

func (r *Role) handleDirect(ctx context.Context, msgs []*types.Message) error {
	if len(msgs) == 0 {
		return nil
	}
	for _, msg := range msgs {
		if msg == nil {
			continue
		}
		runID := extractRunIDFromMeta(msg.Metadata)
		traceCtx, ok := tracing.ExtractMetadata(msg.Metadata)
		if !ok {
			traceCtx = tracing.NewRoot(tracing.ShouldSample(runID, r.ID))
		}
		msgCtx := tracing.ContextWithTrace(ctx, traceCtx)

		curMsgs := []*types.Message{msg}
		for _, h := range r.handlers {
			next := make([]*types.Message, 0, len(curMsgs))
			for _, m := range curMsgs {
				if m == nil {
					continue
				}
				outs, err := h.Handle(m)
				if err != nil {
					logging.Error(msgCtx, logging.EventHandlerError, "handler error", err, logging.Fields{
						"role_id":  r.ID,
						"handler":  handlerName(h),
						"run_id":   runID,
						"task_id":  extractTaskIDFromMeta(m.Metadata),
						"pipeline": r.pipelineMode,
					})
					return fmt.Errorf("handler error: %w", err)
				}
				next = append(next, outs...)
			}
			curMsgs = next
			if len(curMsgs) == 0 {
				break
			}
		}
		if len(curMsgs) == 0 {
			continue
		}
		if err := r.writeMessagesToSink(curMsgs); err != nil {
			logging.Error(msgCtx, logging.EventSinkError, "sink error", err, logging.Fields{
				"role_id": r.ID,
			})
			return fmt.Errorf("sink error: %w", err)
		}
		logging.Info(msgCtx, logging.EventPipelineFinish, "direct pipeline finished", logging.Fields{
			"role_id":   r.ID,
			"run_id":    runID,
			"msg_count": len(curMsgs),
		})
	}
	return nil
}

func (r *Role) runBackfill(ctx context.Context) {
	startWorker := func(workerID int, ch <-chan types.BackfillCmd) {
		if ch == nil {
			return
		}
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case cmd := <-ch:
					r.handleBackfillCmd(ctx, workerID, cmd)
				}
			}
		}()
	}
	startWorker(0, r.backfillSnapCh)
	startWorker(1, r.backfillDiffCh)
	startWorker(2, r.backfillCh)
	<-ctx.Done()
}

func (r *Role) handleBackfillCmd(ctx context.Context, workerID int, cmd types.BackfillCmd) {
	now := time.Now()
	cmd.EnsureDefaults(now)
	maxFailures, exhaustCooldown, retryBackoff := backfillPolicyFromCmd(cmd)
	stateKey := r.backfillStateKey(cmd)
	if wait, skip := r.backfillCooldownRemaining(stateKey, now); skip {
		logging.Warn(ctx, logging.EventIntegrityBackfillSkipped, "backfill skipped by cooldown", logging.Fields{
			"role_id":       r.ID,
			"worker_id":     workerID,
			"backfill_type": cmd.Type,
			"stream_key":    cmd.StreamKey,
			"start":         cmd.Start,
			"end":           cmd.End,
			"cooldown_ms":   wait.Milliseconds(),
		})
		r.emitBackfillResult(cmd, types.BackfillResultTimeout, "timeout", now)
		return
	}

	baseFields := logging.Fields{
		"role_id":       r.ID,
		"worker_id":     workerID,
		"backfill_type": cmd.Type,
		"stream_key":    cmd.StreamKey,
		"start":         cmd.Start,
		"end":           cmd.End,
		"attempts":      len(cmd.Attempts),
		"max_failures":  maxFailures,
	}
	if len(cmd.Attempts) == 0 {
		logging.Warn(ctx, logging.EventIntegrityBackfillSkipped, "backfill skipped (no attempts)", baseFields)
		log.Printf("role %s worker-%d: backfill %s skipped (no attempts)", r.ID, workerID, cmd.Type)
		r.emitBackfillResult(cmd, types.BackfillResultFail, "unknown", time.Now())
		return
	}
	logging.Info(ctx, logging.EventIntegrityBackfillTrigger, "backfill triggered", baseFields)
	lastErrClass := "unknown"
	for _, attempt := range cmd.Attempts {
		ok, errClass := r.executeBackfillAttempt(ctx, workerID, cmd, attempt)
		if ok {
			successFields := logging.Fields{
				"role_id":       r.ID,
				"worker_id":     workerID,
				"backfill_type": cmd.Type,
				"stream_key":    cmd.StreamKey,
				"start":         cmd.Start,
				"end":           cmd.End,
				"attempt":       attempt.Name,
				"state":         "healthy",
			}
			r.backfillMarkSuccess(stateKey)
			logging.Info(ctx, logging.EventIntegrityBackfillSuccess, "backfill succeeded", successFields)
			r.emitBackfillResult(cmd, types.BackfillResultSuccess, "", time.Now())
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
		lastErrClass = errClass
		logging.Warn(ctx, logging.EventIntegrityBackfillAttempt, "backfill attempt failed", logging.Fields{
			"role_id":       r.ID,
			"worker_id":     workerID,
			"backfill_type": cmd.Type,
			"stream_key":    cmd.StreamKey,
			"start":         cmd.Start,
			"end":           cmd.End,
			"attempt":       attempt.Name,
		})
		log.Printf("role %s worker-%d: backfill attempt %s failed for %s", r.ID, workerID, attempt.Name, cmd.Type)
		if retryBackoff > 0 {
			timer := time.NewTimer(retryBackoff)
			select {
			case <-ctx.Done():
				timer.Stop()
				r.emitBackfillResult(cmd, types.BackfillResultTimeout, "timeout", time.Now())
				return
			case <-timer.C:
			}
		}
	}
	state, wait := r.backfillMarkFailure(stateKey, maxFailures, exhaustCooldown, now)
	exhaustFields := cloneBackfillFields(baseFields)
	exhaustFields["state"] = state
	if wait > 0 {
		exhaustFields["cooldown_ms"] = wait.Milliseconds()
	}
	logging.Error(ctx, logging.EventIntegrityBackfillExhaust, "backfill exhausted", errors.New("all backfill attempts failed"), exhaustFields)
	r.emitBackfillResult(cmd, types.BackfillResultFail, lastErrClass, time.Now())
	log.Printf("role %s worker-%d: backfill %s exhausted (start=%d end=%d)", r.ID, workerID, cmd.Type, cmd.Start, cmd.End)
}

func (r *Role) executeBackfillAttempt(ctx context.Context, workerID int, cmd types.BackfillCmd, attempt types.BackfillAttempt) (bool, string) {
	if len(attempt.Requests) == 0 {
		return false, "unknown"
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
				return false, "timeout"
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
				return false, "timeout"
			case <-timer.C:
			}
		}
	}
	for idx, req := range attempt.Requests {
		select {
		case <-ctx.Done():
			return false, "timeout"
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
		if _, ok := meta["role_id"]; !ok {
			meta["role_id"] = r.ID
		}
		if _, ok := meta["is_backfill"]; !ok {
			meta["is_backfill"] = true
		}
		if _, ok := meta["backfill_type"]; !ok {
			meta["backfill_type"] = cmd.Type
		}
		if _, ok := meta["cmd_id"]; !ok && cmd.CmdID != "" {
			meta["cmd_id"] = cmd.CmdID
		}
		if _, ok := meta["session_id"]; !ok && cmd.SessionID != "" {
			meta["session_id"] = cmd.SessionID
		}
		if _, ok := meta["backfill_key"]; !ok && cmd.Key != "" {
			meta["backfill_key"] = cmd.Key
		}
		if _, ok := meta["backfill_cmd_attempt"]; !ok && cmd.Attempt > 0 {
			meta["backfill_cmd_attempt"] = cmd.Attempt
		}
		if attempt.Name != "" {
			meta["backfill_attempt"] = attempt.Name
		}
		meta["backfill_step"] = idx + 1
		meta["backfill_steps"] = len(attempt.Requests)
		if cmd.Type == types.BackfillTypeSnapshot {
			meta["snapshot"] = true
			if cmd.SnapshotSource != "" {
				meta["snapshot_source"] = cmd.SnapshotSource
			}
			if cmd.SnapshotReason != "" {
				meta["snapshot_reason"] = cmd.SnapshotReason
			}
		}
		args["metadata"] = meta

		msgs, err := r.executeBackfillRequest(ctx, strings.ToLower(req.Transport), args)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return false, "timeout"
			}
			var callErr *caller.CallError
			if errors.As(err, &callErr) {
				logging.Warn(ctx, logging.EventIntegrityBackfillAttempt, "backfill request failed", logging.Fields{
					"role_id":       r.ID,
					"worker_id":     workerID,
					"backfill_type": cmd.Type,
					"attempt":       attempt.Name,
					"request_step":  idx + 1,
					"status_code":   callErr.StatusCode,
					"retryable":     callErr.Retryable,
					"error":         callErr.Err.Error(),
				})
				log.Printf("role %s worker-%d: attempt %s request %d failed (status=%d retryable=%v): %v", r.ID, workerID, attempt.Name, idx+1, callErr.StatusCode, callErr.Retryable, callErr.Err)
			} else {
				logging.Warn(ctx, logging.EventIntegrityBackfillAttempt, "backfill request failed", logging.Fields{
					"role_id":       r.ID,
					"worker_id":     workerID,
					"backfill_type": cmd.Type,
					"attempt":       attempt.Name,
					"request_step":  idx + 1,
					"error":         err.Error(),
				})
				log.Printf("role %s worker-%d: attempt %s request %d failed: %v", r.ID, workerID, attempt.Name, idx+1, err)
			}
			return false, "unknown"
		}

		for _, msg := range msgs {
			if msg == nil {
				continue
			}
			if msg.Metadata == nil {
				msg.Metadata = map[string]any{}
			}
			if _, ok := msg.Metadata["role_id"]; !ok {
				msg.Metadata["role_id"] = r.ID
			}
			if attempt.Name != "" {
				msg.Metadata["backfill_attempt"] = attempt.Name
			}
			if _, ok := msg.Metadata["backfill_type"]; !ok {
				msg.Metadata["backfill_type"] = cmd.Type
			}
			if _, ok := msg.Metadata["cmd_id"]; !ok && cmd.CmdID != "" {
				msg.Metadata["cmd_id"] = cmd.CmdID
			}
			if _, ok := msg.Metadata["session_id"]; !ok && cmd.SessionID != "" {
				msg.Metadata["session_id"] = cmd.SessionID
			}
			if _, ok := msg.Metadata["backfill_key"]; !ok && cmd.Key != "" {
				msg.Metadata["backfill_key"] = cmd.Key
			}
			if _, ok := msg.Metadata["backfill_cmd_attempt"]; !ok && cmd.Attempt > 0 {
				msg.Metadata["backfill_cmd_attempt"] = cmd.Attempt
			}
			if cmd.Type == types.BackfillTypeSnapshot {
				msg.Metadata["snapshot"] = true
				if cmd.SnapshotSource != "" {
					msg.Metadata["snapshot_source"] = cmd.SnapshotSource
				}
				if cmd.SnapshotReason != "" {
					msg.Metadata["snapshot_reason"] = cmd.SnapshotReason
				}
			}
			if err := r.q.Enqueue(ctx, msg); err != nil {
				logging.Error(ctx, logging.EventIntegrityBackfillEnqueue, "enqueue backfill message failed", err, logging.Fields{
					"role_id":       r.ID,
					"worker_id":     workerID,
					"backfill_type": cmd.Type,
					"attempt":       attempt.Name,
					"request_step":  idx + 1,
				})
				log.Printf("role %s worker-%d: enqueue backfill message failed: %v", r.ID, workerID, err)
				if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
					return false, "timeout"
				}
				return false, "unknown"
			}
		}
	}
	return true, ""
}

func backfillPolicyFromCmd(cmd types.BackfillCmd) (int, time.Duration, time.Duration) {
	maxFailures := 3
	exhaustCooldown := 30 * time.Second
	retryBackoff := 300 * time.Millisecond
	if len(cmd.Attempts) == 0 || len(cmd.Attempts[0].Requests) == 0 {
		return maxFailures, exhaustCooldown, retryBackoff
	}
	meta := extractMetadata(cmd.Attempts[0].Requests[0].Args)
	if meta == nil {
		return maxFailures, exhaustCooldown, retryBackoff
	}
	if v, ok := meta["backfill_max_failures"]; ok {
		if n := toInt(v, maxFailures); n > 0 {
			maxFailures = n
		}
	}
	if v, ok := meta["backfill_exhaust_cooldown_ms"]; ok {
		if ms := toInt(v, int(exhaustCooldown/time.Millisecond)); ms >= 0 {
			exhaustCooldown = time.Duration(ms) * time.Millisecond
		}
	}
	if v, ok := meta["backfill_retry_backoff_ms"]; ok {
		if ms := toInt(v, int(retryBackoff/time.Millisecond)); ms >= 0 {
			retryBackoff = time.Duration(ms) * time.Millisecond
		}
	}
	return maxFailures, exhaustCooldown, retryBackoff
}

func cloneBackfillFields(in logging.Fields) logging.Fields {
	out := make(logging.Fields, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func (r *Role) backfillStateKey(cmd types.BackfillCmd) string {
	if cmd.Key != "" {
		return cmd.Key
	}
	stream := cmd.StreamKey
	if stream == "" {
		stream = "default"
	}
	roleID := cmd.RoleID
	if roleID == "" {
		roleID = r.ID
	}
	return types.BackfillSessionKey(roleID, stream, cmd.Type)
}

func (r *Role) backfillCooldownRemaining(key string, now time.Time) (time.Duration, bool) {
	r.backfillMu.Lock()
	defer r.backfillMu.Unlock()
	state, ok := r.backfillStates[key]
	if !ok {
		return 0, false
	}
	if state.CooldownUntil.IsZero() || !now.Before(state.CooldownUntil) {
		return 0, false
	}
	return state.CooldownUntil.Sub(now), true
}

func (r *Role) backfillMarkSuccess(key string) {
	r.backfillMu.Lock()
	defer r.backfillMu.Unlock()
	delete(r.backfillStates, key)
}

func (r *Role) backfillMarkFailure(key string, maxFailures int, cooldown time.Duration, now time.Time) (string, time.Duration) {
	if maxFailures <= 0 {
		maxFailures = 1
	}
	r.backfillMu.Lock()
	defer r.backfillMu.Unlock()
	state := r.backfillStates[key]
	state.ConsecutiveFailures++
	state.Status = "degraded"
	if state.ConsecutiveFailures >= maxFailures {
		if cooldown > 0 {
			state.CooldownUntil = now.Add(cooldown)
		} else {
			state.CooldownUntil = time.Time{}
		}
		state.Status = "cooldown"
		state.ConsecutiveFailures = 0
	}
	r.backfillStates[key] = state
	if state.CooldownUntil.IsZero() || !now.Before(state.CooldownUntil) {
		return state.Status, 0
	}
	return state.Status, state.CooldownUntil.Sub(now)
}

func (r *Role) emitBackfillResult(cmd types.BackfillCmd, status, errorClass string, finishedAt time.Time) {
	if len(r.backfillResult) == 0 {
		return
	}
	result := types.BackfillResult{
		Status:     strings.ToLower(strings.TrimSpace(status)),
		ErrorClass: types.NormalizeBackfillErrorClass(errorClass),
		FinishedAt: finishedAt,
	}
	result.EnsureDefaultsFromCmd(cmd, finishedAt)
	for _, aware := range r.backfillResult {
		aware.OnBackfillResult(result)
	}
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

func (r *Role) registerQueuedTask(taskID, runID string, count int, startedAt time.Time) {
	if taskID == "" || count <= 0 {
		return
	}
	key := taskTrackingKey(taskID, runID)
	r.taskMu.Lock()
	defer r.taskMu.Unlock()
	r.cleanupExpiredTasksLocked(time.Now())
	state, ok := r.tasks[key]
	if !ok {
		if r.maxTrackedTask > 0 && len(r.tasks) >= r.maxTrackedTask {
			r.evictOldestTaskLocked()
		}
		state = &queueTaskState{
			TaskID:    taskID,
			RunID:     runID,
			StartedAt: startedAt,
		}
		r.tasks[key] = state
	}
	if state.StartedAt.IsZero() || startedAt.Before(state.StartedAt) {
		state.StartedAt = startedAt
	}
	state.Pending += count
}

func (r *Role) cleanupExpiredTasksLocked(now time.Time) {
	if r.taskTTL <= 0 || len(r.tasks) == 0 {
		return
	}
	for key, state := range r.tasks {
		if state == nil || state.StartedAt.IsZero() {
			continue
		}
		if now.Sub(state.StartedAt) > r.taskTTL {
			delete(r.tasks, key)
			logging.Warn(context.Background(), logging.EventPipelineError, "queue task tracker expired", logging.Fields{
				"role_id":    r.ID,
				"task_id":    state.TaskID,
				"run_id":     state.RunID,
				"task_ttl_s": int(r.taskTTL.Seconds()),
			})
		}
	}
}

func (r *Role) evictOldestTaskLocked() {
	var oldestKey string
	var oldest *queueTaskState
	for key, state := range r.tasks {
		if state == nil {
			continue
		}
		if oldest == nil || state.StartedAt.Before(oldest.StartedAt) {
			oldest = state
			oldestKey = key
		}
	}
	if oldest == nil {
		return
	}
	delete(r.tasks, oldestKey)
	logging.Warn(context.Background(), logging.EventPipelineError, "queue task tracker evicted by cap", logging.Fields{
		"role_id":          r.ID,
		"task_id":          oldest.TaskID,
		"run_id":           oldest.RunID,
		"max_tracked_task": r.maxTrackedTask,
	})
}

func (r *Role) markQueuedTaskFailed(taskID, runID string, err error) {
	if taskID == "" {
		return
	}
	key := taskTrackingKey(taskID, runID)
	r.taskMu.Lock()
	defer r.taskMu.Unlock()
	state, ok := r.tasks[key]
	if !ok {
		return
	}
	state.Failed = true
	if err != nil {
		state.LastError = err.Error()
	}
}

func taskTrackingKey(taskID, runID string) string {
	return taskID + "|" + runID
}

func classifyQueueError(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline_exceeded"
	case errors.Is(err, context.Canceled):
		return "canceled"
	default:
		return "pipeline_error"
	}
}

func (r *Role) finalizeQueuedMessage(ctx context.Context, msg *types.Message, runID string, err error, attempt int, stage string) {
	taskID := ""
	if msg != nil {
		taskID = extractTaskIDFromMeta(msg.Metadata)
	}
	if taskID == "" {
		return
	}
	key := taskTrackingKey(taskID, runID)
	now := time.Now()
	shouldReport := false
	reportFailed := false
	duration := int64(0)
	lastErr := err

	r.taskMu.Lock()
	state, ok := r.tasks[key]
	if !ok {
		r.taskMu.Unlock()
		return
	}
	if err != nil {
		state.Failed = true
		state.LastError = err.Error()
		state.LastAttempt = attempt
	}
	if state.Pending > 0 {
		state.Pending--
	}
	if state.Pending == 0 && !state.Finalized {
		state.Finalized = true
		shouldReport = true
		reportFailed = state.Failed
		if state.StartedAt.IsZero() {
			state.StartedAt = now
		}
		duration = now.Sub(state.StartedAt).Milliseconds()
		if reportFailed && lastErr == nil && state.LastError != "" {
			lastErr = errors.New(state.LastError)
		}
		delete(r.tasks, key)
	}
	r.taskMu.Unlock()

	if stage != "" {
		r.reportStage(ctx, taskID, runID, stage, err, attempt)
	}
	if !shouldReport {
		return
	}
	if reportFailed {
		r.reportStage(ctx, taskID, runID, "final_failed", lastErr, attempt)
		r.reportFailure(ctx, taskID, runID, lastErr, time.Duration(duration)*time.Millisecond, false, 0, "queue pipeline failed")
		return
	}
	r.reportStage(ctx, taskID, runID, "pipeline_succeeded", nil, attempt)
	r.reportSuccess(ctx, taskID, runID, time.Duration(duration)*time.Millisecond)
}

func (r *Role) pushDLQ(msg *types.Message, err error, attempt int) {
	if msg == nil {
		return
	}
	cloneMeta := cloneArgs(msg.Metadata)
	if cloneMeta == nil {
		cloneMeta = map[string]any{}
	}
	cloneMeta["dlq"] = true
	cloneMeta["error"] = err.Error()
	cloneMeta["attempt"] = attempt
	clone := &types.Message{
		Metadata: cloneMeta,
		Payload:  append([]byte(nil), msg.Payload...),
	}
	r.dlqMu.Lock()
	r.dlqBuffer = append(r.dlqBuffer, clone)
	dlqSize := len(r.dlqBuffer)
	r.dlqMu.Unlock()
	logging.Error(context.Background(), logging.EventPipelineError, "queue message moved to dlq", err, logging.Fields{
		"role_id":  r.ID,
		"task_id":  extractTaskIDFromMeta(msg.Metadata),
		"run_id":   extractRunIDFromMeta(msg.Metadata),
		"attempt":  attempt,
		"dlq_size": dlqSize,
	})
}

func (r *Role) retryOrDLQ(ctx context.Context, msgCtx context.Context, msg *types.Message, runID string, err error) {
	if msg == nil {
		return
	}
	if msg.Metadata == nil {
		msg.Metadata = map[string]any{}
	}
	attempt := toInt(msg.Metadata["_queue_attempt"], 0)
	if attempt < r.queueRetries {
		msg.Metadata["_queue_attempt"] = attempt + 1
		r.reportStage(msgCtx, extractTaskIDFromMeta(msg.Metadata), runID, "pipeline_retry", err, attempt+1)
		if requeueErr := r.q.Enqueue(ctx, msg); requeueErr == nil {
			return
		} else {
			err = fmt.Errorf("requeue failed after pipeline error: %w", requeueErr)
		}
	}
	r.pushDLQ(msg, err, attempt)
	r.reportStage(msgCtx, extractTaskIDFromMeta(msg.Metadata), runID, "pipeline_failed", err, attempt)
	r.finalizeQueuedMessage(msgCtx, msg, runID, err, attempt, "")
}

func (r *Role) consume(ctx context.Context) {
	for {
		msg, err := r.q.Dequeue(ctx)
		if err != nil {
			// 正常退出
			return
		}
		if msg == nil {
			continue
		}
		if msg.Metadata == nil {
			msg.Metadata = map[string]any{}
		}
		runID := extractRunIDFromMeta(msg.Metadata)
		traceCtx, ok := tracing.ExtractMetadata(msg.Metadata)
		if !ok {
			traceCtx = tracing.NewRoot(tracing.ShouldSample(runID, r.ID))
		}
		msgCtx := tracing.ContextWithTrace(ctx, traceCtx)
		curMsgs := []*types.Message{msg}
		handlerErr := error(nil)
		failedMsg := msg

		for _, h := range r.handlers {
			next := make([]*types.Message, 0, len(curMsgs))
			for _, m := range curMsgs {
				if m == nil {
					continue
				}
				outs, err := h.Handle(m)
				if err != nil {
					handlerErr = err
					failedMsg = m
					log.Printf("role %s: handler error: %v", r.ID, err)
					logging.Error(msgCtx, logging.EventHandlerError, "handler error", err, logging.Fields{
						"role_id":  r.ID,
						"handler":  handlerName(h),
						"run_id":   runID,
						"task_id":  extractTaskIDFromMeta(m.Metadata),
						"pipeline": r.pipelineMode,
					})
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
		if handlerErr != nil {
			r.retryOrDLQ(ctx, msgCtx, failedMsg, runID, handlerErr)
			continue
		}
		if len(curMsgs) == 0 {
			r.finalizeQueuedMessage(msgCtx, msg, runID, nil, toInt(msg.Metadata["_queue_attempt"], 0), "")
			continue
		}
		wrote := hasNonNilMessage(curMsgs)
		if !wrote {
			r.finalizeQueuedMessage(msgCtx, msg, runID, nil, toInt(msg.Metadata["_queue_attempt"], 0), "")
			continue
		}
		if err := r.writeMessagesToSink(curMsgs); err != nil {
			log.Printf("role %s: sink error: %v", r.ID, err)
			logging.Error(msgCtx, logging.EventSinkError, "sink error", err, logging.Fields{
				"role_id": r.ID,
				"run_id":  runID,
			})
			r.retryOrDLQ(ctx, msgCtx, msg, runID, err)
			continue
		}
		r.finalizeQueuedMessage(msgCtx, msg, runID, nil, toInt(msg.Metadata["_queue_attempt"], 0), "")
		logging.Info(msgCtx, logging.EventPipelineFinish, "queue pipeline finished", logging.Fields{
			"role_id":   r.ID,
			"run_id":    runID,
			"msg_count": len(curMsgs),
		})
	}
}

func hasNonNilMessage(msgs []*types.Message) bool {
	for _, msg := range msgs {
		if msg != nil {
			return true
		}
	}
	return false
}

func (r *Role) writeMessagesToSink(msgs []*types.Message) error {
	if len(msgs) == 0 {
		return nil
	}
	if batchSink, ok := r.sink.(sink.BatchSink); ok {
		filtered := make([]*types.Message, 0, len(msgs))
		for _, msg := range msgs {
			if msg != nil {
				filtered = append(filtered, msg)
			}
		}
		if len(filtered) == 0 {
			return nil
		}
		return batchSink.WriteBatch(filtered)
	}
	for _, msg := range msgs {
		if msg == nil {
			continue
		}
		if err := r.sink.Write(msg); err != nil {
			return err
		}
	}
	return nil
}
