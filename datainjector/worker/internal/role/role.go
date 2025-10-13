package role

import (
	"context"
	"io"
	"log"
	"strconv"
	"time"

	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/caller"
	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/config"
	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/emitter"
	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/handler"
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
	q              *queue.BoundedQueue[*types.Message]
	handlers       []handler.Handler
	sink           sink.Sink
	closers        []io.Closer
	backfillCh     chan types.BackfillCmd
	backfillers    map[string]caller.BlockFetcher
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

	sk, err := sink.New(rc.Sink.Type, rc.Sink.With)
	if err != nil {
		return nil, err
	}

	r := &Role{
		ID:          rc.RoleID,
		emitterType: rc.Emitter,
		caller:      cl,
		q:           queue.NewBounded[*types.Message](rc.Queue.Size),
		handlers:    handlers,
		sink:        sk,
		closers:     closers,
		backfillers: make(map[string]caller.BlockFetcher),
	}

	if exec, ok := cl.(caller.BlockFetcher); ok {
		r.backfillers[exec.TransportName()] = exec
	}
	if provider, ok := cl.(caller.BackfillProvider); ok {
		for _, extra := range provider.BackfillExecutors() {
			if extra == nil {
				continue
			}
			r.backfillers[extra.TransportName()] = extra
		}
	}

	if len(backfillAware) > 0 {
		ch := make(chan types.BackfillCmd, 256) // 增加缓冲容量：16 -> 256
		r.backfillCh = ch
		for _, aware := range backfillAware {
			aware.SetBackfillChannel(ch)
		}
		if len(r.backfillers) == 0 {
			log.Printf("role %s: backfill channel enabled but no executors registered", rc.RoleID)
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
		// 可在此填充args（例如cursor管理）
		msgs, err := r.caller.CallOnce(ctx, args)
		if err != nil {
			log.Printf("role %s: caller error: %v", r.ID, err)
			return
		}

		for _, m := range msgs {
			if m == nil {
				continue
			}
			if err := r.q.Enqueue(ctx, m); err != nil {
				log.Printf("role %s: enqueue error: %v", r.ID, err)
				return
			}
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

func (r *Role) runBackfill(ctx context.Context) {
	// 启动多个并发 worker 处理补数任务
	const numWorkers = 3
	for i := 0; i < numWorkers; i++ {
		go func(workerID int) {
			for {
				select {
				case <-ctx.Done():
					return
				case cmd := <-r.backfillCh:
					if cmd.End < cmd.Start {
						log.Printf("role %s worker-%d: invalid backfill range [%d, %d]", r.ID, workerID, cmd.Start, cmd.End)
						continue
					}
					succeeded := false
					for _, opt := range cmd.Options {
						exec := r.backfillers[opt.Transport]
						if exec == nil {
							log.Printf("role %s worker-%d: backfill transport %s not available", r.ID, workerID, opt.Transport)
							continue
						}
						msgs, err := exec.FetchBlocks(ctx, cmd.Start, cmd.End, opt.RPCMethod, opt.Params)
						if err != nil {
							log.Printf("role %s worker-%d: backfill %s [%d, %d] failed: %v", r.ID, workerID, opt.Transport, cmd.Start, cmd.End, err)
							continue
						}
						for _, m := range msgs {
							if m == nil {
								continue
							}
							if err := r.q.Enqueue(ctx, m); err != nil {
								log.Printf("role %s worker-%d: enqueue backfill message failed: %v", r.ID, workerID, err)
								break
							}
						}
						succeeded = true
						log.Printf("role %s worker-%d: backfill [%d, %d] succeeded via %s", r.ID, workerID, cmd.Start, cmd.End, opt.Transport)
						break
					}
					if !succeeded {
						log.Printf("role %s worker-%d: backfill [%d, %d] all transports failed", r.ID, workerID, cmd.Start, cmd.End)
					}
				}
			}
		}(i)
	}
	// 主协程保持存活直到 context 取消
	<-ctx.Done()
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
