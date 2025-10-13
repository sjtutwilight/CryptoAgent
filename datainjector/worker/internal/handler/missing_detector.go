package handler

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/types"
)

// MissingDetector 按序检测、乱序缓冲并触发补数。
// 仅支持严格单调递增的整数序列场景。
type MissingDetector struct {
	sequenceField string

	expectedNext uint64
	initialized  bool

	buffer    map[uint64][]*types.Message
	firstSeen map[uint64]time.Time
	seenMax   uint64

	lastSweep    time.Time
	lastBackfill backfillRecord
	cfg          detectorConfig

	backfillCh chan<- types.BackfillCmd
}

type detectorConfig struct {
	eagerGap      uint64
	maxRange      uint64
	maxDelay      time.Duration
	hardTimeout   time.Duration
	maxGap        uint64
	sweepInterval time.Duration
	bucketTTL     time.Duration
	maxBuckets    int

	backfillOptions []types.BackfillOption
}

type backfillRecord struct {
	start uint64
	end   uint64
	at    time.Time
}

func init() {
	Register("missing_detector", func(cfg map[string]any) (Handler, error) {
		return newMissingDetector(cfg)
	})
}

func newMissingDetector(cfg map[string]any) (Handler, error) {
	if cfg == nil {
		return nil, fmt.Errorf("missing_detector: 配置不能为空")
	}

	sequenceField := cfgString(cfg, "sequence_field", "")
	if sequenceField == "" {
		return nil, fmt.Errorf("missing_detector: sequence_field 必填")
	}

	dc := detectorConfig{
		eagerGap:      uint64(cfgInt(cfg, "eager_gap", 3)),
		maxRange:      uint64(cfgInt(cfg, "max_range", 20)),
		maxDelay:      time.Duration(cfgInt(cfg, "max_delay_ms", 800)) * time.Millisecond,
		hardTimeout:   time.Duration(cfgInt(cfg, "hard_timeout_ms", 3000)) * time.Millisecond,
		maxGap:        uint64(cfgInt(cfg, "max_gap", 8)),
		sweepInterval: time.Duration(cfgInt(cfg, "sweep_interval_ms", 200)) * time.Millisecond,
		bucketTTL:     time.Duration(cfgInt(cfg, "bucket_ttl_ms", 3000)) * time.Millisecond,
		maxBuckets:    cfgInt(cfg, "max_buckets", 2000),
	}

	backfillOptions := make([]types.BackfillOption, 0, 2)
	if bfRaw, ok := cfg["backfill"].(map[string]any); ok {
		if wsCfg, ok := bfRaw["ws"].(map[string]any); ok && cfgBool(wsCfg, "enabled", false) {
			params := map[string]any{}
			if cfgBool(wsCfg, "include_full_tx", false) {
				params["include_full_tx"] = true
			}
			backfillOptions = append(backfillOptions, types.BackfillOption{
				Transport: types.BackfillTransportWebSocket,
				RPCMethod: cfgString(wsCfg, "rpc_method", "eth_getBlockByNumber"),
				Params:    params,
			})
		}
		if httpCfg, ok := bfRaw["http"].(map[string]any); ok && cfgBool(httpCfg, "enabled", false) {
			params := map[string]any{}
			if cfgBool(httpCfg, "include_full_tx", false) {
				params["include_full_tx"] = true
			}
			backfillOptions = append(backfillOptions, types.BackfillOption{
				Transport: types.BackfillTransportHTTP,
				RPCMethod: cfgString(httpCfg, "rpc_method", "eth_getBlockByNumber"),
				Params:    params,
			})
		}
	}
	dc.backfillOptions = backfillOptions

	md := &MissingDetector{
		sequenceField: sequenceField,
		buffer:        make(map[uint64][]*types.Message),
		firstSeen:     make(map[uint64]time.Time),
		cfg:           dc,
	}

	if v, ok := cfg["start_sequence"]; ok {
		if seq, err := cfgToUint64(v); err == nil {
			md.expectedNext = seq
			md.initialized = true
		}
	}

	return md, nil
}

func (h *MissingDetector) SetBackfillChannel(ch chan<- types.BackfillCmd) {
	if len(h.cfg.backfillOptions) == 0 {
		return
	}
	h.backfillCh = ch
}

func (h *MissingDetector) Handle(msg *types.Message) ([]*types.Message, error) {
	if msg == nil {
		return nil, nil
	}
	seqVal, err := h.extractSequence(msg)
	if err != nil {
		return nil, err
	}
	seq := uint64(seqVal)
	now := time.Now()

	// 初始化
	if !h.initialized {
		h.initialized = true
		h.expectedNext = seq
		h.seenMax = seq
		log.Printf("missing_detector: initialized with expectedNext=%d", seq)
	}

	// 规则1：等于 - 立即下游
	if seq == h.expectedNext {
		log.Printf("missing_detector: [RULE-1 EQUAL] seq=%d == expectedNext, outputting", seq)
		out := make([]*types.Message, 0, 8)
		out = append(out, msg)
		delete(h.firstSeen, seq)
		h.expectedNext++

		// 持续冲刷 buffer
		drained := h.drainBuffer(now)
		if len(drained) > 0 {
			log.Printf("missing_detector: drained %d messages from buffer, expectedNext advanced to %d", len(drained), h.expectedNext)
		}
		out = append(out, drained...)

		// 更新 seenMax
		if h.expectedNext-1 > h.seenMax {
			h.seenMax = h.expectedNext - 1
		}

		h.ensureGapTimer(now)
		h.sweep(now)
		log.Printf("missing_detector: outputting %d messages, expectedNext=%d, bufferSize=%d", len(out), h.expectedNext, len(h.buffer))
		return out, nil
	}

	// 规则3：更小 - 丢弃重复
	if seq < h.expectedNext {
		log.Printf("missing_detector: [RULE-3 SMALLER] seq=%d < expectedNext=%d, dropping duplicate", seq, h.expectedNext)
		return nil, nil
	}

	// 规则2：更大 - 缓冲，判断是否触发 eager 补数
	gap := seq - h.expectedNext
	log.Printf("missing_detector: [RULE-2 GREATER] seq=%d > expectedNext=%d, gap=%d, buffering", seq, h.expectedNext, gap)

	h.buffer[seq] = append(h.buffer[seq], msg)
	if _, exists := h.firstSeen[seq]; !exists {
		h.firstSeen[seq] = now
	}
	if seq > h.seenMax {
		h.seenMax = seq
	}

	// 确保 expectedNext 有 firstSeen 记录
	h.ensureGapTimer(now)

	// 规则2 子规则：gap > eagerGap 触发补数（范围限制为 maxRange）
	if h.cfg.eagerGap > 0 && gap > h.cfg.eagerGap {
		backfillEnd := seq - 1
		backfillRange := backfillEnd - h.expectedNext + 1
		if h.cfg.maxRange > 0 && backfillRange > h.cfg.maxRange {
			backfillEnd = h.expectedNext + h.cfg.maxRange - 1
		}
		log.Printf("missing_detector: [EAGER-BACKFILL] gap=%d > eagerGap=%d, triggering backfill [%d, %d]", gap, h.cfg.eagerGap, h.expectedNext, backfillEnd)
		h.triggerBackfillRange(h.expectedNext, backfillEnd, now)
	}

	// 规则4 和规则5 互斥检查：优先软超时补数，如果不满足软超时再检查硬超时跳过
	softTimeoutTriggered := h.evaluateTimeouts(now)
	if !softTimeoutTriggered {
		h.evaluateBudget(now)
	}

	// 清理
	h.sweep(now)

	return nil, nil
}

func (h *MissingDetector) extractSequence(msg *types.Message) (int64, error) {
	if msg.Metadata == nil {
		return 0, fmt.Errorf("missing_detector: 缺少 metadata")
	}
	val, ok := msg.Metadata[h.sequenceField]
	if !ok {
		return 0, fmt.Errorf("missing_detector: metadata.%s 缺失", h.sequenceField)
	}
	seq, err := cfgToInt64(val)
	if err != nil {
		return 0, fmt.Errorf("missing_detector: metadata.%s 非整数: %v", h.sequenceField, err)
	}
	return seq, nil
}

func (h *MissingDetector) drainBuffer(_ time.Time) []*types.Message {
	out := make([]*types.Message, 0)
	for {
		msgs, ok := h.buffer[h.expectedNext]
		if !ok {
			break
		}
		delete(h.buffer, h.expectedNext)
		delete(h.firstSeen, h.expectedNext)
		out = append(out, msgs...)
		h.expectedNext++
	}
	if h.seenMax < h.expectedNext && len(h.buffer) == 0 {
		h.seenMax = h.expectedNext
	}
	return out
}

func (h *MissingDetector) ensureGapTimer(now time.Time) {
	if !h.initialized {
		return
	}
	if _, ok := h.firstSeen[h.expectedNext]; !ok {
		h.firstSeen[h.expectedNext] = now
	}
}

// 规则4：软超时 - 对 expectedNext 等待过久则主动补数
// 返回 true 表示触发了补数，此时不应再检查硬超时跳过
func (h *MissingDetector) evaluateTimeouts(now time.Time) bool {
	if !h.initialized || h.cfg.maxDelay <= 0 {
		return false
	}

	ts, ok := h.firstSeen[h.expectedNext]
	if !ok {
		return false
	}

	waitTime := now.Sub(ts)

	// 软超时触发补数
	if waitTime > h.cfg.maxDelay && waitTime <= h.cfg.hardTimeout {
		backfillEnd := h.expectedNext
		// 补数到 buffer 中最小的已知序号
		if len(h.buffer) > 0 {
			minBuffered, ok := h.minBufferedSeq()
			if ok && minBuffered > h.expectedNext {
				backfillEnd = minBuffered - 1
			}
		}
		// 限制 maxRange
		if h.cfg.maxRange > 0 && backfillEnd-h.expectedNext+1 > h.cfg.maxRange {
			backfillEnd = h.expectedNext + h.cfg.maxRange - 1
		}

		log.Printf("missing_detector: [RULE-4 SOFT-TIMEOUT] expectedNext=%d waiting for %dms > maxDelay=%dms, triggering backfill [%d, %d]",
			h.expectedNext, waitTime.Milliseconds(), h.cfg.maxDelay.Milliseconds(), h.expectedNext, backfillEnd)
		h.triggerBackfillRange(h.expectedNext, backfillEnd, now)
		return true // 已触发补数，不应再跳过
	}
	return false
}

// 规则5：超出实时预算 - 强制跳过缺口
func (h *MissingDetector) evaluateBudget(now time.Time) {
	if !h.initialized {
		return
	}

	shouldJump := false
	reason := ""
	target := uint64(0)

	// 检查硬超时
	if h.cfg.hardTimeout > 0 {
		if ts, ok := h.firstSeen[h.expectedNext]; ok {
			waitTime := now.Sub(ts)
			if waitTime > h.cfg.hardTimeout {
				shouldJump = true
				reason = fmt.Sprintf("hardTimeout (%dms > %dms)", waitTime.Milliseconds(), h.cfg.hardTimeout.Milliseconds())
				// 跳到 buffer 中最小的已知序号（如果存在）
				if len(h.buffer) > 0 {
					minBuffered, ok := h.minBufferedSeq()
					if ok {
						target = minBuffered
					}
				}
				if target == 0 || target <= h.expectedNext {
					target = h.expectedNext + 1
				}
			}
		}
	}

	// 检查 maxGap（只在没有硬超时时检查）
	if !shouldJump && h.cfg.maxGap > 0 && h.seenMax > h.expectedNext {
		if h.seenMax-h.expectedNext > h.cfg.maxGap {
			shouldJump = true
			reason = fmt.Sprintf("maxGap (%d - %d = %d > %d)", h.seenMax, h.expectedNext, h.seenMax-h.expectedNext, h.cfg.maxGap)
			target = h.computeJumpTarget()
		}
	}

	if shouldJump && target > h.expectedNext {
		log.Printf("missing_detector: [RULE-5 BUDGET-EXCEEDED] %s, jumping expectedNext %d -> %d", reason, h.expectedNext, target)
		h.advanceExpected(target, now)
	}
}

func (h *MissingDetector) computeJumpTarget() uint64 {
	target := h.expectedNext + 1
	if h.cfg.maxGap > 0 && h.seenMax > h.cfg.maxGap {
		cand := h.seenMax - h.cfg.maxGap
		if cand > target {
			target = cand
		}
	}
	if target <= h.expectedNext {
		target = h.expectedNext + 1
	}
	return target
}

func (h *MissingDetector) advanceExpected(target uint64, now time.Time) {
	if target <= h.expectedNext {
		return
	}
	// 删除被跳过区间的所有 buffer 和 firstSeen
	deletedCount := 0
	for seq := h.expectedNext; seq < target; seq++ {
		if _, ok := h.buffer[seq]; ok {
			delete(h.buffer, seq)
			deletedCount++
		}
		delete(h.firstSeen, seq)
	}
	log.Printf("missing_detector: advanced expectedNext %d -> %d, deleted %d buffered sequences", h.expectedNext, target, deletedCount)
	h.expectedNext = target
	h.ensureGapTimer(now)
}

func (h *MissingDetector) triggerBackfillRange(start, end uint64, now time.Time) {
	if h.backfillCh == nil || len(h.cfg.backfillOptions) == 0 {
		return
	}
	if end < start {
		return
	}

	// 限制 maxRange
	if h.cfg.maxRange > 0 {
		limit := start + h.cfg.maxRange - 1
		if limit < start {
			limit = math.MaxUint64
		}
		if end > limit {
			end = limit
		}
	}

	// 去重：相同范围的补数请求在 cooldown 内只发一次
	if h.lastBackfill.start == start && h.lastBackfill.end == end &&
		now.Sub(h.lastBackfill.at) < h.cooldownDuration() {
		log.Printf("missing_detector: backfill [%d, %d] skipped (cooldown %dms)", start, end, now.Sub(h.lastBackfill.at).Milliseconds())
		return
	}

	opts := make([]types.BackfillOption, len(h.cfg.backfillOptions))
	for i, opt := range h.cfg.backfillOptions {
		paramsCopy := make(map[string]any, len(opt.Params))
		for k, v := range opt.Params {
			paramsCopy[k] = v
		}
		opts[i] = types.BackfillOption{
			Transport: opt.Transport,
			RPCMethod: opt.RPCMethod,
			Params:    paramsCopy,
		}
	}

	if start > math.MaxInt64 || end > math.MaxInt64 {
		log.Printf("missing_detector: backfill range [%d, %d] exceeds int64, skip", start, end)
		return
	}

	cmd := types.BackfillCmd{
		Start:   int64(start),
		End:     int64(end),
		Options: opts,
	}

	log.Printf("missing_detector: sending backfill cmd [%d, %d] to channel", start, end)
	select {
	case h.backfillCh <- cmd:
		h.lastBackfill = backfillRecord{start: start, end: end, at: now}
		log.Printf("missing_detector: backfill [%d, %d] sent successfully", start, end)
	default:
		// Channel 满时使用阻塞发送，但设置超时
		log.Printf("missing_detector: backfill channel full, attempting blocking send [%d, %d]", start, end)
		timer := time.NewTimer(100 * time.Millisecond)
		defer timer.Stop()
		select {
		case h.backfillCh <- cmd:
			h.lastBackfill = backfillRecord{start: start, end: end, at: now}
			log.Printf("missing_detector: backfill [%d, %d] sent (delayed)", start, end)
		case <-timer.C:
			log.Printf("missing_detector: backfill [%d, %d] DROPPED (channel timeout)", start, end)
		}
	}
}

func (h *MissingDetector) cooldownDuration() time.Duration {
	if h.cfg.hardTimeout > 0 {
		return h.cfg.hardTimeout
	}
	if h.cfg.maxDelay > 0 {
		return h.cfg.maxDelay
	}
	return 2 * time.Second
}

// 清理：删除过期桶 + 限制最大桶数量
func (h *MissingDetector) sweep(now time.Time) {
	if h.cfg.sweepInterval > 0 && now.Sub(h.lastSweep) < h.cfg.sweepInterval {
		return
	}
	h.lastSweep = now

	// 删除过期桶
	if h.cfg.bucketTTL > 0 {
		deletedTTL := 0
		for seq, t := range h.firstSeen {
			if now.Sub(t) > h.cfg.bucketTTL {
				delete(h.firstSeen, seq)
				delete(h.buffer, seq)
				deletedTTL++
			}
		}
		if deletedTTL > 0 {
			log.Printf("missing_detector: [SWEEP-TTL] deleted %d expired buckets (TTL=%dms)", deletedTTL, h.cfg.bucketTTL.Milliseconds())
		}
	}

	// 限制最大桶数量（从最小 seq 开始删）
	if h.cfg.maxBuckets > 0 && len(h.buffer) > h.cfg.maxBuckets {
		deletedMax := 0
		for len(h.buffer) > h.cfg.maxBuckets {
			seq, ok := h.minBufferedSeq()
			if !ok {
				break
			}
			// 尝试单点补数
			log.Printf("missing_detector: [SWEEP-MAX] buffer overflow (%d > %d), trying backfill seq=%d", len(h.buffer), h.cfg.maxBuckets, seq)
			h.triggerBackfillRange(seq, seq, now)
			delete(h.buffer, seq)
			delete(h.firstSeen, seq)
			deletedMax++
		}
		if deletedMax > 0 {
			log.Printf("missing_detector: [SWEEP-MAX] deleted %d buckets to enforce maxBuckets=%d", deletedMax, h.cfg.maxBuckets)
		}
	}
}

func (h *MissingDetector) minBufferedSeq() (uint64, bool) {
	if len(h.buffer) == 0 {
		return 0, false
	}
	minSeq := uint64(math.MaxUint64)
	for seq := range h.buffer {
		if seq < minSeq {
			minSeq = seq
		}
	}
	if minSeq == math.MaxUint64 {
		return 0, false
	}
	return minSeq, true
}

func cfgString(m map[string]any, key, def string) string {
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

func cfgInt(m map[string]any, key string, def int) int {
	if m == nil {
		return def
	}
	if v, ok := m[key]; ok {
		switch vv := v.(type) {
		case int:
			return vv
		case int64:
			return int(vv)
		case float64:
			return int(vv)
		case json.Number:
			if i, err := vv.Int64(); err == nil {
				return int(i)
			}
		case string:
			if i, err := strconv.Atoi(vv); err == nil {
				return i
			}
		}
	}
	return def
}

func cfgBool(m map[string]any, key string, def bool) bool {
	if m == nil {
		return def
	}
	if v, ok := m[key]; ok {
		switch vv := v.(type) {
		case bool:
			return vv
		case string:
			switch strings.ToLower(vv) {
			case "true", "1", "yes":
				return true
			case "false", "0", "no":
				return false
			}
		case int:
			return vv != 0
		case int64:
			return vv != 0
		case float64:
			return vv != 0
		}
	}
	return def
}

func cfgToInt64(v interface{}) (int64, error) {
	switch vv := v.(type) {
	case int:
		return int64(vv), nil
	case int64:
		return vv, nil
	case float64:
		return int64(vv), nil
	case string:
		if strings.HasPrefix(vv, "0x") || strings.HasPrefix(vv, "0X") {
			return strconv.ParseInt(vv[2:], 16, 64)
		}
		return strconv.ParseInt(vv, 10, 64)
	case json.Number:
		return vv.Int64()
	default:
		return 0, fmt.Errorf("unsupported type %T", v)
	}
}

func cfgToUint64(v interface{}) (uint64, error) {
	val, err := cfgToInt64(v)
	if err != nil {
		return 0, err
	}
	if val < 0 {
		return 0, fmt.Errorf("negative value %d", val)
	}
	return uint64(val), nil
}
