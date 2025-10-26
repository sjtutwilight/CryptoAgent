package handler

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/types"
)

type BinanceKlineNormalizer struct {
	Exchange       string
	DropUnfinished bool
	IncludeRaw     bool
}

func init() {
	Register("binance_kline_normalizer", func(cfg map[string]any) (Handler, error) {
		handler := &BinanceKlineNormalizer{
			Exchange:       getString(cfg, "exchange", "binance"),
			DropUnfinished: getBool(cfg, "drop_unfinished", true),
			IncludeRaw:     getBool(cfg, "include_raw_payload", false),
		}
		return handler, nil
	})
}

func (b *BinanceKlineNormalizer) Handle(msg *types.Message) ([]*types.Message, error) {
	if msg == nil || len(msg.Payload) == 0 {
		return nil, nil
	}

	var root map[string]any
	if err := json.Unmarshal(msg.Payload, &root); err != nil {
		return nil, fmt.Errorf("binance_kline_normalizer: invalid json payload: %w", err)
	}

	payload := extractMap(root, "data")
	if payload == nil {
		payload = root
	}

	klineSource := extractMap(payload, "k")
	if klineSource == nil {
		klineSource = extractMap(payload, "kline")
	}
	if klineSource == nil {
		// 非kline数据或订阅确认消息，直接忽略
		return nil, nil
	}

	if b.DropUnfinished {
		if finished, ok := toBool(klineSource["x"]); ok && !finished {
			return nil, nil
		}
	}

	exchange := firstNonEmpty(toString(payload["exchange"]), toString(payload["datasource_id"]), b.Exchange)
	symbol := firstNonEmpty(
		toString(payload["symbol"]),
		toString(payload["s"]),
		toString(klineSource["s"]),
	)
	if symbol == "" {
		symbol = "UNKNOWN"
	}

	interval := firstNonEmpty(
		toString(payload["interval"]),
		toString(klineSource["i"]),
	)
	if interval == "" {
		interval = "UNKNOWN"
	}

	eventTime := firstNonZero(
		toInt64(payload["eventTime"]),
		toInt64(payload["E"]),
		toInt64(root["eventTime"]),
		toInt64(root["E"]),
	)

	ingestTime := firstNonZero(
		toInt64(payload["ingestTime"]),
		toInt64(root["ingestTime"]),
	)
	if ingestTime == 0 {
		ingestTime = time.Now().UnixMilli()
	}

	startTime := toInt64(klineSource["t"])
	closeTime := toInt64(klineSource["T"])
	openPrice := toString(klineSource["o"])
	closePrice := toString(klineSource["c"])
	highPrice := toString(klineSource["h"])
	lowPrice := toString(klineSource["l"])
	baseVolume := toString(klineSource["v"])
	quoteVolume := firstNonEmpty(toString(klineSource["q"]), toString(klineSource["Q"]))
	tradeCount := toInt(klineSource["n"])
	closed, _ := toBool(klineSource["x"])

	if baseVolume == "" {
		baseVolume = "0"
	}
	if quoteVolume == "" {
		quoteVolume = "0"
	}

	normalized := map[string]any{
		"exchange":   exchange,
		"symbol":     symbol,
		"interval":   interval,
		"eventTime":  eventTime,
		"ingestTime": ingestTime,
		"kline": map[string]any{
			"startTime":   startTime,
			"closeTime":   closeTime,
			"openPrice":   openPrice,
			"closePrice":  closePrice,
			"highPrice":   highPrice,
			"lowPrice":    lowPrice,
			"baseVolume":  baseVolume,
			"quoteVolume": quoteVolume,
			"tradeCount":  tradeCount,
			"closed":      closed,
		},
	}

	if b.IncludeRaw {
		normalized["raw"] = root
	}

	payloadBytes, err := json.Marshal(normalized)
	if err != nil {
		return nil, fmt.Errorf("binance_kline_normalizer: marshal normalized payload failed: %w", err)
	}

	meta := copyMap(msg.Metadata)
	meta["exchange"] = exchange
	meta["symbol"] = symbol
	meta["interval"] = interval
	if eventTime != 0 {
		meta["event_time"] = eventTime
	}
	if startTime != 0 {
		meta["start_time"] = startTime
	}
	if closeTime != 0 {
		meta["close_time"] = closeTime
	}

	msg.Metadata = meta
	msg.Payload = payloadBytes
	return []*types.Message{msg}, nil
}

func extractMap(src map[string]any, keys ...string) map[string]any {
	for _, key := range keys {
		if key == "" {
			continue
		}
		if v, ok := src[key]; ok {
			if m, ok := v.(map[string]any); ok {
				return m
			}
		}
	}
	return nil
}

// func firstNonEmpty(values ...string) string {
// 	for _, v := range values {
// 		if strings.TrimSpace(v) != "" {
// 			return v
// 		}
// 	}
// 	return ""
// }

func firstNonZero(values ...int64) int64 {
	for _, v := range values {
		if v != 0 {
			return v
		}
	}
	return 0
}

func toString(v interface{}) string {
	switch vv := v.(type) {
	case string:
		return vv
	case fmt.Stringer:
		return vv.String()
	case float64:
		return strings.TrimRight(strings.TrimRight(fmt.Sprintf("%.12f", vv), "0"), ".")
	case json.Number:
		return vv.String()
	case nil:
		return ""
	default:
		return fmt.Sprintf("%v", vv)
	}
}

func toInt64(v interface{}) int64 {
	switch vv := v.(type) {
	case int64:
		return vv
	case int:
		return int64(vv)
	case float64:
		return int64(vv)
	case json.Number:
		if i, err := vv.Int64(); err == nil {
			return i
		}
	case string:
		if vv == "" {
			return 0
		}
		if i, err := json.Number(vv).Int64(); err == nil {
			return i
		}
	}
	return 0
}

func toInt(v interface{}) int {
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
		if vv == "" {
			return 0
		}
		if i, err := json.Number(vv).Int64(); err == nil {
			return int(i)
		}
	}
	return 0
}

func toBool(v interface{}) (bool, bool) {
	switch vv := v.(type) {
	case bool:
		return vv, true
	case string:
		switch strings.ToLower(strings.TrimSpace(vv)) {
		case "true", "1", "yes":
			return true, true
		case "false", "0", "no":
			return false, true
		}
	case int:
		return vv != 0, true
	case int64:
		return vv != 0, true
	case float64:
		return vv != 0, true
	}
	return false, false
}

func copyMap(src map[string]any) map[string]any {
	if len(src) == 0 {
		return map[string]any{}
	}
	dst := make(map[string]any, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func getBool(m map[string]any, key string, def bool) bool {
	if m == nil {
		return def
	}
	if v, ok := m[key]; ok {
		b, ok := toBool(v)
		if ok {
			return b
		}
	}
	return def
}
