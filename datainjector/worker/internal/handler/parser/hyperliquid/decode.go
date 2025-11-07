package hyperliquid

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/util"
)

// DecodeOrderbookMessage normalises a Hyperliquid orderbook payload.
func DecodeOrderbookMessage(raw []byte) (*OrderbookSnapshot, error) {
	var root map[string]any
	if err := json.Unmarshal(raw, &root); err != nil {
		return nil, err
	}
	channel := strings.ToLower(util.ToString(root["channel"]))
	if channel == "pong" || (channel == "" && strings.ToLower(util.ToString(root["type"])) == "pong") {
		return nil, nil
	}
	data := util.ExtractMap(root, "data")
	if data == nil {
		data = root
	}
	if len(data) == 0 {
		return nil, fmt.Errorf("hyperliquid orderbook: missing data field")
	}
	snapshot := &OrderbookSnapshot{
		Coin:      strings.ToUpper(util.ToString(data["coin"])),
		Time:      util.ToInt64(data["time"]),
		Sequence:  util.ToInt64(data["sequence"]),
		RawLevels: data["levels"],
	}
	if snapshot.Sequence == 0 {
		snapshot.Sequence = util.ToInt64(data["seq"])
	}
	if snapshot.Sequence == 0 {
		snapshot.Sequence = util.ToInt64(data["version"])
	}
	if snapshot.Sequence == 0 {
		snapshot.Sequence = snapshot.Time
	}

	bids, asks := extractLevels(data)
	if len(bids) == 0 && len(asks) == 0 {
		// subscription ack or heartbeat message, ignore silently
		return nil, nil
	}
	snapshot.Bids = bids
	snapshot.Asks = asks
	return snapshot, nil
}

// DecodeTradeMessage parses trade payloads. Hyperliquid batches trades in an array.
func DecodeTradeMessage(raw []byte) ([]*TradeMessage, error) {
	var root map[string]any
	if err := json.Unmarshal(raw, &root); err != nil {
		return nil, err
	}
	channel := strings.ToLower(util.ToString(root["channel"]))
	if channel == "pong" || (channel == "" && strings.ToLower(util.ToString(root["type"])) == "pong") {
		return nil, nil
	}
	var trades []*TradeMessage

	switch data := root["data"].(type) {
	case []interface{}:
		for _, item := range data {
			if m, ok := item.(map[string]any); ok {
				if trade := buildTradeMessage(m); trade != nil {
					trades = append(trades, trade)
				}
			}
		}
	case map[string]any:
		if trade := buildTradeMessage(data); trade != nil {
			trades = append(trades, trade)
		}
	case nil:
		// no payload
	default:
		// unexpected type, ignore
	}

	// Some responses may embed trades directly at root without data key.
	if len(trades) == 0 {
		if trade := buildTradeMessage(root); trade != nil && trade.Coin != "" {
			trades = append(trades, trade)
		}
	}

	return trades, nil
}

func buildTradeMessage(data map[string]any) *TradeMessage {
	if len(data) == 0 {
		return nil
	}
	trade := &TradeMessage{
		Coin:    strings.ToUpper(util.ToString(data["coin"])),
		Side:    strings.ToLower(util.ToString(data["side"])),
		Px:      util.ToString(data["px"]),
		Sz:      util.ToString(data["sz"]),
		Time:    util.ToInt64(data["time"]),
		TID:     util.ToString(data["tid"]),
		Seq:     util.ToInt64(data["sequence"]),
		RawData: data,
	}
	if trade.Coin == "" {
		return nil
	}
	if trade.Seq == 0 {
		trade.Seq = util.ToInt64(data["seq"])
	}
	if trade.Seq == 0 {
		trade.Seq = trade.Time
	}
	return trade
}

// DecodeAssetCtxMessage parses active asset context websocket payloads.
func DecodeAssetCtxMessage(raw []byte) (*AssetContext, error) {
	var root map[string]any
	if err := json.Unmarshal(raw, &root); err != nil {
		return nil, err
	}
	channel := strings.ToLower(util.ToString(root["channel"]))
	if channel == "pong" || (channel == "" && strings.ToLower(util.ToString(root["type"])) == "pong") {
		return nil, nil
	}
	data := util.ExtractMap(root, "data")
	if data == nil {
		data = root
	}
	if len(data) == 0 {
		return nil, fmt.Errorf("hyperliquid asset_ctx: missing data field")
	}
	ctxMap := util.ExtractMap(data, "ctx")
	if ctxMap == nil {
		ctxMap = util.ExtractMap(data, "assetCtx")
	}
	if ctxMap == nil {
		ctxMap = data
	}
	ctx := &AssetContext{
		Coin:   strings.ToUpper(util.ToString(data["coin"])),
		RawCtx: ctxMap,
	}
	fillCtxFromMap(ctx, ctxMap)
	if ctx.Timestamp == 0 {
		ctx.Timestamp = util.ToInt64(data["time"])
	}
	return ctx, nil
}

// DecodeMetaAndAssetCtxs normalises REST metaAndAssetCtxs response.
func DecodeMetaAndAssetCtxs(raw []byte) ([]*AssetContext, error) {
	var root interface{}
	if err := json.Unmarshal(raw, &root); err != nil {
		return nil, err
	}
	var out []*AssetContext

	switch v := root.(type) {
	case []interface{}:
		if len(v) >= 2 {
			universeNames := extractUniverseNames(v[0])
			if ctxArr, ok := v[1].([]interface{}); ok {
				for idx, item := range ctxArr {
					ctx, err := decodeAssetCtxFromAny(item)
					if err != nil {
						return nil, err
					}
					if ctx == nil {
						ctx = &AssetContext{}
					}
					if ctx.Coin == "" && idx < len(universeNames) {
						ctx.Coin = universeNames[idx]
					}
					if ctx.Coin == "" {
						continue
					}
					out = append(out, ctx)
				}
			}
			return out, nil
		}
		for _, item := range v {
			ctx, err := decodeAssetCtxFromAny(item)
			if err != nil {
				return nil, err
			}
			if ctx != nil {
				out = append(out, ctx)
			}
		}
	case map[string]any:
		if items, ok := v["ctxs"].([]interface{}); ok {
			for _, item := range items {
				ctx, err := decodeAssetCtxFromAny(item)
				if err != nil {
					return nil, err
				}
				if ctx != nil {
					out = append(out, ctx)
				}
			}
		}
		if coinMap, ok := v["ctxs"].(map[string]any); ok {
			for coin, rawCtx := range coinMap {
				ctx, err := decodeAssetCtxFromAny(rawCtx)
				if err != nil {
					return nil, err
				}
				if ctx == nil {
					ctx = &AssetContext{}
				}
				if ctx.Coin == "" {
					ctx.Coin = strings.ToUpper(coin)
				}
				out = append(out, ctx)
			}
		}
		if assetCtxs, ok := v["assetCtxs"].(map[string]any); ok {
			for coin, rawCtx := range assetCtxs {
				ctx, err := decodeAssetCtxFromAny(rawCtx)
				if err != nil {
					return nil, err
				}
				if ctx == nil {
					ctx = &AssetContext{}
				}
				if ctx.Coin == "" {
					ctx.Coin = strings.ToUpper(coin)
				}
				out = append(out, ctx)
			}
		}
	default:
		return nil, fmt.Errorf("unsupported metaAndAssetCtxs payload: %T", v)
	}
	return out, nil
}

func decodeAssetCtxFromAny(value interface{}) (*AssetContext, error) {
	switch v := value.(type) {
	case nil:
		return nil, nil
	case map[string]any:
		ctx := &AssetContext{
			Coin:   strings.ToUpper(util.ToString(v["coin"])),
			RawCtx: extractCtxMap(v),
		}
		if ctx.RawCtx == nil {
			ctx.RawCtx = v
		}
		fillCtxFromMap(ctx, ctx.RawCtx)
		return ctx, nil
	case []interface{}:
		var coin string
		var ctxMap map[string]any
		for _, item := range v {
			switch iv := item.(type) {
			case string:
				if coin == "" {
					coin = strings.ToUpper(strings.TrimSpace(iv))
				}
			case map[string]any:
				if ctxMap == nil || len(ctxMap) == 0 {
					ctxMap = extractCtxMap(iv)
					if ctxMap == nil {
						ctxMap = iv
					}
				}
				if coin == "" {
					coin = strings.ToUpper(util.ToString(iv["coin"]))
				}
			default:
				if ctxMap == nil {
					ctxMap = toMap(iv)
				}
			}
		}
		if ctxMap == nil {
			return nil, nil
		}
		ctx := &AssetContext{
			Coin:   coin,
			RawCtx: ctxMap,
		}
		fillCtxFromMap(ctx, ctxMap)
		if ctx.Coin == "" {
			ctx.Coin = strings.ToUpper(util.ToString(ctxMap["coin"]))
		}
		return ctx, nil
	default:
		return nil, fmt.Errorf("asset ctx entry is %T", value)
	}
}

func extractCtxMap(src map[string]any) map[string]any {
	if src == nil {
		return nil
	}
	if ctxMap := util.ExtractMap(src, "ctx"); ctxMap != nil {
		return ctxMap
	}
	if ctxMap := util.ExtractMap(src, "assetCtx"); ctxMap != nil {
		return ctxMap
	}
	return nil
}

func extractUniverseNames(meta interface{}) []string {
	m, ok := meta.(map[string]any)
	if !ok {
		return nil
	}
	universe, ok := m["universe"].([]interface{})
	if !ok {
		return nil
	}
	names := make([]string, 0, len(universe))
	for _, item := range universe {
		if entry, ok := item.(map[string]any); ok {
			name := strings.ToUpper(strings.TrimSpace(util.ToString(entry["name"])))
			if name != "" {
				names = append(names, name)
			}
		}
	}
	return names
}

func fillCtxFromMap(ctx *AssetContext, data map[string]any) {
	if ctx == nil || data == nil {
		return
	}
	ctx.MarkPx = util.ToString(data["markPx"])
	ctx.OraclePx = util.ToString(data["oraclePx"])
	ctx.OpenInterest = util.ToString(data["openInterest"])
	ctx.Funding = util.ToString(data["funding"])
	ctx.DayVolume = util.ToString(data["dayNtlVlm"])
	ctx.PrevDayPx = util.ToString(data["prevDayPx"])
	ctx.MidPx = util.ToString(data["midPx"])
	ctx.Timestamp = util.ToInt64(data["time"])
	if ctx.Timestamp == 0 {
		ctx.Timestamp = util.ToInt64(data["timestamp"])
	}
	if ctx.Coin != "" {
		ctx.Coin = strings.ToUpper(ctx.Coin)
	}
}

func extractLevels(data map[string]any) ([][]string, [][]string) {
	if data == nil {
		return nil, nil
	}
	var bids [][]string
	var asks [][]string

	if levels, ok := data["levels"]; ok {
		switch lv := levels.(type) {
		case map[string]any:
			bids = append(bids, parseLevelSlice(lv["bids"])...)
			asks = append(asks, parseLevelSlice(lv["asks"])...)
		case []interface{}:
			if len(lv) > 0 {
				bids = append(bids, parseLevelSlice(lv[0])...)
			}
			if len(lv) > 1 {
				asks = append(asks, parseLevelSlice(lv[1])...)
			}
		}
	}

	if len(bids) == 0 {
		bids = parseLevelSlice(data["bids"])
	}
	if len(asks) == 0 {
		asks = parseLevelSlice(data["asks"])
	}
	return bids, asks
}

func parseLevelSlice(raw interface{}) [][]string {
	switch v := raw.(type) {
	case nil:
		return nil
	case []interface{}:
		out := make([][]string, 0, len(v))
		for _, item := range v {
			switch lv := item.(type) {
			case []interface{}:
				out = append(out, extractLevelFromArray(lv))
			case map[string]any:
				out = append(out, extractLevelFromMap(lv))
			default:
				// ignore
			}
		}
		return filterEmptyLevels(out)
	default:
		return nil
	}
}

func extractLevelFromArray(values []interface{}) []string {
	if len(values) == 0 {
		return nil
	}
	price := util.ToString(values[0])
	size := ""
	if len(values) > 1 {
		size = util.ToString(values[1])
	}
	return []string{price, size}
}

func extractLevelFromMap(m map[string]any) []string {
	if len(m) == 0 {
		return nil
	}
	price := util.ToString(m["px"])
	if price == "" {
		price = util.ToString(m["price"])
	}
	size := util.ToString(m["sz"])
	if size == "" {
		size = util.ToString(m["size"])
	}
	if price == "" && size == "" {
		return nil
	}
	return []string{price, size}
}

func filterEmptyLevels(levels [][]string) [][]string {
	if len(levels) == 0 {
		return nil
	}
	out := make([][]string, 0, len(levels))
	for _, lv := range levels {
		if len(lv) < 2 {
			continue
		}
		if strings.TrimSpace(lv[0]) == "" {
			continue
		}
		out = append(out, lv[:2])
	}
	return out
}

func toMap(value interface{}) map[string]any {
	if value == nil {
		return nil
	}
	if m, ok := value.(map[string]any); ok {
		return m
	}
	if m, ok := value.(map[string]interface{}); ok {
		return m
	}
	return nil
}
