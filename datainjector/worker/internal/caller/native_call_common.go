package caller

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/types"
	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/util"
)

type wsRPCResponse struct {
	result json.RawMessage
	err    error
}

func parseHexUint64(hexStr string) (uint64, error) {
	s := strings.TrimPrefix(strings.ToLower(hexStr), "0x")
	if s == "" {
		return 0, fmt.Errorf("invalid hex: %s", hexStr)
	}
	return strconv.ParseUint(s, 16, 64)
}

func copyMetadata(src map[string]any) map[string]any {
	dst := make(map[string]any, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func wrapBlockPayload(block map[string]any, meta map[string]any, method string) map[string]any {
	subscriptionID := util.ToString(meta["subscription"])
	if subscriptionID == "" {
		subscriptionID = defaultSubscriptionID(meta, method)
	}
	params := map[string]any{
		"subscription": subscriptionID,
		"result":       block,
	}
	if backfill, ok := meta["is_backfill"].(bool); ok && backfill {
		params["is_backfill"] = true
	}
	return map[string]any{
		"jsonrpc": "2.0",
		"method":  "eth_subscription",
		"params":  params,
	}
}

func defaultSubscriptionID(meta map[string]any, method string) string {
	if id := util.ToString(meta["datasource_id"]); id != "" {
		return fmt.Sprintf("%s#%s", id, method)
	}
	if chainID := util.ToString(meta["chain_id"]); chainID != "" {
		return fmt.Sprintf("chain#%s#%s", chainID, method)
	}
	return fmt.Sprintf("backfill#%s", method)
}

func buildMessagesFromResult(method string, result json.RawMessage, base map[string]any) ([]*types.Message, error) {
	if len(result) == 0 {
		return nil, nil
	}

	var decoded interface{}
	if err := json.Unmarshal(result, &decoded); err != nil {
		payload := make([]byte, len(result))
		copy(payload, result)
		meta := copyMetadata(base)
		return []*types.Message{{Metadata: meta, Payload: payload}}, nil
	}

	switch v := decoded.(type) {
	case []interface{}:
		msgs := make([]*types.Message, 0, len(v))
		for _, item := range v {
			payload, meta := buildPayloadItem(item, base, method)
			if payload == nil {
				continue
			}
			msgs = append(msgs, &types.Message{Metadata: meta, Payload: payload})
		}
		return msgs, nil
	case []map[string]any:
		msgs := make([]*types.Message, 0, len(v))
		for _, item := range v {
			payload, meta := buildPayloadItem(item, base, method)
			if payload == nil {
				continue
			}
			msgs = append(msgs, &types.Message{Metadata: meta, Payload: payload})
		}
		return msgs, nil
	case map[string]any:
		payload, meta := buildPayloadItem(v, base, method)
		if payload == nil {
			return nil, nil
		}
		return []*types.Message{{Metadata: meta, Payload: payload}}, nil
	default:
		payload, err := json.Marshal(v)
		if err != nil {
			return []*types.Message{{Metadata: copyMetadata(base), Payload: result}}, nil
		}
		return []*types.Message{{Metadata: copyMetadata(base), Payload: payload}}, nil
	}
}

func buildPayloadItem(item interface{}, base map[string]any, method string) ([]byte, map[string]any) {
	if item == nil {
		return nil, nil
	}
	meta := copyMetadata(base)
	var payload []byte
	switch blk := item.(type) {
	case map[string]any:
		// 优先从 block 对象提取 block_number
		if numHex, ok := blk["number"].(string); ok {
			if n, err := parseHexUint64(numHex); err == nil {
				meta["block_number"] = int64(n)
			}
		}
		if hash, ok := blk["hash"].(string); ok && hash != "" {
			meta["block_hash"] = hash
		}
		// 兜底：从 block_query 或其他字段提取
		ensureBlockNumber(meta, blk)

		// 补数消息必须有 block_number
		if _, ok := meta["block_number"]; !ok {
			log.Printf("[buildPayloadItem] WARNING: block_number missing! base=%v, block=%v", base, blk)
		}

		wrapper := wrapBlockPayload(blk, meta, method)
		var err error
		payload, err = json.Marshal(wrapper)
		if err != nil {
			return nil, nil
		}
	case []byte:
		payload = blk
	default:
		var err error
		payload, err = json.Marshal(blk)
		if err != nil {
			return nil, nil
		}
	}
	return payload, meta
}

func ensureBlockNumber(meta map[string]any, block map[string]any) {
	if _, ok := meta["block_number"]; ok {
		return
	}
	if numRaw, ok := block["number"]; ok {
		switch v := numRaw.(type) {
		case string:
			if n, err := parseHexUint64(v); err == nil {
				meta["block_number"] = int64(n)
				return
			}
		case float64:
			meta["block_number"] = int64(v)
			return
		case int:
			meta["block_number"] = int64(v)
			return
		case int64:
			meta["block_number"] = v
			return
		}
	}
	if bq, ok := meta["block_query"]; ok {
		switch v := bq.(type) {
		case int64:
			meta["block_number"] = v
		case int:
			meta["block_number"] = int64(v)
		case float64:
			meta["block_number"] = int64(v)
		case string:
			if strings.HasPrefix(v, "0x") || strings.HasPrefix(v, "0X") {
				if n, err := strconv.ParseInt(v[2:], 16, 64); err == nil {
					meta["block_number"] = n
				}
			} else if n, err := strconv.ParseInt(v, 10, 64); err == nil {
				meta["block_number"] = n
			}
		}
	}
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

func getIntValue(m map[string]any, key string, def int) int {
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
		case string:
			if i, err := strconv.Atoi(vv); err == nil {
				return i
			}
		}
	}
	return def
}

func getBoolValue(m map[string]any, key string, def bool) bool {
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

func isBackfillMethod(method string) bool {
	switch method {
	case "eth_getBlockRange", "eth_getBlockByNumber":
		return true
	default:
		return false
	}
}

func normalizeRawJSON(value interface{}) ([]byte, error) {
	switch v := value.(type) {
	case nil:
		return nil, nil
	case []byte:
		return v, nil
	case json.RawMessage:
		return []byte(v), nil
	case string:
		return []byte(v), nil
	default:
		return json.Marshal(v)
	}
}

func getStringSlice(m map[string]any, key string) []string {
	if m == nil {
		return nil
	}
	v, ok := m[key]
	if !ok || v == nil {
		return nil
	}
	switch vv := v.(type) {
	case []string:
		out := make([]string, 0, len(vv))
		for _, item := range vv {
			if strings.TrimSpace(item) != "" {
				out = append(out, strings.TrimSpace(item))
			}
		}
		return out
	case []interface{}:
		out := make([]string, 0, len(vv))
		for _, item := range vv {
			str := util.ToString(item)
			if strings.TrimSpace(str) != "" {
				out = append(out, strings.TrimSpace(str))
			}
		}
		if len(out) == 0 {
			return nil
		}
		return out
	case string:
		if strings.TrimSpace(vv) == "" {
			return nil
		}
		if strings.Contains(vv, ",") {
			parts := strings.Split(vv, ",")
			out := make([]string, 0, len(parts))
			for _, item := range parts {
				if s := strings.TrimSpace(item); s != "" {
					out = append(out, s)
				}
			}
			if len(out) == 0 {
				return nil
			}
			return out
		}
		return []string{strings.TrimSpace(vv)}
	default:
		return nil
	}
}

func mergeStringMap(dst map[string]string, src map[string]any) map[string]string {
	if src == nil {
		return dst
	}
	if dst == nil {
		dst = make(map[string]string, len(src))
	}
	for k, v := range src {
		if s, ok := v.(string); ok {
			dst[k] = s
		}
	}
	return dst
}

func mergeAnyMap(dst map[string]any, src map[string]any) map[string]any {
	if src == nil {
		return dst
	}
	if dst == nil {
		dst = make(map[string]any, len(src))
	}
	for k, v := range src {
		dst[k] = v
	}
	return dst
}
