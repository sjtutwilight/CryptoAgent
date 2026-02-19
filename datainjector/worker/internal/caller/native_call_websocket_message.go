package caller

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/observability/logging"
	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/util"
)

func (w *WebSocketCall) receiveMessages() {
	source := w.wsClient.MessageChan()
	if w.sharedMessageCh != nil {
		source = w.sharedMessageCh
	}
	for data := range source {
		w.refreshMessageBackpressure(source)
		if err := w.handleIncomingMessage(data); err != nil {
			logging.Warn(context.Background(), logging.EventWSMessageProcessErr, "process websocket message failed", logging.Fields{
				"error": err.Error(),
			})
		}
	}
}

func (w *WebSocketCall) handleIncomingMessage(data []byte) error {
	switch w.messageFormat {
	case "binance":
		return w.handleBinanceMessage(data)
	case "hyperliquid":
		return w.handleHyperliquidMessage(data)
	default:
		return w.handleJSONRPCMessage(data)
	}
}

func (w *WebSocketCall) handleJSONRPCMessage(data []byte) error {
	var base struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      json.RawMessage `json:"id"`
		Method  string          `json:"method"`
		Params  json.RawMessage `json:"params"`
		Result  json.RawMessage `json:"result"`
		Error   *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}

	if err := json.Unmarshal(data, &base); err != nil {
		return fmt.Errorf("解析 websocket 数据失败: %w", err)
	}

	if len(base.ID) > 0 {
		if w.deliverPendingResponse(base.ID, base.Result, base.Error) {
			return nil
		}
		reqID := rawIDToString(base.ID)
		if w.shareByEndpoint && reqID != "" && !w.isPendingSubscribeRequestID(reqID) {
			// 共享连接下，忽略其他 caller 的 RPC 响应，避免串流与误报。
			return nil
		}
	}

	if base.Error != nil {
		return fmt.Errorf("websocket 返回错误: code=%d msg=%s", base.Error.Code, base.Error.Message)
	}

	if base.Method == w.notifyMethod {
		var params struct {
			Subscription string          `json:"subscription"`
			Result       json.RawMessage `json:"result"`
		}
		if err := json.Unmarshal(base.Params, &params); err != nil {
			return fmt.Errorf("解析订阅通知失败: %w", err)
		}
		if w.shouldDropBySubscription(params.Subscription) {
			return nil
		}

		meta := w.baseMetadata()
		if params.Subscription != "" {
			meta["subscription"] = params.Subscription
		}

		var payload interface{}
		if len(params.Result) > 0 {
			if err := json.Unmarshal(params.Result, &payload); err != nil {
				logging.Warn(context.Background(), logging.EventWSSubscribeAckParse, "parse subscription result failed", logging.Fields{
					"error": err.Error(),
				})
			}
		}
		w.applyResultMetadata(meta, payload)
		w.bufferMessage(meta, data)
		return nil
	}

	if len(base.Result) > 0 {
		var subID string
		if err := json.Unmarshal(base.Result, &subID); err == nil && subID != "" {
			reqID := rawIDToString(base.ID)
			if w.recordSubscriptionAck(reqID, subID) {
				logging.Info(context.Background(), logging.EventWSSubscribeAck, "subscription ack received", logging.Fields{
					"subscription_id": subID,
					"request_id":      reqID,
				})
			}
		}
	}

	return nil
}

func (w *WebSocketCall) handleBinanceMessage(data []byte) error {
	var payload interface{}
	if err := json.Unmarshal(data, &payload); err != nil {
		return fmt.Errorf("解析 websocket 数据失败: %w", err)
	}

	if obj, ok := payload.(map[string]any); ok {
		if _, hasResult := obj["result"]; hasResult && len(obj) <= 2 {
			// 订阅确认或心跳，直接忽略
			return nil
		}
	}

	meta := w.baseMetadata()
	if obj, ok := payload.(map[string]any); ok {
		if stream, ok := obj["stream"].(string); ok && stream != "" {
			if w.shouldDropByStream(stream) {
				return nil
			}
			meta["stream"] = stream
			if _, exists := meta["subscription"]; !exists {
				meta["subscription"] = stream
			}
		}
	}

	w.applyResultMetadata(meta, payload)
	if obj, ok := payload.(map[string]any); ok {
		if dataField, ok := obj["data"]; ok {
			w.applyResultMetadata(meta, dataField)
		}
	}

	w.bufferMessage(meta, data)
	return nil
}

func (w *WebSocketCall) handleHyperliquidMessage(data []byte) error {
	var payload interface{}
	if err := json.Unmarshal(data, &payload); err != nil {
		return fmt.Errorf("解析 websocket 数据失败: %w", err)
	}

	if obj, ok := payload.(map[string]any); ok {
		if channel, ok := obj["channel"].(string); ok {
			if w.shouldDropByStream(channel) {
				return nil
			}
			lower := strings.ToLower(channel)
			if lower == "pong" {
				return nil
			}
		}
	}

	meta := w.baseMetadata()
	if obj, ok := payload.(map[string]any); ok {
		if channel, ok := obj["channel"].(string); ok && channel != "" {
			meta["channel"] = channel
			meta["hyperliquid_channel"] = channel
			if _, exists := meta["subscription"]; !exists {
				meta["subscription"] = channel
			}
		}
		if typ, ok := obj["type"].(string); ok && typ != "" {
			meta["hyperliquid_type"] = typ
			if _, exists := meta["channel"]; !exists {
				meta["channel"] = typ
			}
		}
	}

	w.applyResultMetadata(meta, payload)
	if obj, ok := payload.(map[string]any); ok {
		if nested, ok := obj["data"]; ok {
			w.applyResultMetadata(meta, nested)
			if dataMap, ok := nested.(map[string]any); ok {
				if coin, ok := dataMap["coin"]; ok {
					meta["coin"] = util.ToString(coin)
				}
			}
		}
	}

	w.bufferMessage(meta, data)
	return nil
}

func (w *WebSocketCall) baseMetadata() map[string]any {
	w.mu.Lock()
	staticMetadata := w.staticMetadata
	chainID := w.chainID
	subscribeTopic := w.subscribeTopic
	messageBackpressure := w.messageBackpressure
	w.mu.Unlock()

	meta := make(map[string]any, len(staticMetadata)+3)
	if staticMetadata != nil {
		for k, v := range staticMetadata {
			meta[k] = v
		}
	}
	meta["protocol"] = "websocket"
	if chainID != "" {
		if _, ok := meta["chain_id"]; !ok {
			meta["chain_id"] = chainID
		}
	}
	if subscribeTopic != "" {
		if _, ok := meta["subscription"]; !ok {
			meta["subscription"] = subscribeTopic
		}
	}
	if messageBackpressure {
		meta["ws_backpressure"] = true
	}
	return meta
}

func (w *WebSocketCall) applyResultMetadata(meta map[string]any, payload interface{}) {
	if payload == nil {
		return
	}

	if len(w.resultMetadata) > 0 {
		for key, path := range w.resultMetadata {
			if value, ok := lookupJSONPath(payload, path); ok && value != nil {
				meta[key] = normalizeMetadataValue(value)
			}
		}
	}

	if !w.extractBlockMetadata {
		return
	}

	switch v := payload.(type) {
	case map[string]any:
		if _, ok := meta["block_hash"]; !ok {
			if hash, ok := v["hash"].(string); ok && hash != "" {
				meta["block_hash"] = hash
			}
		}
		ensureBlockNumber(meta, v)
	case []interface{}:
		for _, item := range v {
			if block, ok := item.(map[string]any); ok {
				if _, ok := meta["block_hash"]; !ok {
					if hash, ok := block["hash"].(string); ok && hash != "" {
						meta["block_hash"] = hash
					}
				}
				ensureBlockNumber(meta, block)
				if _, exists := meta["block_number"]; exists {
					break
				}
			}
		}
	}
}

func lookupJSONPath(data interface{}, path string) (interface{}, bool) {
	if path == "" {
		return data, true
	}
	current := data
	segments := strings.Split(path, ".")
	for _, segment := range segments {
		if segment == "" {
			continue
		}
		idx := -1
		name := segment
		if open := strings.Index(segment, "["); open >= 0 && strings.HasSuffix(segment, "]") {
			name = segment[:open]
			indexPart := segment[open+1 : len(segment)-1]
			if indexPart != "" {
				if n, err := strconv.Atoi(indexPart); err == nil {
					idx = n
				} else {
					return nil, false
				}
			}
		}

		if name != "" {
			obj, ok := current.(map[string]any)
			if !ok {
				return nil, false
			}
			next, ok := obj[name]
			if !ok {
				return nil, false
			}
			current = next
		}

		if idx >= 0 {
			arr, ok := current.([]interface{})
			if !ok || idx < 0 || idx >= len(arr) {
				return nil, false
			}
			current = arr[idx]
		}
	}
	return current, true
}

func normalizeMetadataValue(value interface{}) interface{} {
	switch v := value.(type) {
	case string:
		s := strings.TrimSpace(v)
		if strings.HasPrefix(s, "0x") || strings.HasPrefix(s, "0X") {
			if n, err := parseHexUint64(s); err == nil {
				return int64(n)
			}
		}
		if n, err := strconv.ParseInt(s, 10, 64); err == nil {
			return int64(n)
		}
		return s
	case float64:
		return int64(v)
	case int:
		return int64(v)
	case int64:
		return v
	default:
		return v
	}
}
