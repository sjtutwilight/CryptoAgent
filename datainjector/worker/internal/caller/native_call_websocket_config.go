package caller

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/gorilla/websocket"

	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/observability/logging"
	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/protocol"
	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/types"
)

func NewWebSocketCall(callerConfig map[string]any, params map[string]any) (*WebSocketCall, error) {
	url := getStringValue(callerConfig, "url", "")
	if url == "" {
		return nil, fmt.Errorf("websocket 缺少 url")
	}

	call := &WebSocketCall{
		msgBuffer:            make([]*types.Message, 0),
		msgBufferMaxMessages: 2048,
		msgBufferMaxBytes:    16 * 1024 * 1024,
		msgBufferDropPolicy:  "drop_oldest",
		wsPending:            make(map[string]chan wsRPCResponse),
		subscribeReqIDs:      make(map[string]struct{}),
		subscribeIDs:         make(map[string]struct{}),
		subscribeMethod:      "eth_subscribe",
		subscribeJSONRPC:     "2.0",
		messageFormat:        "jsonrpc",
		notifyMethod:         "eth_subscription",
		extractBlockMetadata: true,
		resultMetadata:       make(map[string]string),
	}

	callerParams, _ := params["caller_params"].(map[string]any)
	roleID := getStringValue(params, "role_id", "")
	if roleID == "" && callerParams != nil {
		roleID = getStringValue(callerParams, "role_id", "")
	}
	call.roleID = roleID

	if cid := getStringValue(callerConfig, "chain_id", ""); cid != "" {
		call.chainID = cid
	}
	if callerParams != nil {
		if cid := getStringValue(callerParams, "chain_id", ""); cid != "" && call.chainID == "" {
			call.chainID = cid
		}
	}

	if format := getStringValue(callerConfig, "message_format", ""); format != "" {
		call.messageFormat = strings.ToLower(format)
	}
	if callerParams != nil {
		if format := getStringValue(callerParams, "message_format", ""); format != "" {
			call.messageFormat = strings.ToLower(format)
		}
	}

	if nm := getStringValue(callerConfig, "notify_method", ""); nm != "" {
		call.notifyMethod = nm
	}
	if callerParams != nil {
		if nm := getStringValue(callerParams, "notify_method", ""); nm != "" {
			call.notifyMethod = nm
		}
	}

	if getBoolValue(callerConfig, "disable_block_metadata", false) {
		call.extractBlockMetadata = false
	}
	if callerParams != nil && getBoolValue(callerParams, "disable_block_metadata", false) {
		call.extractBlockMetadata = false
	}

	if streams := getStringSlice(callerConfig, "streams"); len(streams) > 0 {
		call.defaultStreams = streams
	}
	if callerParams != nil {
		if streams := getStringSlice(callerParams, "streams"); len(streams) > 0 {
			call.defaultStreams = streams
		}
	}

	if metaFields, ok := callerConfig["metadata_fields"].(map[string]any); ok {
		call.resultMetadata = mergeStringMap(call.resultMetadata, metaFields)
	}
	if callerParams != nil {
		if metaFields, ok := callerParams["metadata_fields"].(map[string]any); ok {
			call.resultMetadata = mergeStringMap(call.resultMetadata, metaFields)
		}
	}

	if staticMeta, ok := callerConfig["static_metadata"].(map[string]any); ok {
		call.staticMetadata = mergeAnyMap(call.staticMetadata, staticMeta)
	}
	if callerParams != nil {
		if staticMeta, ok := callerParams["static_metadata"].(map[string]any); ok {
			call.staticMetadata = mergeAnyMap(call.staticMetadata, staticMeta)
		}
	}

	if call.extractBlockMetadata {
		if _, ok := call.resultMetadata["block_number"]; !ok {
			call.resultMetadata["block_number"] = "number"
		}
		if _, ok := call.resultMetadata["block_hash"]; !ok {
			call.resultMetadata["block_hash"] = "hash"
		}
	} else {
		delete(call.resultMetadata, "block_number")
		delete(call.resultMetadata, "block_hash")
	}

	call.skipAckResults = getBoolValue(callerConfig, "skip_ack_results", call.messageFormat != "jsonrpc")
	if callerParams != nil {
		call.skipAckResults = getBoolValue(callerParams, "skip_ack_results", call.skipAckResults)
	}

	heartbeatMs := getIntValue(callerParams, "heartbeat_ms", 30000)
	heartbeatPayload := parseHeartbeatPayload(callerConfig, callerParams)
	heartbeatOpcode := websocket.PingMessage
	if hp, ok := callerConfig["heartbeat_opcode"].(string); ok {
		if op := parseHeartbeatOpcode(hp); op != 0 {
			heartbeatOpcode = op
		}
	}
	if callerParams != nil {
		if hp, ok := callerParams["heartbeat_opcode"].(string); ok {
			if op := parseHeartbeatOpcode(hp); op != 0 {
				heartbeatOpcode = op
			}
		}
	}
	reconnectBase := 2
	reconnectMax := 60
	minReconnectIntervalMs := 3000
	backoffJitterPercent := 30
	policyViolationThreshold := 3
	policyCooldownSeconds := 60
	subscribeDedupeWindowMs := 10000
	// 默认不启用 endpoint 级复用，避免不同订阅流在缺少可路由字段时发生串流。
	// 是否复用由配置显式开启（caller_params.reconnect.share_by_endpoint）。
	shareByEndpoint := false
	dispatchBufferSize := 1024
	backpressureHighPct := 80
	backpressureLowPct := 40
	msgBufferMaxMessages := call.msgBufferMaxMessages
	msgBufferMaxBytes := call.msgBufferMaxBytes
	msgBufferDropPolicy := call.msgBufferDropPolicy
	wsBoundedBuffer := getBoolValue(callerParams, "ws_bounded_buffer", true)
	if rcMap, ok := callerParams["reconnect"].(map[string]any); ok {
		reconnectBase = getIntValue(rcMap, "backoff_base_seconds", reconnectBase)
		reconnectMax = getIntValue(rcMap, "backoff_max_seconds", reconnectMax)
		minReconnectIntervalMs = getIntValue(rcMap, "min_interval_ms", minReconnectIntervalMs)
		backoffJitterPercent = getIntValue(rcMap, "jitter_percent", backoffJitterPercent)
		policyViolationThreshold = getIntValue(rcMap, "policy_violation_threshold", policyViolationThreshold)
		policyCooldownSeconds = getIntValue(rcMap, "policy_cooldown_seconds", policyCooldownSeconds)
		subscribeDedupeWindowMs = getIntValue(rcMap, "subscribe_dedupe_window_ms", subscribeDedupeWindowMs)
		shareByEndpoint = getBoolValue(rcMap, "share_by_endpoint", shareByEndpoint)
		dispatchBufferSize = getIntValue(rcMap, "dispatch_buffer_size", dispatchBufferSize)
		backpressureHighPct = getIntValue(rcMap, "backpressure_high_watermark_percent", backpressureHighPct)
		backpressureLowPct = getIntValue(rcMap, "backpressure_low_watermark_percent", backpressureLowPct)
	}
	if callerParams != nil {
		if v := getIntValue(callerParams, "buffer_max_messages", 0); v > 0 {
			msgBufferMaxMessages = v
		}
		if v := getIntValue(callerParams, "buffer_max_bytes", 0); v > 0 {
			msgBufferMaxBytes = v
		}
		if v := getStringValue(callerParams, "buffer_drop_policy", ""); v != "" {
			msgBufferDropPolicy = strings.ToLower(strings.TrimSpace(v))
		}
		if bufferMap, ok := callerParams["buffer"].(map[string]any); ok {
			if v := getIntValue(bufferMap, "max_messages", 0); v > 0 {
				msgBufferMaxMessages = v
			}
			if v := getIntValue(bufferMap, "max_bytes", 0); v > 0 {
				msgBufferMaxBytes = v
			}
			if v := getStringValue(bufferMap, "drop_policy", ""); v != "" {
				msgBufferDropPolicy = strings.ToLower(strings.TrimSpace(v))
			}
		}
	}
	if msgBufferMaxMessages <= 0 {
		msgBufferMaxMessages = 2048
	}
	if msgBufferMaxBytes <= 0 {
		msgBufferMaxBytes = 16 * 1024 * 1024
	}
	switch msgBufferDropPolicy {
	case "drop_oldest", "drop_newest":
	default:
		msgBufferDropPolicy = "drop_oldest"
	}
	if !wsBoundedBuffer {
		msgBufferMaxMessages = 0
		msgBufferMaxBytes = 0
	}
	if backpressureHighPct <= 0 || backpressureHighPct > 100 {
		backpressureHighPct = 80
	}
	if backpressureLowPct < 0 || backpressureLowPct >= backpressureHighPct {
		backpressureLowPct = backpressureHighPct / 2
	}

	if method := getStringValue(callerConfig, "subscribe_method", ""); method != "" {
		call.subscribeMethod = method
	}
	if version := getStringValue(callerConfig, "subscribe_jsonrpc", ""); version != "" {
		call.subscribeJSONRPC = version
	}
	if callerParams != nil {
		if method := getStringValue(callerParams, "subscribe_method", ""); method != "" {
			call.subscribeMethod = method
		}
		if version := getStringValue(callerParams, "subscribe_jsonrpc", ""); version != "" {
			call.subscribeJSONRPC = version
		}
	}

	cfg := protocol.WebSocketConfig{
		URL:                      url,
		HeartbeatMs:              heartbeatMs,
		BackoffBaseSeconds:       reconnectBase,
		BackoffMaxSeconds:        reconnectMax,
		MinReconnectIntervalMs:   minReconnectIntervalMs,
		BackoffJitterPercent:     backoffJitterPercent,
		PolicyViolationThreshold: policyViolationThreshold,
		PolicyCooldownSeconds:    policyCooldownSeconds,
		SubscribeDedupeWindowMs:  subscribeDedupeWindowMs,
		HeartbeatPayload:         heartbeatPayload,
		HeartbeatOpcode:          heartbeatOpcode,
	}

	endpointKey := buildEndpointShareKey(url, call.messageFormat)
	wsClient, hub, release, err := acquireWebSocketEndpoint(endpointKey, cfg, shareByEndpoint)
	if err != nil {
		return nil, err
	}
	call.wsClient = wsClient
	call.sharedHub = hub
	call.releaseWSClient = release
	call.shareByEndpoint = shareByEndpoint
	call.backpressureHighPct = backpressureHighPct
	call.backpressureLowPct = backpressureLowPct
	call.msgBufferMaxMessages = msgBufferMaxMessages
	call.msgBufferMaxBytes = msgBufferMaxBytes
	call.msgBufferDropPolicy = msgBufferDropPolicy
	call.wsBoundedBuffer = wsBoundedBuffer
	subID, subCh, err := hub.subscribe(dispatchBufferSize, nil)
	if err != nil {
		if release != nil {
			release()
		}
		return nil, err
	}
	call.sharedSubID = subID
	call.sharedMessageCh = subCh

	if err := call.wsClient.Connect(); err != nil {
		logging.Warn(context.Background(), logging.EventWSInitConnectError, "initial websocket connect failed, will retry", logging.Fields{
			"error": err.Error(),
		})
	}

	call.refreshSubscribeRequest(callerParams)
	call.syncSharedHubRoutes()
	go call.receiveMessages()

	logging.Info(context.Background(), logging.EventWSInit, "websocket client initialized", logging.Fields{
		"message_format":    call.messageFormat,
		"share_by_endpoint": call.shareByEndpoint,
		"endpoint_key":      endpointKey,
		"buffer_max_msgs":   call.msgBufferMaxMessages,
		"buffer_max_bytes":  call.msgBufferMaxBytes,
		"buffer_drop":       call.msgBufferDropPolicy,
		"role_id":           call.roleID,
		"ws_bounded_buffer": call.wsBoundedBuffer,
	})
	return call, nil
}

func (w *WebSocketCall) refreshSubscribeRequest(callerParams map[string]any) {
	w.mu.Lock()
	w.refreshSubscribeRequestLocked(callerParams)
	w.mu.Unlock()
}

func (w *WebSocketCall) refreshSubscribeRequestLocked(callerParams map[string]any) {
	w.useRawSubscribe = false
	w.subscribePayloads = nil
	w.skipSubscribe = false

	if callerParams == nil {
		callerParams = map[string]any{}
	}
	streams := getStringSlice(callerParams, "streams")
	if len(streams) == 0 && len(w.defaultStreams) > 0 {
		streams = append(streams, w.defaultStreams...)
	}
	w.setAllowedStreamsLocked(streams)

	if rawPayload, ok := callerParams["subscribe_raw"]; ok && rawPayload != nil {
		payloads, err := normalizeRawPayloads(rawPayload)
		if err != nil {
			logging.Warn(context.Background(), logging.EventWSSubscribeParseErr, "subscribe_raw parse failed", logging.Fields{
				"error": err.Error(),
			})
		} else if len(payloads) > 0 {
			w.useRawSubscribe = true
			w.subscribePayloads = payloads
			if topic := getStringValue(callerParams, "subscribe", ""); topic != "" {
				w.subscribeTopic = topic
			}
			return
		}
	}

	if strings.EqualFold(w.messageFormat, "binance") {
		streams := getStringSlice(callerParams, "streams")
		if len(streams) == 0 && len(w.defaultStreams) > 0 {
			streams = append(streams, w.defaultStreams...)
		}
		if len(streams) == 0 {
			if topic := getStringValue(callerParams, "subscribe", ""); topic != "" {
				streams = append(streams, topic)
			}
		}
		if len(streams) > 0 {
			w.setAllowedStreamsLocked(streams)
			method := getStringValue(callerParams, "subscribe_method", "")
			if method == "" {
				method = "SUBSCRIBE"
			}
			var requestID interface{}
			if val, ok := callerParams["subscribe_id"]; ok {
				requestID = val
			} else if idStr := getStringValue(callerParams, "subscribe_id", ""); idStr != "" {
				requestID = idStr
			} else {
				requestID = w.nextRequestIDString()
			}

			payload := map[string]any{
				"method": method,
				"params": streams,
				"id":     requestID,
			}
			if listenKey := getStringValue(callerParams, "listen_key", ""); listenKey != "" {
				payload["listenKey"] = listenKey
			}
			if extra, ok := callerParams["subscribe_extra"].(map[string]any); ok && len(extra) > 0 {
				for k, v := range extra {
					payload[k] = v
				}
			}
			if bytes, err := json.Marshal(payload); err != nil {
				logging.Warn(context.Background(), logging.EventWSSubscribeBuildErr, "build binance subscribe request failed", logging.Fields{
					"error": err.Error(),
				})
			} else {
				w.useRawSubscribe = true
				w.subscribePayloads = [][]byte{bytes}
				w.subscribeTopic = strings.Join(streams, ",")
				return
			}
		} else {
			w.skipSubscribe = true
			return
		}
	}

	topic := getStringValue(callerParams, "subscribe", w.subscribeTopic)
	if topic == "" {
		topic = "newHeads"
	}
	extra := callerParams["subscribe_params"]
	if extra == nil {
		extra = w.subscribePayload
	}

	w.subscribeTopic = topic
	w.subscribePayload = extra
	w.subscribeReq = w.buildSubscribeRequest(topic, extra)
	if topic != "" && len(w.allowedStreams) == 0 {
		w.setAllowedStreamsLocked([]string{topic})
	}
}

func (w *WebSocketCall) buildSubscribeRequest(topic string, extra interface{}) protocol.JSONRPCRequest {
	params := []interface{}{}
	if topic != "" {
		params = append(params, topic)
	}
	if extra != nil {
		switch v := extra.(type) {
		case []interface{}:
			params = append(params, v...)
		default:
			params = append(params, v)
		}
	}
	req := protocol.JSONRPCRequest{
		JSONRPC: w.subscribeJSONRPC,
		ID:      w.nextRequestIDString(),
		Method:  w.subscribeMethod,
		Params:  params,
	}
	return req
}

func parseHeartbeatPayload(config map[string]any, params map[string]any) []byte {
	if payload, ok := extractHeartbeatPayload(params); ok {
		return payload
	}
	if payload, ok := extractHeartbeatPayload(config); ok {
		return payload
	}
	return nil
}

func extractHeartbeatPayload(source map[string]any) ([]byte, bool) {
	if source == nil {
		return nil, false
	}
	if payload, ok := source["heartbeat_payload"]; ok && payload != nil {
		if bytes, err := normalizeRawJSON(payload); err == nil {
			return append([]byte(nil), bytes...), true
		} else {
			logging.Warn(context.Background(), logging.EventWSHeartbeatPayload, "heartbeat payload parse failed", logging.Fields{
				"error": err.Error(),
			})
		}
	}
	return nil, false
}

func parseHeartbeatOpcode(value string) int {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "text", "txt":
		return websocket.TextMessage
	case "binary", "bin":
		return websocket.BinaryMessage
	case "ping":
		return websocket.PingMessage
	case "pong":
		return websocket.PongMessage
	default:
		return 0
	}
}

func normalizeRawPayloads(value interface{}) ([][]byte, error) {
	switch v := value.(type) {
	case nil:
		return nil, nil
	case []byte, json.RawMessage, string, map[string]any:
		bytes, err := normalizeRawJSON(v)
		if err != nil {
			return nil, err
		}
		if len(bytes) == 0 {
			return nil, nil
		}
		return [][]byte{append([]byte(nil), bytes...)}, nil
	case []interface{}:
		var out [][]byte
		for _, item := range v {
			bytes, err := normalizeRawJSON(item)
			if err != nil {
				return nil, err
			}
			if len(bytes) == 0 {
				continue
			}
			out = append(out, append([]byte(nil), bytes...))
		}
		return out, nil
	case []string:
		var out [][]byte
		for _, item := range v {
			if strings.TrimSpace(item) == "" {
				continue
			}
			out = append(out, []byte(item))
		}
		return out, nil
	default:
		bytes, err := normalizeRawJSON(v)
		if err != nil {
			return nil, fmt.Errorf("unsupported subscribe_raw type %T", value)
		}
		if len(bytes) == 0 {
			return nil, nil
		}
		return [][]byte{append([]byte(nil), bytes...)}, nil
	}
}
