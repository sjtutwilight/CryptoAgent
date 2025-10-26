package caller

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/protocol"
	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/types"
)

type WebSocketCall struct {
	chainID               string
	subscribeTopic        string
	subscribePayload      interface{}
	subscribeReq          protocol.JSONRPCRequest
	subscribeMethod       string
	subscribeJSONRPC      string
	subscribeID           string
	wsClient              *protocol.WebSocketClient
	mu                    sync.Mutex
	subscribed            bool
	msgBuffer             []*types.Message
	reqCounter            int64
	wsPendingMu           sync.Mutex
	wsPending             map[string]chan wsRPCResponse
	defaultBackfillMethod string
	defaultIncludeFullTx  bool
	fallbacks             []BlockFetcher

	messageFormat        string
	useRawSubscribe      bool
	subscribeRaw         []byte
	skipAckResults       bool
	defaultStreams       []string
	staticMetadata       map[string]any
	notifyMethod         string
	extractBlockMetadata bool
	resultMetadata       map[string]string
}

func NewWebSocketCall(callerConfig map[string]any, params map[string]any) (*WebSocketCall, error) {
	url := getStringValue(callerConfig, "url", "")
	if url == "" {
		return nil, fmt.Errorf("websocket 缺少 url")
	}

	call := &WebSocketCall{
		msgBuffer:             make([]*types.Message, 0),
		wsPending:             make(map[string]chan wsRPCResponse),
		defaultBackfillMethod: "eth_getBlockByNumber",
		subscribeMethod:       "eth_subscribe",
		subscribeJSONRPC:      "2.0",
		messageFormat:         "jsonrpc",
		notifyMethod:          "eth_subscription",
		extractBlockMetadata:  true,
		resultMetadata:        make(map[string]string),
	}

	callerParams, _ := params["caller_params"].(map[string]any)

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

	if bfCfg, ok := callerConfig["backfill"].(map[string]any); ok {
		if wsCfg, ok := bfCfg["ws"].(map[string]any); ok {
			call.defaultBackfillMethod = getStringValue(wsCfg, "rpc_method", call.defaultBackfillMethod)
			call.defaultIncludeFullTx = getBoolValue(wsCfg, "include_full_tx", false)
		}
	}
	if callerParams != nil {
		if bfCfg, ok := callerParams["backfill"].(map[string]any); ok {
			if wsCfg, ok := bfCfg["ws"].(map[string]any); ok {
				call.defaultBackfillMethod = getStringValue(wsCfg, "rpc_method", call.defaultBackfillMethod)
				call.defaultIncludeFullTx = getBoolValue(wsCfg, "include_full_tx", call.defaultIncludeFullTx)
			}
			if httpCfg, ok := bfCfg["http"].(map[string]any); ok {
				if fallback := buildHTTPFallback(httpCfg, call.chainID); fallback != nil {
					call.fallbacks = append(call.fallbacks, fallback)
				}
			}
		}
	}

	if bfCfg, ok := callerConfig["backfill"].(map[string]any); ok {
		if httpCfg, ok := bfCfg["http"].(map[string]any); ok {
			if fallback := buildHTTPFallback(httpCfg, call.chainID); fallback != nil {
				call.fallbacks = append(call.fallbacks, fallback)
			}
		}
	}

	heartbeatMs := getIntValue(callerParams, "heartbeat_ms", 30000)
	reconnectBase := 2
	reconnectMax := 60
	if rcMap, ok := callerParams["reconnect"].(map[string]any); ok {
		reconnectBase = getIntValue(rcMap, "backoff_base_seconds", reconnectBase)
		reconnectMax = getIntValue(rcMap, "backoff_max_seconds", reconnectMax)
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
		URL:                url,
		HeartbeatMs:        heartbeatMs,
		BackoffBaseSeconds: reconnectBase,
		BackoffMaxSeconds:  reconnectMax,
	}

	call.wsClient = protocol.NewWebSocketClient(cfg)
	if err := call.wsClient.Connect(); err != nil {
		return nil, fmt.Errorf("websocket 连接失败: %w", err)
	}

	call.refreshSubscribeRequest(callerParams)
	go call.receiveMessages()

	log.Printf("[WebSocketCall] WebSocket 客户端初始化完成")
	return call, nil
}

func (w *WebSocketCall) sendSubscribe() error {
	if w.wsClient == nil {
		return fmt.Errorf("websocket client 未初始化")
	}
	if w.useRawSubscribe {
		if len(w.subscribeRaw) == 0 {
			return fmt.Errorf("subscribe payload 为空")
		}
		return w.wsClient.SendRawSubscribe(w.subscribeRaw)
	}
	req := w.buildSubscribeRequest(w.subscribeTopic, w.subscribePayload)
	w.subscribeReq = req
	return w.wsClient.Subscribe(req)
}

func (w *WebSocketCall) CallOnce(ctx context.Context, args map[string]any) ([]*types.Message, error) {
	w.updateSubscribeFromArgs(args)

	w.mu.Lock()
	if !w.subscribed {
		if err := w.sendSubscribe(); err != nil {
			w.mu.Unlock()
			return nil, fmt.Errorf("websocket 订阅失败: %w", err)
		}
		w.subscribed = true
		log.Printf("[WebSocketCall] WebSocket 已发起订阅: %s", w.subscribeTopic)
	}

	msgs := w.msgBuffer
	w.msgBuffer = make([]*types.Message, 0)
	w.mu.Unlock()

	return msgs, nil
}

func (w *WebSocketCall) Close() error {
	if w.wsClient != nil {
		return w.wsClient.Close()
	}
	return nil
}

func (w *WebSocketCall) FetchBlocks(ctx context.Context, start, end int64, rpcMethod string, options map[string]any) ([]*types.Message, error) {
	if start > end {
		return nil, fmt.Errorf("invalid backfill range [%d, %d]", start, end)
	}
	method := rpcMethod
	if method == "" {
		method = w.defaultBackfillMethod
	}
	includeFullTx := w.defaultIncludeFullTx
	if options != nil {
		if v, ok := options["include_full_tx"]; ok {
			includeFullTx = toBool(v, includeFullTx)
		}
	}

	all := make([]*types.Message, 0, int(end-start+1))
	for blk := start; blk <= end; blk++ {
		params := []interface{}{fmt.Sprintf("0x%x", blk), false} // 始终传 false（不包含完整交易）
		if includeFullTx {
			params[1] = true
		}
		result, err := w.callWebSocket(ctx, method, params)
		if err != nil {
			log.Printf("[WebSocketCall] FetchBlocks: block=%d RPC call failed: %v", blk, err)
			return nil, err
		}
		if len(result) == 0 || string(result) == "null" {
			log.Printf("[WebSocketCall] FetchBlocks: block=%d returned null/empty result", blk)
			continue // 跳过 null 结果
		}

		meta := map[string]any{
			"protocol":    "websocket",
			"ws_method":   method,
			"source":      "ws_backfill",
			"block_query": blk,
			"is_backfill": true,
		}
		if includeFullTx {
			meta["include_full_tx"] = true
		}
		if w.chainID != "" {
			meta["chain_id"] = w.chainID
		}

		msgs, err := buildMessagesFromResult(method, result, meta)
		if err != nil {
			log.Printf("[WebSocketCall] FetchBlocks: block=%d buildMessages failed: %v, result=%s", blk, err, string(result))
			return nil, err
		}

		all = append(all, msgs...)
	}
	return all, nil
}

func (w *WebSocketCall) TransportName() string {
	return types.BackfillTransportWebSocket
}

func (w *WebSocketCall) BackfillExecutors() []BlockFetcher {
	if len(w.fallbacks) == 0 {
		return nil
	}
	out := make([]BlockFetcher, len(w.fallbacks))
	copy(out, w.fallbacks)
	return out
}

func buildHTTPFallback(cfg map[string]any, defaultChainID string) BlockFetcher {
	if !getBoolValue(cfg, "enabled", false) {
		return nil
	}
	endpoint := getStringValue(cfg, "endpoint", "")
	if endpoint == "" {
		return nil
	}

	httpCallerCfg := map[string]any{
		"endpoint":             endpoint,
		"datasource_id":        getStringValue(cfg, "datasource_id", ""),
		"chain_id":             getStringValue(cfg, "chain_id", defaultChainID),
		"timeout_ms":           getIntValue(cfg, "timeout_ms", 5000),
		"max_idle_conns":       getIntValue(cfg, "max_idle_conns", 0),
		"max_idle_per_host":    getIntValue(cfg, "max_idle_per_host", 0),
		"idle_conn_timeout_ms": getIntValue(cfg, "idle_conn_timeout_ms", 0),
	}

	callerParams := map[string]any{
		"caller_params": map[string]any{
			"chain_id":   httpCallerCfg["chain_id"],
			"timeout_ms": httpCallerCfg["timeout_ms"],
		},
	}

	httpCall, err := NewHTTPCall(httpCallerCfg, callerParams)
	if err != nil {
		log.Printf("[WebSocketCall] 创建 HTTP 回补失败: %v", err)
		return nil
	}
	return httpCall
}

func (w *WebSocketCall) refreshSubscribeRequest(callerParams map[string]any) {
	w.useRawSubscribe = false
	w.subscribeRaw = nil

	if callerParams == nil {
		callerParams = map[string]any{}
	}

	if rawPayload, ok := callerParams["subscribe_raw"]; ok && rawPayload != nil {
		if payloadBytes, err := normalizeRawJSON(rawPayload); err != nil {
			log.Printf("[WebSocketCall] subscribe_raw 解析失败: %v", err)
		} else {
			w.useRawSubscribe = true
			w.subscribeRaw = payloadBytes
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
				log.Printf("[WebSocketCall] 构造 Binance 订阅请求失败: %v", err)
			} else {
				w.useRawSubscribe = true
				w.subscribeRaw = bytes
				w.subscribeTopic = strings.Join(streams, ",")
				return
			}
		} else {
			log.Printf("[WebSocketCall] Binance 订阅缺少 streams 配置，将跳过订阅")
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

func (w *WebSocketCall) receiveMessages() {
	for data := range w.wsClient.MessageChan() {
		if err := w.handleIncomingMessage(data); err != nil {
			log.Printf("[WebSocketCall] 处理 websocket 消息失败: %v", err)
		}
	}
}

func (w *WebSocketCall) handleIncomingMessage(data []byte) error {
	switch w.messageFormat {
	case "binance":
		return w.handleBinanceMessage(data)
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

		meta := w.baseMetadata()
		if params.Subscription != "" {
			meta["subscription"] = params.Subscription
		}

		var payload interface{}
		if len(params.Result) > 0 {
			if err := json.Unmarshal(params.Result, &payload); err != nil {
				log.Printf("[WebSocketCall] 解析订阅结果失败: %v", err)
			}
		}
		w.applyResultMetadata(meta, payload)
		w.bufferMessage(meta, data)
		return nil
	}

	if len(base.Result) > 0 {
		var subID string
		if err := json.Unmarshal(base.Result, &subID); err == nil && subID != "" {
			w.mu.Lock()
			w.subscribeID = subID
			w.mu.Unlock()
			log.Printf("[WebSocketCall] 订阅成功，ID=%s", subID)
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

func (w *WebSocketCall) baseMetadata() map[string]any {
	meta := make(map[string]any, len(w.staticMetadata)+3)
	if w.staticMetadata != nil {
		for k, v := range w.staticMetadata {
			meta[k] = v
		}
	}
	meta["protocol"] = "websocket"
	if w.chainID != "" {
		if _, ok := meta["chain_id"]; !ok {
			meta["chain_id"] = w.chainID
		}
	}
	if w.subscribeTopic != "" {
		if _, ok := meta["subscription"]; !ok {
			meta["subscription"] = w.subscribeTopic
		}
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

func (w *WebSocketCall) bufferMessage(meta map[string]any, data []byte) {
	if meta == nil {
		meta = make(map[string]any)
	}
	payload := make([]byte, len(data))
	copy(payload, data)

	w.mu.Lock()
	w.msgBuffer = append(w.msgBuffer, &types.Message{Metadata: meta, Payload: payload})
	w.mu.Unlock()
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

func (w *WebSocketCall) updateSubscribeFromArgs(args map[string]any) {
	if args == nil {
		return
	}
	w.refreshSubscribeRequest(args)
	w.mu.Lock()
	w.subscribed = false
	w.mu.Unlock()
}

func (w *WebSocketCall) callWebSocket(ctx context.Context, method string, params interface{}) (json.RawMessage, error) {
	if w.wsClient == nil {
		return nil, fmt.Errorf("websocket client 未初始化")
	}
	reqID := w.nextRequestIDString()
	req := protocol.JSONRPCRequest{
		JSONRPC: "2.0",
		ID:      reqID,
		Method:  method,
		Params:  params,
	}
	payload, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("序列化 websocket 请求失败: %w", err)
	}
	respCh := make(chan wsRPCResponse, 1)
	w.wsPendingMu.Lock()
	w.wsPending[reqID] = respCh
	w.wsPendingMu.Unlock()

	if err := w.wsClient.WriteMessage(payload); err != nil {
		w.rejectPending(reqID, fmt.Errorf("发送 websocket 请求失败: %w", err))
		return nil, err
	}

	select {
	case resp := <-respCh:
		if resp.err != nil {
			return nil, resp.err
		}
		return resp.result, nil
	case <-ctx.Done():
		w.rejectPending(reqID, ctx.Err())
		return nil, ctx.Err()
	}
}

func (w *WebSocketCall) deliverPendingResponse(idRaw json.RawMessage, result json.RawMessage, rpcErr *struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}) bool {
	id := rawIDToString(idRaw)
	if id == "" {
		return false
	}
	w.wsPendingMu.Lock()
	ch, ok := w.wsPending[id]
	if ok {
		delete(w.wsPending, id)
	}
	w.wsPendingMu.Unlock()
	if !ok {
		return false
	}
	resp := wsRPCResponse{result: result}
	if rpcErr != nil {
		resp.err = fmt.Errorf("rpc error code=%d msg=%s", rpcErr.Code, rpcErr.Message)
	}
	select {
	case ch <- resp:
	default:
	}
	return true
}

func (w *WebSocketCall) rejectPending(id string, err error) {
	w.wsPendingMu.Lock()
	ch, ok := w.wsPending[id]
	if ok {
		delete(w.wsPending, id)
	}
	w.wsPendingMu.Unlock()
	if !ok {
		return
	}
	select {
	case ch <- wsRPCResponse{err: err}:
	default:
	}
}

func (w *WebSocketCall) nextRequestIDString() string {
	return fmt.Sprintf("%d", atomic.AddInt64(&w.reqCounter, 1))
}

func rawIDToString(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s
	}
	var n float64
	if err := json.Unmarshal(raw, &n); err == nil {
		return fmt.Sprintf("%d", int64(n))
	}
	return ""
}

func toBool(v interface{}, def bool) bool {
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
	default:
	}
	return def
}
