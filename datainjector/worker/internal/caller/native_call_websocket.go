package caller

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/gorilla/websocket"

	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/observability/logging"
	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/protocol"
	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/types"
	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/util"
)

type WebSocketCall struct {
	chainID              string
	subscribeTopic       string
	subscribePayload     interface{}
	subscribeReq         protocol.JSONRPCRequest
	subscribeMethod      string
	subscribeJSONRPC     string
	subscribeID          string
	wsClient             *protocol.WebSocketClient
	mu                   sync.Mutex
	subscribed           bool
	msgBuffer            []*types.Message
	reqCounter           int64
	wsPendingMu          sync.Mutex
	wsPending            map[string]chan wsRPCResponse
	messageFormat        string
	useRawSubscribe      bool
	subscribePayloads    [][]byte
	skipSubscribe        bool
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
		msgBuffer:            make([]*types.Message, 0),
		wsPending:            make(map[string]chan wsRPCResponse),
		subscribeMethod:      "eth_subscribe",
		subscribeJSONRPC:     "2.0",
		messageFormat:        "jsonrpc",
		notifyMethod:         "eth_subscription",
		extractBlockMetadata: true,
		resultMetadata:       make(map[string]string),
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
		HeartbeatPayload:   heartbeatPayload,
		HeartbeatOpcode:    heartbeatOpcode,
	}

	call.wsClient = protocol.NewWebSocketClient(cfg)
	if err := call.wsClient.Connect(); err != nil {
		logging.Warn(context.Background(), logging.EventWSInitConnectError, "initial websocket connect failed, will retry", logging.Fields{
			"error": err.Error(),
		})
	}

	call.refreshSubscribeRequest(callerParams)
	go call.receiveMessages()

	logging.Info(context.Background(), logging.EventWSInit, "websocket client initialized", logging.Fields{
		"message_format": call.messageFormat,
	})
	return call, nil
}

func (w *WebSocketCall) sendSubscribe() error {
	if w.wsClient == nil {
		return fmt.Errorf("websocket client 未初始化")
	}
	if w.skipSubscribe {
		w.subscribed = true
		return nil
	}
	if w.useRawSubscribe {
		if len(w.subscribePayloads) == 0 {
			return fmt.Errorf("subscribe payload 为空")
		}
		return w.wsClient.SendRawSubscribes(w.subscribePayloads)
	}
	req := w.buildSubscribeRequest(w.subscribeTopic, w.subscribePayload)
	w.subscribeReq = req
	return w.wsClient.Subscribe(req)
}

func (w *WebSocketCall) CallOnce(ctx context.Context, args map[string]any) ([]*types.Message, error) {
	if isRPCRequest(args) {
		return w.executeRPC(ctx, args)
	}

	if err := w.ensureConnected(); err != nil {
		logging.Warn(ctx, logging.EventWSConnectPending, "websocket connecting, waiting for retry", logging.Fields{
			"error": err.Error(),
		})
		return nil, nil
	}

	w.updateSubscribeFromArgs(args)

	w.mu.Lock()
	if !w.subscribed {
		if err := w.sendSubscribe(); err != nil {
			w.mu.Unlock()
			return nil, fmt.Errorf("websocket 订阅失败: %w", err)
		}
		w.subscribed = true
		if !w.skipSubscribe {
			logging.Info(ctx, logging.EventWSSubscribeRequested, "websocket subscribe requested", logging.Fields{
				"subscription": w.subscribeTopic,
			})
		}
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

func isRPCRequest(args map[string]any) bool {
	if len(args) == 0 {
		return false
	}
	if v, ok := args["rpc"]; ok {
		return toBool(v, false)
	}
	if method := util.ToString(args["rpc_method"]); method != "" {
		return true
	}
	if method := util.ToString(args["method"]); method != "" {
		if _, hasParams := args["params"]; hasParams {
			if _, hasSubscribe := args["subscribe"]; hasSubscribe {
				return false
			}
			if _, hasStreams := args["streams"]; hasStreams {
				return false
			}
			return true
		}
	}
	return false
}

func (w *WebSocketCall) executeRPC(ctx context.Context, args map[string]any) ([]*types.Message, error) {
	method := util.ToString(args["rpc_method"])
	if method == "" {
		method = getStringValue(args, "method", "")
	}
	if method == "" {
		return nil, fmt.Errorf("websocket rpc method required")
	}
	params := args["params"]
	if params == nil {
		params = args["rpc_params"]
	}

	result, err := w.callWebSocket(ctx, method, params)
	if err != nil {
		return nil, err
	}

	meta := w.baseMetadata()
	if extra, ok := args["metadata"].(map[string]any); ok {
		for k, v := range extra {
			meta[k] = v
		}
	}
	if chainID := getStringValue(args, "chain_id", ""); chainID != "" {
		meta["chain_id"] = chainID
	}
	if source := getStringValue(args, "source", ""); source != "" {
		meta["source"] = source
	}
	if taskID := util.ToString(args["task_id"]); taskID != "" {
		meta["task_id"] = taskID
	}
	meta["protocol"] = "websocket"
	meta["ws_method"] = method

	return buildMessagesFromResult(method, result, meta)
}

func (w *WebSocketCall) refreshSubscribeRequest(callerParams map[string]any) {
	w.useRawSubscribe = false
	w.subscribePayloads = nil
	w.skipSubscribe = false

	if callerParams == nil {
		callerParams = map[string]any{}
	}

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
			w.mu.Lock()
			w.subscribeID = subID
			w.mu.Unlock()
			logging.Info(context.Background(), logging.EventWSSubscribeAck, "subscription ack received", logging.Fields{
				"subscription_id": subID,
			})
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

func (w *WebSocketCall) handleHyperliquidMessage(data []byte) error {
	var payload interface{}
	if err := json.Unmarshal(data, &payload); err != nil {
		return fmt.Errorf("解析 websocket 数据失败: %w", err)
	}

	if obj, ok := payload.(map[string]any); ok {
		if channel, ok := obj["channel"].(string); ok {
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

func (w *WebSocketCall) ensureConnected() error {
	if w.wsClient == nil {
		return fmt.Errorf("websocket client 未初始化")
	}
	if w.wsClient.IsConnected() {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.wsClient.IsConnected() {
		return nil
	}
	if err := w.wsClient.Connect(); err != nil {
		return err
	}
	w.subscribed = false
	return nil
}

func (w *WebSocketCall) updateSubscribeFromArgs(args map[string]any) {
	if !hasSubscribeOverrides(args) {
		return
	}
	w.refreshSubscribeRequest(args)
	w.mu.Lock()
	w.subscribed = false
	w.mu.Unlock()
}

func hasSubscribeOverrides(args map[string]any) bool {
	if args == nil || len(args) == 0 {
		return false
	}
	keys := []string{
		"subscribe",
		"subscribe_raw",
		"subscribe_method",
		"subscribe_jsonrpc",
		"subscribe_params",
		"subscribe_id",
		"subscribe_extra",
		"streams",
		"listen_key",
		"message_format",
		"notify_method",
		"metadata_fields",
		"static_metadata",
	}
	for _, k := range keys {
		if _, ok := args[k]; ok {
			return true
		}
	}
	return false
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
