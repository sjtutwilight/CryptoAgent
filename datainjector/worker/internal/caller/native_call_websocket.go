package caller

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
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
	}

	if cid := getStringValue(callerConfig, "chain_id", ""); cid != "" {
		call.chainID = cid
	}

	callerParams, _ := params["caller_params"].(map[string]any)
	if callerParams != nil {
		if cid := getStringValue(callerParams, "chain_id", ""); cid != "" && call.chainID == "" {
			call.chainID = cid
		}
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

func (w *WebSocketCall) CallOnce(ctx context.Context, args map[string]any) ([]*types.Message, error) {
	w.updateSubscribeFromArgs(args)

	w.mu.Lock()
	if !w.subscribed {
		if err := w.wsClient.Subscribe(w.subscribeReq); err != nil {
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
	topic := "newHeads"
	if callerParams != nil {
		if v := getStringValue(callerParams, "subscribe", ""); v != "" {
			topic = v
		}
	}
	var extra interface{}
	if callerParams != nil {
		extra = callerParams["subscribe_params"]
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
	return protocol.JSONRPCRequest{
		JSONRPC: "2.0",
		ID:      w.nextRequestIDString(),
		Method:  "eth_subscribe",
		Params:  params,
	}
}

func (w *WebSocketCall) receiveMessages() {
	for data := range w.wsClient.MessageChan() {
		if err := w.handleIncomingMessage(data); err != nil {
			log.Printf("[WebSocketCall] 处理 websocket 消息失败: %v", err)
		}
	}
}

func (w *WebSocketCall) handleIncomingMessage(data []byte) error {
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

	if base.Method == "eth_subscription" {
		var notif struct {
			JSONRPC string `json:"jsonrpc"`
			Method  string `json:"method"`
			Params  struct {
				Subscription string          `json:"subscription"`
				Result       json.RawMessage `json:"result"`
			} `json:"params"`
		}
		if err := json.Unmarshal(data, &notif); err != nil {
			return fmt.Errorf("解析订阅通知失败: %w", err)
		}

		metadata := map[string]any{
			"protocol":     "websocket",
			"subscription": notif.Params.Subscription,
		}
		if w.chainID != "" {
			metadata["chain_id"] = w.chainID
		}
		var block map[string]any
		if err := json.Unmarshal(notif.Params.Result, &block); err == nil {
			if numHex, ok := block["number"].(string); ok {
				if n, err := parseHexUint64(numHex); err == nil {
					metadata["block_number"] = int64(n)
				}
			}
		}
		payload := make([]byte, len(data))
		copy(payload, data)

		w.mu.Lock()
		w.msgBuffer = append(w.msgBuffer, &types.Message{Metadata: metadata, Payload: payload})
		w.mu.Unlock()
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

func (w *WebSocketCall) updateSubscribeFromArgs(args map[string]any) {
	if args == nil {
		return
	}
	if topic, ok := args["subscribe"].(string); ok && topic != "" && topic != w.subscribeTopic {
		w.subscribeTopic = topic
		w.subscribePayload = args["subscribe_params"]
		w.subscribeReq = w.buildSubscribeRequest(topic, w.subscribePayload)
		w.mu.Lock()
		w.subscribed = false
		w.mu.Unlock()
	}
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
