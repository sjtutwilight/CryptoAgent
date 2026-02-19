package caller

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/protocol"
	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/types"
	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/util"
)

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
	seq := atomic.AddInt64(&wsGlobalReqCounter, 1)
	// 仅在 endpoint 级共享 + JSON-RPC 场景使用 subscriber 前缀。
	// Binance/Hyperliquid 等协议要求更严格的请求 id 格式，避免携带 ":"。
	if w.shareByEndpoint && w.sharedSubID > 0 && strings.EqualFold(w.messageFormat, "jsonrpc") {
		return fmt.Sprintf("%d:%d", w.sharedSubID, seq)
	}
	return fmt.Sprintf("%d", seq)
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
