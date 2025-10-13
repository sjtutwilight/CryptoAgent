package caller

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync/atomic"

	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/protocol"
	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/types"
)

type HTTPCall struct {
	httpConfig   protocol.HTTPClientConfig
	httpEndpoint string
	datasourceID string
	chainID      string
	reqCounter   int64
}

func NewHTTPCall(callerConfig map[string]any, params map[string]any) (*HTTPCall, error) {
	c := &HTTPCall{}
	if ds := getStringValue(callerConfig, "datasource_id", ""); ds != "" {
		c.datasourceID = ds
	}
	if cid := getStringValue(callerConfig, "chain_id", ""); cid != "" {
		c.chainID = cid
	}
	if endpoint := getStringValue(callerConfig, "endpoint", ""); endpoint != "" {
		c.httpEndpoint = endpoint
	}

	callerParams, _ := params["caller_params"].(map[string]any)
	if callerParams != nil {
		if cid := getStringValue(callerParams, "chain_id", ""); cid != "" && c.chainID == "" {
			c.chainID = cid
		}
	}

	timeout := getIntValue(callerConfig, "timeout_ms", getIntValue(callerParams, "timeout_ms", 30000))
	c.httpConfig = protocol.HTTPClientConfig{
		TimeoutMs:         timeout,
		MaxIdleConns:      getIntValue(callerConfig, "max_idle_conns", 0),
		MaxIdlePerHost:    getIntValue(callerConfig, "max_idle_per_host", 0),
		IdleConnTimeoutMs: getIntValue(callerConfig, "idle_conn_timeout_ms", 0),
	}
	return c, nil
}

func (h *HTTPCall) CallOnce(ctx context.Context, args map[string]any) ([]*types.Message, error) {
	if args == nil {
		return nil, nil
	}

	url := getStringValue(args, "url", h.httpEndpoint)
	if url == "" {
		return nil, fmt.Errorf("http url required")
	}

	method := getStringValue(args, "method", "")
	if method == "" {
		return nil, fmt.Errorf("http method required")
	}

	paramsVal := args["params"]
	if paramsStr, ok := paramsVal.(string); ok && paramsStr != "" {
		var parsed interface{}
		if err := json.Unmarshal([]byte(paramsStr), &parsed); err == nil {
			paramsVal = parsed
		}
	}

	idVal := args["id"]
	if idVal == nil {
		idVal = fmt.Sprintf("%d", atomic.AddInt64(&h.reqCounter, 1))
	}

	jsonrpc := getStringValue(args, "jsonrpc", "2.0")

	req := protocol.JSONRPCRequest{
		JSONRPC: jsonrpc,
		ID:      idVal,
		Method:  method,
		Params:  paramsVal,
	}

	cfg := h.httpConfig
	cfg.Endpoint = url
	client := protocol.GetHTTPClient(cfg)
	if client == nil {
		return nil, fmt.Errorf("http client 初始化失败")
	}

	respBody, err := client.Call(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("http 调用失败: %w", err)
	}

	var resp protocol.JSONRPCResponse
	if err := json.Unmarshal(respBody, &resp); err != nil {
		return nil, fmt.Errorf("解析 http 响应失败: %w", err)
	}
	if resp.Error != nil {
		return nil, fmt.Errorf("rpc error code=%d msg=%s", resp.Error.Code, resp.Error.Message)
	}

	baseMetadata := map[string]any{
		"protocol":    "http",
		"http_method": method,
		"http_url":    url,
	}
	if h.datasourceID != "" {
		baseMetadata["datasource_id"] = h.datasourceID
	}
	if cid := firstNonEmpty(h.chainID, toString(args["chain_id"])); cid != "" {
		baseMetadata["chain_id"] = cid
	}
	if src := toString(args["source"]); src != "" {
		baseMetadata["source"] = src
	}
	if taskID := toString(args["task_id"]); taskID != "" {
		baseMetadata["task_id"] = taskID
	}
	if isBackfillMethod(method) {
		baseMetadata["is_backfill"] = true
	}
	if subID := toString(args["subscription"]); subID != "" {
		baseMetadata["subscription"] = subID
	}

	msgs, err := buildMessagesFromResult(method, resp.Result, baseMetadata)
	if err != nil {
		return nil, fmt.Errorf("构建 http 消息失败: %w", err)
	}
	log.Printf("[HTTPCall] CallOnce msgs=%d", len(msgs))
	return msgs, nil
}

func (h *HTTPCall) Close() error {
	return nil
}

func (h *HTTPCall) FetchBlocks(ctx context.Context, start, end int64, rpcMethod string, options map[string]any) ([]*types.Message, error) {
	if start > end {
		return nil, fmt.Errorf("invalid backfill range [%d, %d]", start, end)
	}
	method := rpcMethod
	if method == "" {
		method = "eth_getBlockByNumber"
	}
	includeFullTx := false
	if options != nil {
		if v, ok := options["include_full_tx"]; ok {
			includeFullTx = toBoolGeneric(v, includeFullTx)
		}
	}

	all := make([]*types.Message, 0, int(end-start+1))
	for blk := start; blk <= end; blk++ {
		params := buildBlockParams(method, blk, includeFullTx)
		args := map[string]any{
			"url":      h.httpEndpoint,
			"method":   method,
			"params":   params,
			"jsonrpc":  "2.0",
			"source":   "http_backfill",
			"chain_id": h.chainID,
		}
		msgs, err := h.CallOnce(ctx, args)
		if err != nil {
			return nil, err
		}
		all = append(all, msgs...)
	}
	return all, nil
}

func (h *HTTPCall) TransportName() string {
	return types.BackfillTransportHTTP
}

func toBoolGeneric(v interface{}, def bool) bool {
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
	return def
}

func buildBlockParams(method string, blockNum int64, includeFullTx bool) []interface{} {
	params := []interface{}{fmt.Sprintf("0x%x", blockNum)}
	if strings.EqualFold(method, "eth_getBlockByNumber") {
		params = append(params, includeFullTx)
	}
	return params
}
