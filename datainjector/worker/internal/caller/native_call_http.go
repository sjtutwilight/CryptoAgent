package caller

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"

	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/protocol"
	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/resource"
	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/types"
	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/util"
)

type HTTPCall struct {
	httpConfig       protocol.HTTPClientConfig
	httpEndpoint     string
	datasourceID     string
	chainID          string
	reqCounter       int64
	defaultTransport string

	baseArgs     map[string]any
	baseMetadata map[string]any

	rateLimiter *resource.RateLimiter
}

type CallError struct {
	StatusCode int
	Retryable  bool
	Err        error
}

func (e *CallError) Error() string {
	if e == nil || e.Err == nil {
		return ""
	}
	return e.Err.Error()
}

func (e *CallError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func NewHTTPCall(callerConfig map[string]any, params map[string]any) (*HTTPCall, error) {
	c := &HTTPCall{
		baseArgs:     map[string]any{},
		baseMetadata: map[string]any{},
	}

	callerParams, _ := params["caller_params"].(map[string]any)

	if ds := getStringValue(callerConfig, "datasource_id", ""); ds != "" {
		c.datasourceID = ds
	}
	if callerParams != nil && c.datasourceID == "" {
		if ds := getStringValue(callerParams, "datasource_id", ""); ds != "" {
			c.datasourceID = ds
		}
	}

	if cid := getStringValue(callerConfig, "chain_id", ""); cid != "" {
		c.chainID = cid
	}
	if callerParams != nil && c.chainID == "" {
		if cid := getStringValue(callerParams, "chain_id", ""); cid != "" {
			c.chainID = cid
		}
	}

	if endpoint := getStringValue(callerConfig, "endpoint", ""); endpoint != "" {
		c.httpEndpoint = endpoint
	}
	if callerParams != nil && c.httpEndpoint == "" {
		if endpoint := getStringValue(callerParams, "endpoint", ""); endpoint != "" {
			c.httpEndpoint = endpoint
		}
	}

	timeout := getIntValue(callerConfig, "timeout_ms", getIntValue(callerParams, "timeout_ms", 30000))
	c.httpConfig = protocol.HTTPClientConfig{
		TimeoutMs:         timeout,
		MaxIdleConns:      getIntValue(callerConfig, "max_idle_conns", 0),
		MaxIdlePerHost:    getIntValue(callerConfig, "max_idle_per_host", 0),
		IdleConnTimeoutMs: getIntValue(callerConfig, "idle_conn_timeout_ms", 0),
	}

	c.defaultTransport = strings.ToLower(getStringValue(callerConfig, "transport", ""))
	if callerParams != nil && c.defaultTransport == "" {
		c.defaultTransport = strings.ToLower(getStringValue(callerParams, "transport", ""))
	}
	if c.defaultTransport == "" {
		c.defaultTransport = "rpc"
	}

	// Merge default request args
	if reqCfg, ok := callerConfig["request"]; ok {
		mergeIntoAnyMap(c.baseArgs, reqCfg)
	}
	if callerParams != nil {
		if reqCfg, ok := callerParams["request"]; ok {
			mergeIntoAnyMap(c.baseArgs, reqCfg)
		}
	}
	if _, ok := c.baseArgs["transport"]; !ok {
		c.baseArgs["transport"] = c.defaultTransport
	}
	if _, ok := c.baseArgs["url"]; !ok && c.httpEndpoint != "" {
		c.baseArgs["url"] = c.httpEndpoint
	}

	// Legacy header/query fields
	if headers, ok := callerConfig["headers"]; ok {
		ensureNestedMap(c.baseArgs, "headers")
		mergeIntoAnyMap(toMapStringAny(c.baseArgs["headers"]), headers)
	}
	if callerParams != nil {
		if headers, ok := callerParams["headers"]; ok {
			ensureNestedMap(c.baseArgs, "headers")
			mergeIntoAnyMap(toMapStringAny(c.baseArgs["headers"]), headers)
		}
	}
	if query, ok := callerConfig["query"]; ok {
		ensureNestedMap(c.baseArgs, "query")
		mergeIntoAnyMap(toMapStringAny(c.baseArgs["query"]), query)
	}
	if callerParams != nil {
		if query, ok := callerParams["query"]; ok {
			ensureNestedMap(c.baseArgs, "query")
			mergeIntoAnyMap(toMapStringAny(c.baseArgs["query"]), query)
		}
	}

	if strings.EqualFold(c.defaultTransport, "rest") {
		method := strings.ToUpper(getStringValue(callerConfig, "method", ""))
		if callerParams != nil && method == "" {
			method = strings.ToUpper(getStringValue(callerParams, "method", ""))
		}
		if method == "" {
			method = http.MethodGet
		}
		if _, ok := c.baseArgs["method"]; !ok {
			c.baseArgs["method"] = method
		}
	}

	if metaCfg, ok := callerConfig["metadata"]; ok {
		mergeIntoAnyMap(c.baseMetadata, metaCfg)
	}
	if callerParams != nil {
		if metaCfg, ok := callerParams["metadata"]; ok {
			mergeIntoAnyMap(c.baseMetadata, metaCfg)
		}
	}

	// Rate limiter
	if rlCfg, ok := callerConfig["rate_limit"].(map[string]any); ok {
		dsID := c.datasourceID
		if dsID == "" {
			dsID = getStringValue(callerConfig, "datasource_id", "")
		}
		if dsID == "" && callerParams != nil {
			dsID = getStringValue(callerParams, "datasource_id", "")
		}
		if dsID == "" {
			dsID = "http_call"
		}
		if conf, err := resource.ParseRateLimitConfig(rlCfg); err != nil {
			log.Printf("[HTTPCall] parse rate_limit failed: %v", err)
		} else {
			c.rateLimiter = resource.GetManager().GetOrCreateRateLimiter(dsID, conf)
		}
	}

	return c, nil
}

func (h *HTTPCall) CallOnce(ctx context.Context, args map[string]any) ([]*types.Message, error) {
	if err := h.waitRateLimit(ctx); err != nil {
		return nil, fmt.Errorf("rate limit wait: %w", err)
	}

	reqArgs := cloneAnyMap(h.baseArgs)
	if args != nil {
		mergeIntoAnyMap(reqArgs, args)
	}
	transport := strings.ToLower(getStringValue(reqArgs, "transport", h.defaultTransport))
	if transport == "" {
		transport = h.defaultTransport
	}

	switch transport {
	case "rest":
		return h.callREST(ctx, reqArgs)
	default:
		return h.callJSONRPC(ctx, reqArgs)
	}
}

func (h *HTTPCall) callJSONRPC(ctx context.Context, args map[string]any) ([]*types.Message, error) {
	urlStr := getStringValue(args, "url", h.httpEndpoint)
	if urlStr == "" {
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
	cfg.Endpoint = urlStr
	client := protocol.GetHTTPClient(cfg)
	if client == nil {
		return nil, fmt.Errorf("http client 初始化失败")
	}

	respBody, err := client.Call(ctx, req)
	if err != nil {
		callErr := &CallError{
			Retryable: true,
			Err:       fmt.Errorf("http 调用失败: %w", err),
		}
		if httpErr, ok := err.(*protocol.HTTPStatusError); ok {
			callErr.StatusCode = httpErr.StatusCode
			callErr.Retryable = shouldRetryStatus(httpErr.StatusCode)
		}
		return nil, callErr
	}

	var resp protocol.JSONRPCResponse
	if err := json.Unmarshal(respBody, &resp); err != nil {
		return nil, fmt.Errorf("解析 http 响应失败: %w", err)
	}
	if resp.Error != nil {
		return nil, fmt.Errorf("rpc error code=%d msg=%s", resp.Error.Code, resp.Error.Message)
	}

	meta := h.buildMetadata(args)
	meta["protocol"] = "http"
	meta["http_method"] = method
	meta["http_url"] = urlStr

	return buildMessagesFromResult(method, resp.Result, meta)
}

func (h *HTTPCall) callREST(ctx context.Context, args map[string]any) ([]*types.Message, error) {
	urlStr := getStringValue(args, "url", h.httpEndpoint)
	if urlStr == "" {
		return nil, fmt.Errorf("http rest url required")
	}

	method := strings.ToUpper(getStringValue(args, "method", ""))
	if method == "" {
		method = http.MethodGet
	}

	requestURL, err := url.Parse(urlStr)
	if err != nil {
		return nil, fmt.Errorf("invalid rest url: %w", err)
	}

	query := toMapStringAny(args["query"])
	if len(query) > 0 {
		values := requestURL.Query()
		for k, v := range query {
			values.Set(k, util.ToString(v))
		}
		requestURL.RawQuery = values.Encode()
	}

	headers := toMapStringAny(args["headers"])

	var bodyReader io.Reader
	if bodyBytes, ok := args["body_bytes"].([]byte); ok && len(bodyBytes) > 0 {
		bodyReader = bytes.NewReader(bodyBytes)
	} else if bodyStr := getStringValue(args, "body", ""); bodyStr != "" {
		bodyReader = strings.NewReader(bodyStr)
	}

	req, err := http.NewRequestWithContext(ctx, method, requestURL.String(), bodyReader)
	if err != nil {
		return nil, fmt.Errorf("build rest request failed: %w", err)
	}
	for k, v := range headers {
		if s := util.ToString(v); s != "" {
			req.Header.Set(k, s)
		}
	}

	cfg := h.httpConfig
	if h.httpEndpoint != "" {
		cfg.Endpoint = h.httpEndpoint
	} else {
		cfg.Endpoint = requestURL.Scheme + "://" + requestURL.Host
	}
	client := protocol.GetHTTPClient(cfg)
	if client == nil {
		return nil, fmt.Errorf("http client 初始化失败")
	}

	respBody, statusCode, err := client.Do(req)
	if err != nil {
		return nil, &CallError{
			StatusCode: statusCode,
			Retryable:  shouldRetryStatus(statusCode),
			Err:        fmt.Errorf("rest 调用失败: %w", err),
		}
	}
	if statusCode >= http.StatusBadRequest {
		return nil, &CallError{
			StatusCode: statusCode,
			Retryable:  shouldRetryStatus(statusCode),
			Err:        fmt.Errorf("rest status=%d body=%s", statusCode, string(respBody)),
		}
	}

	payload := make([]byte, len(respBody))
	copy(payload, respBody)

	meta := h.buildMetadata(args)
	meta["protocol"] = "http"
	meta["http_method"] = method
	meta["http_url"] = requestURL.String()
	meta["http_status"] = statusCode

	return []*types.Message{{
		Metadata: meta,
		Payload:  payload,
	}}, nil
}

func (h *HTTPCall) buildMetadata(args map[string]any) map[string]any {
	meta := cloneAnyMap(h.baseMetadata)
	if h.datasourceID != "" {
		meta["datasource_id"] = h.datasourceID
	}

	if args != nil {
		if extra := toMapStringAny(args["metadata"]); len(extra) > 0 {
			mergeIntoAnyMap(meta, extra)
		}
	}

	if cid := util.FirstNonEmpty(h.chainID, util.ToString(args["chain_id"])); cid != "" {
		meta["chain_id"] = cid
	}
	if src := util.ToString(args["source"]); src != "" {
		meta["source"] = src
	}
	if taskID := util.ToString(args["task_id"]); taskID != "" {
		meta["task_id"] = taskID
	}
	if subID := util.ToString(args["subscription"]); subID != "" {
		meta["subscription"] = subID
	}
	return meta
}

func (h *HTTPCall) waitRateLimit(ctx context.Context) error {
	if h.rateLimiter == nil {
		return nil
	}
	return h.rateLimiter.Wait(ctx)
}

func shouldRetryStatus(code int) bool {
	if code == 0 {
		return true
	}
	switch code {
	case 408, 429, 500, 502, 503, 504:
		return true
	default:
		return code >= 500
	}
}

func ensureNestedMap(base map[string]any, key string) {
	if base == nil {
		return
	}
	if _, ok := base[key]; !ok {
		base[key] = map[string]any{}
	}
}
