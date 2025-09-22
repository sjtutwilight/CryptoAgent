package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"http-worker/internal/config"
	"http-worker/internal/model"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// HTTPClient HTTP客户端接口
type HTTPClient interface {
	ExecuteRequest(ctx context.Context, task *model.HttpTask) (*model.HTTPResponse, error)
	Close() error
}

// httpClient HTTP客户端实现
type httpClient struct {
	clients map[string]*http.Client // 按host:port隔离的客户端池
	mu      sync.RWMutex
	config  *config.HTTPClientConfig
}

// NewHTTPClient 创建新的HTTP客户端
func NewHTTPClient(cfg *config.HTTPClientConfig) HTTPClient {
	return &httpClient{
		clients: make(map[string]*http.Client),
		config:  cfg,
	}
}

// ExecuteRequest 执行HTTP请求
func (hc *httpClient) ExecuteRequest(ctx context.Context, task *model.HttpTask) (*model.HTTPResponse, error) {
	startTime := time.Now()

	// 解析URL获取host信息
	parsedURL, err := url.Parse(task.Payload.DataSourceURL)
	if err != nil {
		return nil, fmt.Errorf("解析URL失败: %w", err)
	}

	// 确保URL有路径，如果没有则添加"/"
	if parsedURL.Path == "" {
		parsedURL.Path = "/"
	}

	hostKey := parsedURL.Host
	client := hc.getOrCreateClient(hostKey)

	// 构建HTTP请求，使用修正后的URL
	req, err := hc.buildRequest(ctx, task, parsedURL)
	if err != nil {
		return nil, fmt.Errorf("构建请求失败: %w", err)
	}
	fmt.Println("req", req)
	// 执行请求
	resp, err := client.Do(req)
	if err != nil {
		return &model.HTTPResponse{
			StatusCode: 0,
			Duration:   time.Since(startTime),
		}, fmt.Errorf("HTTP请求失败: %w", err)
	}
	defer resp.Body.Close()

	// 读取响应体
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return &model.HTTPResponse{
			StatusCode: resp.StatusCode,
			Duration:   time.Since(startTime),
		}, fmt.Errorf("读取响应体失败: %w", err)
	}

	// 解析响应体为JSON
	var responseData interface{}
	if len(body) > 0 && resp.Header.Get("Content-Type") != "" &&
		strings.Contains(resp.Header.Get("Content-Type"), "application/json") {
		if err := json.Unmarshal(body, &responseData); err != nil {
			// 如果JSON解析失败，将原始字符串作为响应
			responseData = string(body)
		}
	} else {
		responseData = string(body)
	}

	// 构建响应头映射
	headers := make(map[string]string)
	for key, values := range resp.Header {
		if len(values) > 0 {
			headers[key] = values[0]
		}
	}

	return &model.HTTPResponse{
		StatusCode: resp.StatusCode,
		Headers:    headers,
		Body:       responseData,
		Duration:   time.Since(startTime),
		Size:       len(body),
	}, nil
}

// buildRequest 构建HTTP请求
func (hc *httpClient) buildRequest(ctx context.Context, task *model.HttpTask, parsedURL *url.URL) (*http.Request, error) {
	var req *http.Request
	var err error

	method := strings.ToUpper(task.Payload.Method)
	if method == "" {
		method = "GET"
	}

	switch method {
	case "GET":
		// GET请求，参数作为查询字符串
		reqURL := parsedURL.String()
		if len(task.Payload.Params) > 0 {
			parsedURL.RawQuery = hc.buildQueryString(task.Payload.Params)
			reqURL = parsedURL.String()
		}
		req, err = http.NewRequestWithContext(ctx, method, reqURL, nil)

	case "POST":
		// POST请求，参数作为JSON-RPC或表单数据
		var bodyReader io.Reader

		if len(task.Payload.Params) > 0 {
			// 检查是否是JSON-RPC请求
			if hc.isJSONRPCRequest(task, parsedURL) {
				jsonData, jsonErr := hc.buildJSONRPCRequest(task.Payload.Params)
				if jsonErr != nil {
					return nil, fmt.Errorf("构建JSON-RPC请求失败: %w", jsonErr)
				}
				bodyReader = bytes.NewReader(jsonData)
			} else {
				// 普通JSON请求
				jsonData, jsonErr := json.Marshal(task.Payload.Params)
				if jsonErr != nil {
					return nil, fmt.Errorf("序列化请求参数失败: %w", jsonErr)
				}
				bodyReader = bytes.NewReader(jsonData)
			}
		}

		req, err = http.NewRequestWithContext(ctx, method, parsedURL.String(), bodyReader)
		if err == nil && bodyReader != nil {
			req.Header.Set("Content-Type", "application/json")
		}

	default:
		return nil, fmt.Errorf("不支持的HTTP方法: %s", method)
	}

	if err != nil {
		return nil, err
	}

	// 设置请求头
	if task.Payload.Headers != nil {
		for key, value := range task.Payload.Headers {
			req.Header.Set(key, value)
		}
	}

	// 设置API Key
	if task.Payload.APIKey != "" {
		// 根据数据源类型设置不同的认证方式
		if strings.Contains(parsedURL.String(), "coinmarketcap") {
			req.Header.Set("X-CMC_PRO_API_KEY", task.Payload.APIKey)
		} else {
			req.Header.Set("Authorization", "Bearer "+task.Payload.APIKey)
		}
	}

	// 设置User-Agent
	req.Header.Set("User-Agent", "HTTP-Worker/1.0")

	return req, nil
}

// isJSONRPCRequest 检查是否是JSON-RPC请求
func (hc *httpClient) isJSONRPCRequest(task *model.HttpTask, parsedURL *url.URL) bool {
	// 检查URL是否指向以太坊类型的接口
	urlStr := parsedURL.String()
	return strings.Contains(urlStr, "localhost:8090") ||
		strings.Contains(urlStr, "ethereum") ||
		strings.Contains(strings.ToLower(urlStr), "rpc")
}

// buildJSONRPCRequest 构建JSON-RPC请求
func (hc *httpClient) buildJSONRPCRequest(params map[string]interface{}) ([]byte, error) {
	// 默认JSON-RPC请求结构
	jsonrpcReq := map[string]interface{}{
		"id":      1,
		"jsonrpc": "2.0",
		"method":  "eth_getBlockByNumber",
		"params":  []interface{}{"latest", false},
	}

	// 如果参数中指定了method，使用指定的method
	if method, exists := params["method"]; exists {
		jsonrpcReq["method"] = method
	}

	// 如果参数中指定了params，使用指定的params
	if rpcParams, exists := params["params"]; exists {
		jsonrpcReq["params"] = rpcParams
	}

	// 如果参数中指定了id，使用指定的id
	if id, exists := params["id"]; exists {
		jsonrpcReq["id"] = id
	}

	return json.Marshal(jsonrpcReq)
}

// buildQueryString 构建查询字符串
func (hc *httpClient) buildQueryString(params map[string]interface{}) string {
	values := url.Values{}
	for key, value := range params {
		values.Add(key, fmt.Sprintf("%v", value))
	}
	return values.Encode()
}

// getOrCreateClient 获取或创建HTTP客户端
func (hc *httpClient) getOrCreateClient(hostKey string) *http.Client {
	hc.mu.RLock()
	if client, exists := hc.clients[hostKey]; exists {
		hc.mu.RUnlock()
		return client
	}
	hc.mu.RUnlock()

	hc.mu.Lock()
	defer hc.mu.Unlock()

	// 双重检查
	if client, exists := hc.clients[hostKey]; exists {
		return client
	}

	// 创建新的HTTP客户端
	transport := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   time.Duration(hc.config.ConnectionTimeout) * time.Second,
			KeepAlive: time.Duration(hc.config.KeepAlive) * time.Second,
		}).DialContext,
		MaxIdleConns:        hc.config.MaxIdleConns,
		MaxIdleConnsPerHost: hc.config.MaxConnsPerHost,
		IdleConnTimeout:     time.Duration(hc.config.IdleConnTimeout) * time.Second,
		TLSHandshakeTimeout: 10 * time.Second,
		DisableKeepAlives:   false,
	}

	client := &http.Client{
		Transport: transport,
		Timeout:   time.Duration(hc.config.Timeout) * time.Second,
	}

	hc.clients[hostKey] = client
	return client
}

// Close 关闭HTTP客户端
func (hc *httpClient) Close() error {
	hc.mu.Lock()
	defer hc.mu.Unlock()

	// 关闭所有客户端的连接
	for _, client := range hc.clients {
		if transport, ok := client.Transport.(*http.Transport); ok {
			transport.CloseIdleConnections()
		}
	}

	hc.clients = make(map[string]*http.Client)
	return nil
}
