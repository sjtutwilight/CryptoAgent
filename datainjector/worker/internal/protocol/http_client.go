package protocol

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

type HTTPClientConfig struct {
	Endpoint          string
	TimeoutMs         int
	MaxIdleConns      int
	MaxIdlePerHost    int
	IdleConnTimeoutMs int
}

type HTTPClient struct {
	endpoint string
	client   *http.Client
}

var httpClients sync.Map

func GetHTTPClient(cfg HTTPClientConfig) *HTTPClient {
	key := cfg.Endpoint
	if key == "" {
		return nil
	}
	if v, ok := httpClients.Load(key); ok {
		return v.(*HTTPClient)
	}

	transport := &http.Transport{}
	if cfg.MaxIdleConns > 0 {
		transport.MaxIdleConns = cfg.MaxIdleConns
	}
	if cfg.MaxIdlePerHost > 0 {
		transport.MaxIdleConnsPerHost = cfg.MaxIdlePerHost
	}
	if cfg.IdleConnTimeoutMs > 0 {
		transport.IdleConnTimeout = time.Duration(cfg.IdleConnTimeoutMs) * time.Millisecond
	}

	timeout := 30 * time.Second
	if cfg.TimeoutMs > 0 {
		timeout = time.Duration(cfg.TimeoutMs) * time.Millisecond
	}

	hc := &HTTPClient{
		endpoint: cfg.Endpoint,
		client: &http.Client{
			Timeout:   timeout,
			Transport: transport,
		},
	}

	actual, _ := httpClients.LoadOrStore(key, hc)
	return actual.(*HTTPClient)
}

func (c *HTTPClient) Call(ctx context.Context, req JSONRPCRequest) ([]byte, error) {
	if c == nil {
		return nil, fmt.Errorf("http client not initialized")
	}
	if req.JSONRPC == "" {
		req.JSONRPC = "2.0"
	}
	body, err := jsonMarshal(req)
	if err != nil {
		return nil, err
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode >= 400 {
		return respBody, fmt.Errorf("http status %d", resp.StatusCode)
	}
	return respBody, nil
}

func jsonMarshal(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}
