package protocol

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
)

// Send 发送HTTP请求
func (h *HTTPHandler) Send(ctx context.Context, message []byte) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, "POST", h.baseURL, bytes.NewReader(message))
	if err != nil {
		return nil, fmt.Errorf("创建HTTP请求失败: %w", err)
	}

	// 设置headers
	req.Header.Set("Content-Type", "application/json")
	for k, v := range h.headers {
		req.Header.Set(k, v)
	}

	// 发送请求
	resp, err := h.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("发送HTTP请求失败: %w", err)
	}
	defer resp.Body.Close()

	// 读取响应
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("读取HTTP响应失败: %w", err)
	}

	// 检查状态码
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("HTTP请求失败: status_code=%d, body=%s", resp.StatusCode, string(body))
	}

	return body, nil
}

// Receive HTTP不支持接收（单向）
func (h *HTTPHandler) Receive(ctx context.Context) (<-chan []byte, <-chan error) {
	dataChan := make(chan []byte)
	errChan := make(chan error, 1)
	errChan <- fmt.Errorf("HTTP协议不支持Receive操作")
	close(dataChan)
	return dataChan, errChan
}

// HealthCheck 健康检查
func (h *HTTPHandler) HealthCheck(ctx context.Context) error {
	// 简单的GET请求测试连通性
	req, err := http.NewRequestWithContext(ctx, "GET", h.baseURL, nil)
	if err != nil {
		return err
	}

	resp, err := h.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return nil
}
