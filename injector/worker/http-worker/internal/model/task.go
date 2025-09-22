package model

import (
	"encoding/json"
	"time"
)

// HttpTask 代表从Kafka接收的HTTP任务
type HttpTask struct {
	TaskID  string      `json:"taskId"`
	Payload TaskPayload `json:"payload"`
}

// TaskPayload 任务载荷
type TaskPayload struct {
	DataSourceURL string                 `json:"dataSourceUrl"`
	Method        string                 `json:"method"`
	Params        map[string]interface{} `json:"params"`
	APIKey        string                 `json:"apikey"`
	DataSourceID  string                 `json:"dataSourceId"`
	Headers       map[string]string      `json:"headers"`
}

// TaskStatus 任务状态
type TaskStatus struct {
	TaskID      string    `json:"taskId"`
	Status      string    `json:"status"`      // SUCCESS, FAILED, RETRY
	Message     string    `json:"message"`     // 错误信息或状态描述
	Timestamp   time.Time `json:"timestamp"`   // 处理时间戳
	Duration    int64     `json:"duration"`    // 处理耗时（毫秒）
	StatusCode  int       `json:"statusCode"`  // HTTP响应状态码
	DataSize    int       `json:"dataSize"`    // 响应数据大小
	RetryCount  int       `json:"retryCount"`  // 重试次数
}

// DataRecord 数据记录，用于发送到数据topic
type DataRecord struct {
	TaskID       string                 `json:"taskId"`
	DataSourceID string                 `json:"dataSourceId"`
	Timestamp    time.Time              `json:"timestamp"`
	Data         interface{}            `json:"data"`
	Metadata     map[string]interface{} `json:"metadata"`
}

// HTTPResponse HTTP响应结构
type HTTPResponse struct {
	StatusCode int               `json:"statusCode"`
	Headers    map[string]string `json:"headers"`
	Body       interface{}       `json:"body"`
	Duration   time.Duration     `json:"duration"`
	Size       int               `json:"size"`
}

// IsRetryable 判断状态码是否可重试
func (r *HTTPResponse) IsRetryable() bool {
	// 5xx错误和网络相关错误可重试
	if r.StatusCode >= 500 && r.StatusCode < 600 {
		return true
	}
	// 429限流错误可重试
	if r.StatusCode == 429 {
		return true
	}
	// 408请求超时可重试
	if r.StatusCode == 408 {
		return true
	}
	return false
}

// ToJSON 将对象转换为JSON字符串
func (t *HttpTask) ToJSON() (string, error) {
	data, err := json.Marshal(t)
	return string(data), err
}

// FromJSON 从JSON字符串解析HttpTask
func (t *HttpTask) FromJSON(jsonStr string) error {
	return json.Unmarshal([]byte(jsonStr), t)
}

// ToJSON 将TaskStatus转换为JSON字符串
func (ts *TaskStatus) ToJSON() (string, error) {
	data, err := json.Marshal(ts)
	return string(data), err
}

// ToJSON 将DataRecord转换为JSON字符串
func (dr *DataRecord) ToJSON() (string, error) {
	data, err := json.Marshal(dr)
	return string(data), err
}