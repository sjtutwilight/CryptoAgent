package util

import (
	"fmt"
	"strconv"
)

// GetString 从map中获取字符串值
func GetString(data map[string]interface{}, key string) string {
	if val, ok := data[key].(string); ok {
		return val
	}
	return ""
}

// GetInt64 获取int64值
func GetInt64(data map[string]interface{}, key string) int64 {
	switch val := data[key].(type) {
	case float64:
		return int64(val)
	case int64:
		return val
	case string:
		// 如果是字符串，尝试解析
		if parsed, err := strconv.ParseInt(val, 10, 64); err == nil {
			return parsed
		}
	}
	return 0
}

// GetBool 获取bool值
func GetBool(data map[string]interface{}, key string) bool {
	if val, ok := data[key].(bool); ok {
		return val
	}
	return false
}

// GetTransactionCount 获取交易数量
func GetTransactionCount(data map[string]interface{}) int {
	if txs, ok := data["transactions"].([]interface{}); ok {
		return len(txs)
	}
	return 0
}

// ParseTimestamp 解析时间戳
func ParseTimestamp(s string) (int64, error) {
	var timestamp int64
	if _, err := fmt.Sscanf(s, "%d", &timestamp); err != nil {
		return 0, err
	}
	return timestamp, nil
}
