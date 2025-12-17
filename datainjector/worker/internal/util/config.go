package util

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// GetString 从配置 map 中获取字符串值，如果不存在或类型不匹配则返回默认值
func GetString(m map[string]any, key, def string) string {
	if m == nil {
		return def
	}
	if v, ok := m[key]; ok {
		if sv, ok := v.(string); ok {
			return sv
		}
	}
	return def
}

// GetInt 从配置 map 中获取整数值，支持多种数值类型转换
func GetInt(m map[string]any, key string, def int) int {
	if m == nil {
		return def
	}
	if v, ok := m[key]; ok {
		switch vv := v.(type) {
		case int:
			return vv
		case int64:
			return int(vv)
		case float64:
			return int(vv)
		case json.Number:
			if i, err := vv.Int64(); err == nil {
				return int(i)
			}
		case string:
			if vv == "" {
				return def
			}
			if i, err := strconv.Atoi(vv); err == nil {
				return i
			}
		}
	}
	return def
}

// GetBool 从配置 map 中获取布尔值，支持多种表示方式
func GetBool(m map[string]any, key string, def bool) bool {
	if m == nil {
		return def
	}
	if v, ok := m[key]; ok {
		switch vv := v.(type) {
		case bool:
			return vv
		case string:
			switch strings.ToLower(strings.TrimSpace(vv)) {
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
	}
	return def
}

// GetStringSlice 从配置 map 中获取字符串切片，支持多种输入格式
func GetStringSlice(m map[string]any, key string) []string {
	if m == nil {
		return nil
	}
	if raw, ok := m[key]; ok {
		switch vv := raw.(type) {
		case []string:
			return vv
		case []any:
			out := make([]string, 0, len(vv))
			for _, item := range vv {
				out = append(out, fmt.Sprint(item))
			}
			return out
		case string:
			if vv == "" {
				return nil
			}
			return strings.Split(vv, ",")
		}
	}
	return nil
}

// ToDuration 将整数值转换为 time.Duration，配合单位使用
func ToDuration(v int, unit time.Duration) time.Duration {
	if v <= 0 {
		return 0
	}
	return time.Duration(v) * unit
}






