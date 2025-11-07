package util

import (
	"encoding/json"
	"fmt"
	"strings"
)

// ToUint64 将各种类型转换为 uint64
func ToUint64(v interface{}) (uint64, error) {
	switch vv := v.(type) {
	case int:
		if vv < 0 {
			return 0, fmt.Errorf("negative value %d", vv)
		}
		return uint64(vv), nil
	case int64:
		if vv < 0 {
			return 0, fmt.Errorf("negative value %d", vv)
		}
		return uint64(vv), nil
	case uint64:
		return vv, nil
	case float64:
		if vv < 0 {
			return 0, fmt.Errorf("negative value %f", vv)
		}
		return uint64(vv), nil
	case string:
		return ParseUintString(vv)
	default:
		return 0, fmt.Errorf("unsupported type %T", v)
	}
}

// ParseUintString 解析字符串为 uint64，支持十进制和十六进制
func ParseUintString(s string) (uint64, error) {
	if strings.HasPrefix(s, "0x") || strings.HasPrefix(s, "0X") {
		var val uint64
		_, err := fmt.Sscanf(s, "%x", &val)
		return val, err
	}
	var val uint64
	_, err := fmt.Sscanf(s, "%d", &val)
	return val, err
}

// ToInt64 将各种类型转换为 int64
func ToInt64(v interface{}) int64 {
	switch vv := v.(type) {
	case int64:
		return vv
	case int:
		return int64(vv)
	case float64:
		return int64(vv)
	case json.Number:
		if i, err := vv.Int64(); err == nil {
			return i
		}
	case string:
		if vv == "" {
			return 0
		}
		if i, err := json.Number(vv).Int64(); err == nil {
			return i
		}
	}
	return 0
}

// ToInt 将各种类型转换为 int
func ToInt(v interface{}) int {
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
			return 0
		}
		if i, err := json.Number(vv).Int64(); err == nil {
			return int(i)
		}
	}
	return 0
}

// ToString 将各种类型转换为字符串
func ToString(v interface{}) string {
	switch vv := v.(type) {
	case string:
		return vv
	case fmt.Stringer:
		return vv.String()
	case float64:
		return strings.TrimRight(strings.TrimRight(fmt.Sprintf("%.12f", vv), "0"), ".")
	case json.Number:
		return vv.String()
	case nil:
		return ""
	default:
		return fmt.Sprintf("%v", vv)
	}
}

// ToBool 将各种类型转换为布尔值，返回值和是否转换成功
func ToBool(v interface{}) (bool, bool) {
	switch vv := v.(type) {
	case bool:
		return vv, true
	case string:
		switch strings.ToLower(strings.TrimSpace(vv)) {
		case "true", "1", "yes":
			return true, true
		case "false", "0", "no":
			return false, true
		}
	case int:
		return vv != 0, true
	case int64:
		return vv != 0, true
	case float64:
		return vv != 0, true
	}
	return false, false
}
