package parser

import (
	"strconv"
	"strings"
)

// parseHexToInt64 将十六进制字符串转换为int64
func parseHexToInt64(hexStr string) int64 {
	// 移除0x前缀
	hexStr = strings.TrimPrefix(hexStr, "0x")
	if hexStr == "" {
		return 0
	}
	
	// 解析为int64
	val, err := strconv.ParseInt(hexStr, 16, 64)
	if err != nil {
		return 0
	}
	
	return val
}
