package caller

import "fmt"

func init() {
	Register("native_call", func(class string, params map[string]any) (Caller, error) {
		return NewNativeCall(class, params)
	})
}

func NewNativeCall(class string, params map[string]any) (Caller, error) {
	callerConfig, ok := params["caller_config"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("native_call: 缺少 caller_config")
	}

	protocolType := getStringValue(callerConfig, "protocol", "")
	switch protocolType {
	case "websocket":
		return NewWebSocketCall(callerConfig, params)
	case "http":
		return NewHTTPCall(callerConfig, params)
	default:
		return nil, fmt.Errorf("native_call: 不支持的协议 %q", protocolType)
	}
}
