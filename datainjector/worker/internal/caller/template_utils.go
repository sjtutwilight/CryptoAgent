package caller

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

var placeholderPattern = regexp.MustCompile(`\{\{\s*([^{}\s]+(?:\.[^{}\s]+)*)\s*\}\}`)

type mapTemplate struct {
	data map[string]any
}

func newMapTemplate(raw map[string]any) mapTemplate {
	if raw == nil {
		raw = map[string]any{}
	}
	return mapTemplate{data: raw}
}

func (t mapTemplate) render(ctx map[string]any) (map[string]any, error) {
	if len(t.data) == 0 {
		return map[string]any{}, nil
	}
	out := make(map[string]any, len(t.data))
	for k, v := range t.data {
		val, err := renderTemplateValue(v, ctx)
		if err != nil {
			return nil, err
		}
		out[k] = val
	}
	return out, nil
}

func renderTemplateValue(value interface{}, ctx map[string]any) (interface{}, error) {
	switch v := value.(type) {
	case string:
		return renderTemplateString(v, ctx), nil
	case map[string]any:
		return renderTemplateMap(v, ctx)
	case map[interface{}]interface{}:
		m := make(map[string]any, len(v))
		for key, val := range v {
			m[fmt.Sprint(key)] = val
		}
		return renderTemplateMap(m, ctx)
	case []interface{}:
		return renderTemplateSlice(v, ctx)
	default:
		return v, nil
	}
}

func renderTemplateMap(m map[string]any, ctx map[string]any) (map[string]any, error) {
	out := make(map[string]any, len(m))
	for key, val := range m {
		rendered, err := renderTemplateValue(val, ctx)
		if err != nil {
			return nil, err
		}
		out[key] = rendered
	}
	return out, nil
}

func renderTemplateSlice(arr []interface{}, ctx map[string]any) ([]interface{}, error) {
	out := make([]interface{}, len(arr))
	for i, item := range arr {
		rendered, err := renderTemplateValue(item, ctx)
		if err != nil {
			return nil, err
		}
		out[i] = rendered
	}
	return out, nil
}

func renderTemplateString(s string, ctx map[string]any) interface{} {
	if !strings.Contains(s, "{{") {
		return s
	}

	matches := placeholderPattern.FindAllStringSubmatchIndex(s, -1)
	if len(matches) == 1 && matches[0][0] == 0 && matches[0][1] == len(s) {
		if key := extractPlaceholderKey(s); key != "" {
			if val, ok := lookupContextValue(ctx, key); ok {
				return val
			}
		}
		return ""
	}

	result := placeholderPattern.ReplaceAllStringFunc(s, func(match string) string {
		key := extractPlaceholderKey(match)
		if key == "" {
			return ""
		}
		val, ok := lookupContextValue(ctx, key)
		if !ok || val == nil {
			return ""
		}
		return fmt.Sprint(val)
	})
	return result
}

func extractPlaceholderKey(raw string) string {
	raw = strings.TrimSpace(raw)
	if !strings.HasPrefix(raw, "{{") || !strings.HasSuffix(raw, "}}") {
		return ""
	}
	raw = strings.TrimPrefix(raw, "{{")
	raw = strings.TrimSuffix(raw, "}}")
	return strings.TrimSpace(raw)
}

func lookupContextValue(ctx map[string]any, path string) (interface{}, bool) {
	if ctx == nil {
		return nil, false
	}
	segments := strings.Split(path, ".")
	var current interface{} = ctx
	for _, segment := range segments {
		if segment == "" {
			continue
		}
		switch typed := current.(type) {
		case map[string]any:
			next, ok := typed[segment]
			if !ok {
				return nil, false
			}
			current = next
		case map[interface{}]interface{}:
			next, ok := typed[segment]
			if !ok {
				return nil, false
			}
			current = next
		default:
			return nil, false
		}
	}
	return current, true
}

func cloneAnyMap(src map[string]any) map[string]any {
	if len(src) == 0 {
		return map[string]any{}
	}
	dst := make(map[string]any, len(src))
	for k, v := range src {
		dst[k] = deepCloneValue(v)
	}
	return dst
}

func deepCloneValue(v interface{}) interface{} {
	switch vv := v.(type) {
	case map[string]any:
		return cloneAnyMap(vv)
	case []interface{}:
		out := make([]interface{}, len(vv))
		for i := range vv {
			out[i] = deepCloneValue(vv[i])
		}
		return out
	default:
		return vv
	}
}

func mergeIntoAnyMap(dst map[string]any, src interface{}) map[string]any {
	switch v := src.(type) {
	case map[string]any:
		for k, val := range v {
			dst[k] = deepCloneValue(val)
		}
	case map[interface{}]interface{}:
		for k, val := range v {
			dst[fmt.Sprint(k)] = deepCloneValue(val)
		}
	}
	return dst
}

func toMapStringAny(src interface{}) map[string]any {
	switch v := src.(type) {
	case nil:
		return map[string]any{}
	case map[string]any:
		return v
	case map[interface{}]interface{}:
		m := make(map[string]any, len(v))
		for k, val := range v {
			m[fmt.Sprint(k)] = val
		}
		return m
	default:
		return map[string]any{}
	}
}

func parseScalarBool(val interface{}, def bool) bool {
	switch v := val.(type) {
	case bool:
		return v
	case string:
		switch strings.ToLower(strings.TrimSpace(v)) {
		case "1", "true", "yes", "y", "on":
			return true
		case "0", "false", "no", "off":
			return false
		}
	case int:
		return v != 0
	case int64:
		return v != 0
	case float64:
		return v != 0
	}
	return def
}

func parseScalarInt(val interface{}, def int64) int64 {
	switch v := val.(type) {
	case int:
		return int64(v)
	case int64:
		return v
	case float64:
		return int64(v)
	case string:
		if strings.HasPrefix(v, "0x") || strings.HasPrefix(v, "0X") {
			if n, err := strconv.ParseInt(v[2:], 16, 64); err == nil {
				return n
			}
			return def
		}
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n
		}
	}
	return def
}
