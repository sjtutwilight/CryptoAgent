package tracing

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
)

type TraceContext struct {
	TraceID      string
	SpanID       string
	ParentSpanID string
	Sampled      bool
	Baggage      map[string]string
}

type ctxKey struct{}

func ContextWithTrace(ctx context.Context, tc TraceContext) context.Context {
	return context.WithValue(ctx, ctxKey{}, tc)
}

func FromContext(ctx context.Context) (TraceContext, bool) {
	if ctx == nil {
		return TraceContext{}, false
	}
	val := ctx.Value(ctxKey{})
	if val == nil {
		return TraceContext{}, false
	}
	tc, ok := val.(TraceContext)
	return tc, ok
}

func NewRoot(sampled bool) TraceContext {
	return TraceContext{
		TraceID: newTraceID(),
		SpanID:  newSpanID(),
		Sampled: sampled,
	}
}

func NewChild(parent TraceContext) TraceContext {
	return TraceContext{
		TraceID:      parent.TraceID,
		SpanID:       newSpanID(),
		ParentSpanID: parent.SpanID,
		Sampled:      parent.Sampled,
		Baggage:      parent.Baggage,
	}
}

func ParseTraceParent(value string) (TraceContext, bool) {
	parts := strings.Split(strings.TrimSpace(value), "-")
	if len(parts) != 4 {
		return TraceContext{}, false
	}
	traceID := parts[1]
	spanID := parts[2]
	flags := parts[3]
	if len(traceID) != 32 || len(spanID) != 16 {
		return TraceContext{}, false
	}
	sampled := strings.HasSuffix(flags, "01")
	return TraceContext{
		TraceID: traceID,
		SpanID:  spanID,
		Sampled: sampled,
	}, true
}

func (t TraceContext) TraceParent() string {
	flags := "00"
	if t.Sampled {
		flags = "01"
	}
	return fmt.Sprintf("00-%s-%s-%s", t.TraceID, t.SpanID, flags)
}

func InjectMetadata(meta map[string]any, tc TraceContext) {
	if meta == nil {
		return
	}
	meta["traceparent"] = tc.TraceParent()
	if len(tc.Baggage) > 0 {
		meta["baggage"] = FormatBaggage(tc.Baggage)
	}
}

func ExtractMetadata(meta map[string]any) (TraceContext, bool) {
	if meta == nil {
		return TraceContext{}, false
	}
	tp, _ := meta["traceparent"].(string)
	tc, ok := ParseTraceParent(tp)
	if !ok {
		return TraceContext{}, false
	}
	if baggage, ok := meta["baggage"].(string); ok {
		tc.Baggage = ParseBaggage(baggage)
	}
	return tc, true
}

func ParseBaggage(value string) map[string]string {
	out := map[string]string{}
	parts := strings.Split(value, ",")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		key, val, ok := strings.Cut(part, "=")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		val = strings.TrimSpace(val)
		if key == "" {
			continue
		}
		out[key] = val
	}
	return out
}

func FormatBaggage(values map[string]string) string {
	if len(values) == 0 {
		return ""
	}
	parts := make([]string, 0, len(values))
	for key, val := range values {
		if key == "" {
			continue
		}
		parts = append(parts, fmt.Sprintf("%s=%s", key, val))
	}
	return strings.Join(parts, ",")
}

func WithBaggage(tc TraceContext, key, value string) TraceContext {
	if key == "" || value == "" {
		return tc
	}
	if tc.Baggage == nil {
		tc.Baggage = map[string]string{}
	}
	tc.Baggage[key] = value
	return tc
}

func newTraceID() string {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return fmt.Sprintf("%032x", buf)
	}
	return hex.EncodeToString(buf)
}

func newSpanID() string {
	buf := make([]byte, 8)
	if _, err := rand.Read(buf); err != nil {
		return fmt.Sprintf("%016x", buf)
	}
	return hex.EncodeToString(buf)
}
