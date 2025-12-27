package logging

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/observability/tracing"
)

type Config struct {
	ServiceName string `yaml:"service_name" json:"service_name"`
	Environment string `yaml:"environment" json:"environment"`
}

type Fields map[string]any

var serviceName = "datainjector-worker"
var environment = ""

func Init(cfg Config) {
	if cfg.ServiceName != "" {
		serviceName = cfg.ServiceName
	}
	if cfg.Environment != "" {
		environment = cfg.Environment
	}
}

func Info(ctx context.Context, event string, message string, fields Fields) {
	logJSON(ctx, "INFO", event, message, fields, nil)
}

func Warn(ctx context.Context, event string, message string, fields Fields) {
	logJSON(ctx, "WARN", event, message, fields, nil)
}

func Error(ctx context.Context, event string, message string, err error, fields Fields) {
	logJSON(ctx, "ERROR", event, message, fields, err)
}

func logJSON(ctx context.Context, level string, event string, message string, fields Fields, err error) {
	payload := map[string]any{
		"ts":      time.Now().UTC().Format(time.RFC3339Nano),
		"level":   level,
		"event":   event,
		"message": message,
		"service": serviceName,
	}
	if environment != "" {
		payload["env"] = environment
	}

	for k, v := range fields {
		if k == "" {
			continue
		}
		payload[k] = v
	}

	if err != nil {
		payload["error_type"] = fmt.Sprintf("%T", err)
		payload["error_detail"] = err.Error()
	}

	if tc, ok := tracing.FromContext(ctx); ok {
		payload["trace_id"] = tc.TraceID
		payload["span_id"] = tc.SpanID
		if tc.ParentSpanID != "" {
			payload["parent_span_id"] = tc.ParentSpanID
		}
	}

	line, err := json.Marshal(payload)
	if err != nil {
		log.Printf("log marshal error: %v", err)
		return
	}
	log.Print(string(line))
}
