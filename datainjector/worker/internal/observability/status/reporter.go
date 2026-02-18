package status

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	kafka "github.com/segmentio/kafka-go"

	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/config"
	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/observability/logging"
)

// Event 表示要上报给控制面的任务状态。
type Event struct {
	TaskID     string    `json:"taskId"`
	Status     string    `json:"status"`
	StatusCode int       `json:"statusCode,omitempty"`
	Message    string    `json:"message,omitempty"`
	DurationMs int64     `json:"durationMs,omitempty"`
	DataSize   int       `json:"dataSize,omitempty"`
	Retryable  bool      `json:"retryable,omitempty"`
	Stage      string    `json:"stage,omitempty"`
	ErrorClass string    `json:"errorClass,omitempty"`
	Attempt    int       `json:"attempt,omitempty"`
	RoleID     string    `json:"roleId,omitempty"`
	RunID      string    `json:"runId,omitempty"`
	Timestamp  time.Time `json:"timestamp"`
}

type Reporter interface {
	Report(ctx context.Context, event Event) error
	Close() error
}

type kafkaReporter struct {
	topic  string
	writer *kafka.Writer
}

func (r *kafkaReporter) Report(ctx context.Context, event Event) error {
	if r == nil {
		return nil
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal status event: %w", err)
	}
	msg := kafka.Message{
		Key:   []byte(event.TaskID),
		Value: payload,
		Time:  time.Now(),
	}
	return r.writer.WriteMessages(ctx, msg)
}

func (r *kafkaReporter) Close() error {
	if r == nil || r.writer == nil {
		return nil
	}
	return r.writer.Close()
}

var (
	globalReporter     Reporter
	globalReporterOnce sync.Once
)

// Init 初始化全局状态上报器。
func Init(cfg config.StatusReporterConfig) {
	globalReporterOnce.Do(func() {
		if !cfg.Enabled {
			logging.Info(context.Background(), logging.EventStatusDisabled, "status reporter disabled", nil)
			return
		}
		if len(cfg.Brokers) == 0 {
			logging.Warn(context.Background(), logging.EventStatusDisabled, "status reporter enabled but no brokers configured", nil)
			return
		}
		if cfg.Topic == "" {
			cfg.Topic = "tasks.status"
		}
		writer := &kafka.Writer{
			Addr:         kafka.TCP(cfg.Brokers...),
			Topic:        cfg.Topic,
			RequiredAcks: kafka.RequireAll,
			Balancer:     &kafka.Hash{},
		}
		globalReporter = &kafkaReporter{
			topic:  cfg.Topic,
			writer: writer,
		}
		logging.Info(context.Background(), logging.EventStatusInit, "status reporter initialized", logging.Fields{
			"topic":   cfg.Topic,
			"brokers": cfg.Brokers,
		})
	})
}

// Get 返回全局 Reporter，如果未初始化则返回空实现。
func Get() Reporter {
	return globalReporter
}

// Close 关闭全局 Reporter（如果存在）。
func Close() {
	if r, ok := globalReporter.(*kafkaReporter); ok {
		if err := r.Close(); err != nil {
			logging.Warn(context.Background(), logging.EventStatusCloseErr, "status reporter close error", logging.Fields{
				"error": err.Error(),
			})
		}
	}
}
