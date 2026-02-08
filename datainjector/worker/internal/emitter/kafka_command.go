package emitter

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

type KafkaCommandConfig struct {
	Brokers  []string
	Topic    string
	GroupID  string
	MinBytes int
	MaxBytes int
}

type pollTask struct {
	ticker   *time.Ticker
	cancel   context.CancelFunc
	deadline time.Time
	params   map[string]any
}

type KafkaCommand struct {
	reader      *kafka.Reader
	activeTasks map[string]*pollTask
	mu          sync.RWMutex
}

func NewKafkaCommand(cfg KafkaCommandConfig) (*KafkaCommand, error) {
	// 如果配置中没有 brokers，尝试从环境变量获取
	if len(cfg.Brokers) == 0 {
		cfg.Brokers = brokersFromEnv()
	}
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("kafka_command: brokers required (set KAFKA_BROKERS env or provide in config)")
	}
	if cfg.Topic == "" {
		return nil, fmt.Errorf("kafka_command: topic required")
	}
	if cfg.GroupID == "" {
		cfg.GroupID = fmt.Sprintf("worker-%s", cfg.Topic)
	}
	if cfg.MinBytes <= 0 {
		cfg.MinBytes = 1
	}
	if cfg.MaxBytes <= 0 {
		cfg.MaxBytes = 10 << 20 // 10MB
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  cfg.Brokers,
		Topic:    cfg.Topic,
		GroupID:  cfg.GroupID,
		MinBytes: cfg.MinBytes,
		MaxBytes: cfg.MaxBytes,
	})

	return &KafkaCommand{
		reader:      reader,
		activeTasks: make(map[string]*pollTask),
	}, nil
}

func (k *KafkaCommand) Start(ctx context.Context, fire func(args map[string]any)) error {
	defer k.reader.Close()
	defer k.stopAllTasks()

	for {
		m, err := k.reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("kafka_command: read message failed: %w", err)
		}

		log.Printf("[KafkaCommand] 收到消息: topic=%s partition=%d offset=%d size=%d",
			m.Topic, m.Partition, m.Offset, len(m.Value))

		var payload map[string]any
		if err := json.Unmarshal(m.Value, &payload); err != nil {
			log.Printf("[KafkaCommand] invalid message, offset=%d error=%v, raw=%s", m.Offset, err, string(m.Value))
			_ = k.reader.CommitMessages(ctx, m)
			continue
		}
		
		// 提取 headers 到 metadata
		meta := map[string]any{}
		for _, header := range m.Headers {
			key := strings.ToLower(header.Key)
			switch key {
			case "traceparent", "tracestate", "baggage", "x-run-id":
				meta[key] = string(header.Value)
			}
		}
		if v, ok := meta["x-run-id"]; ok {
			meta["run_id"] = v
		}
		if len(meta) > 0 {
			if existing, ok := payload["metadata"].(map[string]any); ok && existing != nil {
				for k, v := range meta {
					if _, exists := existing[k]; !exists {
						existing[k] = v
					}
				}
				payload["metadata"] = existing
			} else {
				payload["metadata"] = meta
			}
		}

		log.Printf("[KafkaCommand] 解析成功，payload keys: %v", getKeys(payload))
		
		// 处理不同的 action
		action := getStringValue(payload, "action", "poll_once")
		taskID := getStringValue(payload, "task_id", "")
		
		switch action {
		case "start_poll":
			k.handleStartPoll(ctx, taskID, payload, fire)
		case "stop_poll":
			k.handleStopPoll(taskID)
		case "poll_once":
			fallthrough
		default:
			// 默认行为：触发一次
			fire(payload)
			log.Printf("[KafkaCommand] fire() 调用完成 (poll_once)")
		}

		if err := k.reader.CommitMessages(ctx, m); err != nil {
			log.Printf("[KafkaCommand] commit failed offset=%d err=%v", m.Offset, err)
		}
	}
}

func (k *KafkaCommand) handleStartPoll(ctx context.Context, taskID string, payload map[string]any, fire func(args map[string]any)) {
	if taskID == "" {
		log.Printf("[KafkaCommand] start_poll: task_id is required")
		return
	}
	
	// 检查是否已存在
	k.mu.RLock()
	if _, exists := k.activeTasks[taskID]; exists {
		k.mu.RUnlock()
		log.Printf("[KafkaCommand] start_poll: task %s already running", taskID)
		return
	}
	k.mu.RUnlock()
	
	// 获取配置
	pollIntervalMs := getIntValue(payload, "poll_interval_ms", 5000)
	durationMs := getIntValue(payload, "duration_ms", 0)
	
	pollInterval := time.Duration(pollIntervalMs) * time.Millisecond
	if pollInterval < 100*time.Millisecond {
		pollInterval = 100 * time.Millisecond
	}
	
	// 提取 task_params
	taskParams := make(map[string]any)
	if params, ok := payload["task_params"].(map[string]any); ok {
		for k, v := range params {
			taskParams[k] = v
		}
	}
	// 保留 task_id 和 metadata
	taskParams["task_id"] = taskID
	if meta, ok := payload["metadata"]; ok {
		taskParams["metadata"] = meta
	}
	
	// 创建 task context
	taskCtx, cancel := context.WithCancel(ctx)
	ticker := time.NewTicker(pollInterval)
	
	task := &pollTask{
		ticker: ticker,
		cancel: cancel,
		params: taskParams,
	}
	if durationMs > 0 {
		task.deadline = time.Now().Add(time.Duration(durationMs) * time.Millisecond)
	}
	
	k.mu.Lock()
	k.activeTasks[taskID] = task
	k.mu.Unlock()
	
	log.Printf("[KafkaCommand] start_poll: task %s started, interval=%v, duration=%v", 
		taskID, pollInterval, durationMs)
	
	// 启动周期性任务
	go func() {
		defer func() {
			ticker.Stop()
			k.mu.Lock()
			delete(k.activeTasks, taskID)
			k.mu.Unlock()
			log.Printf("[KafkaCommand] start_poll: task %s stopped", taskID)
		}()
		
		// 立即触发一次
		fire(cloneParams(taskParams))
		
		for {
			select {
			case <-taskCtx.Done():
				return
			case <-ticker.C:
				// 检查是否超时
				if !task.deadline.IsZero() && time.Now().After(task.deadline) {
					log.Printf("[KafkaCommand] start_poll: task %s reached deadline", taskID)
					return
				}
				fire(cloneParams(taskParams))
			}
		}
	}()
}

func (k *KafkaCommand) handleStopPoll(taskID string) {
	if taskID == "" {
		log.Printf("[KafkaCommand] stop_poll: task_id is required")
		return
	}
	
	k.mu.Lock()
	task, exists := k.activeTasks[taskID]
	if exists {
		task.cancel()
		delete(k.activeTasks, taskID)
	}
	k.mu.Unlock()
	
	if exists {
		log.Printf("[KafkaCommand] stop_poll: task %s stopped", taskID)
	} else {
		log.Printf("[KafkaCommand] stop_poll: task %s not found", taskID)
	}
}

func (k *KafkaCommand) stopAllTasks() {
	k.mu.Lock()
	defer k.mu.Unlock()
	
	for taskID, task := range k.activeTasks {
		task.cancel()
		log.Printf("[KafkaCommand] stopping task %s", taskID)
	}
	k.activeTasks = make(map[string]*pollTask)
}

func getKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// brokersFromEnv 从环境变量获取 Kafka brokers
// 支持的环境变量（按优先级）：
// - KAFKA_BROKERS
// - KAFKA_BOOTSTRAP_SERVERS
// - KAFKA_BOOTSTRAP_SERVERS_LOCAL
func brokersFromEnv() []string {
	envKeys := []string{
		"KAFKA_BROKERS",
		"KAFKA_BOOTSTRAP_SERVERS",
		"KAFKA_BOOTSTRAP_SERVERS_LOCAL",
	}
	for _, key := range envKeys {
		raw := strings.TrimSpace(os.Getenv(key))
		if raw == "" {
			continue
		}
		// 支持逗号、分号、空格分隔
		parts := strings.FieldsFunc(raw, func(r rune) bool {
			return r == ',' || r == ';' || r == ' '
		})
		brokers := make([]string, 0, len(parts))
		for _, p := range parts {
			if v := strings.TrimSpace(p); v != "" {
				brokers = append(brokers, v)
			}
		}
		if len(brokers) > 0 {
			return brokers
		}
	}
	return nil
}

func getStringValue(m map[string]any, key, defaultVal string) string {
	if m == nil {
		return defaultVal
	}
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return defaultVal
}

func getIntValue(m map[string]any, key string, defaultVal int) int {
	if m == nil {
		return defaultVal
	}
	if v, ok := m[key]; ok {
		switch vv := v.(type) {
		case int:
			return vv
		case int64:
			return int(vv)
		case float64:
			return int(vv)
		}
	}
	return defaultVal
}

func cloneParams(src map[string]any) map[string]any {
	if src == nil {
		return nil
	}
	dst := make(map[string]any, len(src))
	for k, v := range src {
		// 简单拷贝，不做深拷贝
		dst[k] = v
	}
	return dst
}
