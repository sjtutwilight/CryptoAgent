package sink

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	kafka "github.com/segmentio/kafka-go"

	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/types"
	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/util"
)

type KafkaConfig struct {
	Brokers      []string
	Topic        string
	KeyFrom      []string
	Linger       time.Duration
	WriteTimeout time.Duration
	TopicField   string
	TopicMap     map[string]string
}

type Kafka struct {
	defaultWriter *kafka.Writer
	brokers       []string
	defaultTopic  string
	keyFrom       []string
	linger        time.Duration
	writeTimeout  time.Duration
	topicField    string
	topicMap      map[string]string

	mu      sync.Mutex
	writers map[string]*kafka.Writer
}

func init() {
	Register("kafka", func(cfg map[string]any) (Sink, error) {
		kc, err := parseKafkaConfig(cfg)
		if err != nil {
			return nil, err
		}
		writer := kafka.NewWriter(kafka.WriterConfig{
			Brokers:      kc.Brokers,
			Topic:        kc.Topic,
			Balancer:     &kafka.Hash{},
			BatchTimeout: kc.Linger,
		})
		return &Kafka{
			defaultWriter: writer,
			brokers:       kc.Brokers,
			defaultTopic:  kc.Topic,
			keyFrom:       kc.KeyFrom,
			linger:        kc.Linger,
			writeTimeout:  kc.WriteTimeout,
			topicField:    kc.TopicField,
			topicMap:      kc.TopicMap,
			writers:       make(map[string]*kafka.Writer),
		}, nil
	})
}

func parseKafkaConfig(cfg map[string]any) (*KafkaConfig, error) {
	kc := &KafkaConfig{}
	switch brokers := cfg["brokers"].(type) {
	case []any:
		for _, b := range brokers {
			if s, ok := b.(string); ok {
				kc.Brokers = append(kc.Brokers, s)
			}
		}
	case []string:
		kc.Brokers = append(kc.Brokers, brokers...)
	case string:
		if brokers != "" {
			kc.Brokers = append(kc.Brokers, brokers)
		}
	}
	if len(kc.Brokers) == 0 {
		kc.Brokers = brokersFromEnv()
	}
	if len(kc.Brokers) == 0 {
		return nil, fmt.Errorf("kafka sink: brokers required")
	}
	if topic, ok := cfg["topic"].(string); ok {
		kc.Topic = topic
	}
	if kc.Topic == "" {
		return nil, fmt.Errorf("kafka sink: topic required")
	}
	switch key := cfg["key_from"].(type) {
	case []any:
		for _, v := range key {
			if s, ok := v.(string); ok {
				kc.KeyFrom = append(kc.KeyFrom, s)
			}
		}
	case []string:
		kc.KeyFrom = append(kc.KeyFrom, key...)
	case string:
		if key != "" {
			kc.KeyFrom = append(kc.KeyFrom, key)
		}
	}
	kc.Linger = 50 * time.Millisecond
	if linger, ok := cfg["linger_ms"]; ok {
		switch v := linger.(type) {
		case int:
			if v > 0 {
				kc.Linger = time.Duration(v) * time.Millisecond
			}
		case int64:
			if v > 0 {
				kc.Linger = time.Duration(v) * time.Millisecond
			}
		case float64:
			if v > 0 {
				kc.Linger = time.Duration(v) * time.Millisecond
			}
		}
	}
	kc.WriteTimeout = 5 * time.Second
	if timeout, ok := cfg["write_timeout_ms"]; ok {
		switch v := timeout.(type) {
		case int:
			if v > 0 {
				kc.WriteTimeout = time.Duration(v) * time.Millisecond
			}
		case int64:
			if v > 0 {
				kc.WriteTimeout = time.Duration(v) * time.Millisecond
			}
		case float64:
			if v > 0 {
				kc.WriteTimeout = time.Duration(v) * time.Millisecond
			}
		}
	}
	if topicField, ok := cfg["topic_field"].(string); ok {
		if tf := strings.TrimSpace(topicField); tf != "" {
			kc.TopicField = tf
		}
	}
	if topicMap, ok := cfg["topic_map"].(map[string]any); ok {
		kc.TopicMap = make(map[string]string, len(topicMap))
		for k, v := range topicMap {
			if s, ok := v.(string); ok {
				if trimmed := strings.TrimSpace(s); trimmed != "" {
					kc.TopicMap[k] = trimmed
				}
			}
		}
	}
	return kc, nil
}

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

func (k *Kafka) Write(msg *types.Message) error {
	writer, _, err := k.writerForMessage(msg)
	if err != nil {
		return err
	}
	ctx := context.Background()
	cancel := func() {}
	if k.writeTimeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, k.writeTimeout)
	}
	defer cancel()
	key := k.buildKey(msg)
	var payload []byte
	var metadata map[string]any
	if msg != nil {
		payload = msg.Payload
		metadata = msg.Metadata
	}
	return writer.WriteMessages(ctx, kafka.Message{
		Key:     []byte(key),
		Value:   payload,
		Headers: buildHeaders(metadata),
	})
}

func (k *Kafka) WriteBatch(msgs []*types.Message) error {
	if len(msgs) == 0 {
		return nil
	}
	type topicBatch struct {
		writer *kafka.Writer
		items  []kafka.Message
	}
	batches := make(map[string]*topicBatch)
	for _, msg := range msgs {
		if msg == nil {
			continue
		}
		writer, topic, err := k.writerForMessage(msg)
		if err != nil {
			return err
		}
		batch, ok := batches[topic]
		if !ok {
			batch = &topicBatch{
				writer: writer,
				items:  make([]kafka.Message, 0, 64),
			}
			batches[topic] = batch
		}
		key := k.buildKey(msg)
		batch.items = append(batch.items, kafka.Message{
			Key:     []byte(key),
			Value:   msg.Payload,
			Headers: buildHeaders(msg.Metadata),
		})
	}

	for _, batch := range batches {
		if batch == nil || batch.writer == nil || len(batch.items) == 0 {
			continue
		}
		ctx := context.Background()
		cancel := func() {}
		if k.writeTimeout > 0 {
			ctx, cancel = context.WithTimeout(ctx, k.writeTimeout)
		}
		err := batch.writer.WriteMessages(ctx, batch.items...)
		cancel()
		if err != nil {
			return err
		}
	}
	return nil
}

func (k *Kafka) Close() error {
	var err error
	if k.defaultWriter != nil {
		if cerr := k.defaultWriter.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	for topic, writer := range k.writers {
		if writer == nil {
			continue
		}
		if cerr := writer.Close(); cerr != nil && err == nil {
			err = cerr
		}
		delete(k.writers, topic)
	}
	return err
}

func (k *Kafka) buildKey(msg *types.Message) string {
	if len(k.keyFrom) == 0 {
		return ""
	}
	if msg == nil {
		return ""
	}
	parts := make([]string, 0, len(k.keyFrom))
	for _, field := range k.keyFrom {
		if msg.Metadata != nil {
			if v, ok := msg.Metadata[field]; ok {
				switch vv := v.(type) {
				case string:
					parts = append(parts, vv)
				case fmt.Stringer:
					parts = append(parts, vv.String())
				default:
					b, _ := json.Marshal(vv)
					parts = append(parts, string(b))
				}
				continue
			}
		}
		parts = append(parts, "")
	}
	return strings.Join(parts, "|")
}

func buildHeaders(meta map[string]any) []kafka.Header {
	if meta == nil {
		return nil
	}
	headers := make([]kafka.Header, 0, 4)
	if v, ok := meta["traceparent"]; ok {
		if s := fmt.Sprint(v); s != "" {
			headers = append(headers, kafka.Header{Key: "traceparent", Value: []byte(s)})
		}
	}
	if v, ok := meta["tracestate"]; ok {
		if s := fmt.Sprint(v); s != "" {
			headers = append(headers, kafka.Header{Key: "tracestate", Value: []byte(s)})
		}
	}
	if v, ok := meta["baggage"]; ok {
		if s := fmt.Sprint(v); s != "" {
			headers = append(headers, kafka.Header{Key: "baggage", Value: []byte(s)})
		}
	}
	if v, ok := meta["run_id"]; ok {
		if s := fmt.Sprint(v); s != "" {
			headers = append(headers, kafka.Header{Key: "x-run-id", Value: []byte(s)})
		}
	}
	return headers
}

func (k *Kafka) writerForMessage(msg *types.Message) (*kafka.Writer, string, error) {
	if k.defaultWriter == nil {
		return nil, "", fmt.Errorf("kafka sink not initialized")
	}
	topic := k.defaultTopic
	if k.topicField != "" && msg != nil && msg.Metadata != nil {
		if val, ok := msg.Metadata[k.topicField]; ok {
			str := strings.TrimSpace(util.ToString(val))
			if mapped, ok := k.topicMap[str]; ok && strings.TrimSpace(mapped) != "" {
				str = strings.TrimSpace(mapped)
			}
			if str != "" {
				topic = str
			}
		}
	}
	if topic == "" {
		return nil, "", fmt.Errorf("kafka sink: topic not resolved")
	}
	if topic == k.defaultTopic {
		return k.defaultWriter, topic, nil
	}

	k.mu.Lock()
	defer k.mu.Unlock()
	if writer, ok := k.writers[topic]; ok {
		return writer, topic, nil
	}
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:      k.brokers,
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		BatchTimeout: k.linger,
	})
	k.writers[topic] = writer
	return writer, topic, nil
}
