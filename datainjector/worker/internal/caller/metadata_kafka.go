package caller

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"
	"time"

	kafka "github.com/segmentio/kafka-go"

	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/types"
)

func init() {
	Register("metadata_kafka", func(class string, params map[string]any) (Caller, error) {
		return newKafkaMetadataCaller(params)
	})
}

type kafkaMetadataCaller struct {
	brokers         []string
	dialTimeout     time.Duration
	includeInternal bool
	allowedTopics   map[string]struct{}
	fetchOffsets    bool
	clusterName     string
	domain          string
	dialer          *kafka.Dialer
}

type kafkaTopicMetadata struct {
	Cluster           string                   `json:"cluster"`
	Domain            string                   `json:"domain"`
	Topic             string                   `json:"topic"`
	IsInternal        bool                     `json:"is_internal"`
	PartitionCount    int                      `json:"partition_count"`
	ReplicationFactor int                      `json:"replication_factor"`
	Partitions        []kafkaPartitionMetadata `json:"partitions"`
	CollectedAt       time.Time                `json:"collected_at"`
}

type kafkaPartitionMetadata struct {
	ID             int    `json:"id"`
	LeaderID       int    `json:"leader_id"`
	LeaderHost     string `json:"leader_host"`
	Replicas       []int  `json:"replicas"`
	InSyncReplicas []int  `json:"in_sync_replicas"`
	EarliestOffset int64  `json:"earliest_offset,omitempty"`
	LatestOffset   int64  `json:"latest_offset,omitempty"`
}

func newKafkaMetadataCaller(params map[string]any) (*kafkaMetadataCaller, error) {
	brokers := toStringSlice(params["brokers"])
	if len(brokers) == 0 {
		return nil, fmt.Errorf("metadata_kafka: brokers required")
	}

	clusterName := getStringParam(params, "cluster", "default")
	domain := getStringParam(params, "domain", "kafka")
	includeInternal := getBoolParam(params, "include_internal", false)
	fetchOffsets := getBoolParam(params, "fetch_offsets", false)
	timeout := time.Duration(getIntParam(params, "dial_timeout_ms", 5000)) * time.Millisecond
	if timeout <= 0 {
		timeout = 5 * time.Second
	}

	var allowedTopics map[string]struct{}
	if raw := params["topics"]; raw != nil {
		topics := toStringSlice(raw)
		if len(topics) > 0 {
			allowedTopics = make(map[string]struct{}, len(topics))
			for _, t := range topics {
				topic := strings.TrimSpace(t)
				if topic == "" {
					continue
				}
				allowedTopics[topic] = struct{}{}
			}
		}
	}

	return &kafkaMetadataCaller{
		brokers:         brokers,
		dialTimeout:     timeout,
		includeInternal: includeInternal,
		allowedTopics:   allowedTopics,
		fetchOffsets:    fetchOffsets,
		clusterName:     clusterName,
		domain:          domain,
		dialer: &kafka.Dialer{
			Timeout:   timeout,
			DualStack: true,
		},
	}, nil
}

func (c *kafkaMetadataCaller) CallOnce(ctx context.Context, args map[string]any) ([]*types.Message, error) {
	conn, err := c.connectAnyBroker(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	parts, err := conn.ReadPartitions()
	if err != nil {
		return nil, fmt.Errorf("metadata_kafka: read partitions: %w", err)
	}

	topics := c.groupPartitions(parts)
	if len(topics) == 0 {
		return nil, nil
	}

	collectedAt := time.Now().UTC()
	messages := make([]*types.Message, 0, len(topics))

	for topic, partitions := range topics {
		isInternal := strings.HasPrefix(topic, "__")
		meta := kafkaTopicMetadata{
			Cluster:           c.clusterName,
			Domain:            c.domain,
			Topic:             topic,
			IsInternal:        isInternal,
			PartitionCount:    len(partitions),
			ReplicationFactor: c.replicationFactor(partitions),
			Partitions:        make([]kafkaPartitionMetadata, 0, len(partitions)),
			CollectedAt:       collectedAt,
		}

		sort.Slice(partitions, func(i, j int) bool {
			return partitions[i].ID < partitions[j].ID
		})

		for _, p := range partitions {
			partMeta := kafkaPartitionMetadata{
				ID:             p.ID,
				LeaderID:       p.Leader.ID,
				LeaderHost:     net.JoinHostPort(p.Leader.Host, strconv.Itoa(p.Leader.Port)),
				Replicas:       brokerIDs(p.Replicas),
				InSyncReplicas: brokerIDs(p.Isr),
			}

			if c.fetchOffsets {
				if first, last, err := c.fetchPartitionOffsets(ctx, topic, p); err == nil {
					partMeta.EarliestOffset = first
					partMeta.LatestOffset = last
				}
			}

			meta.Partitions = append(meta.Partitions, partMeta)
		}

		payload, err := json.Marshal(meta)
		if err != nil {
			return nil, fmt.Errorf("metadata_kafka: marshal topic %s: %w", topic, err)
		}

		msg := &types.Message{
			Metadata: map[string]any{
				"cluster":     c.clusterName,
				"domain":      c.domain,
				"entity_type": "kafka_topic",
				"topic":       topic,
			},
			Payload: payload,
		}
		messages = append(messages, msg)
	}

	return messages, nil
}

func (c *kafkaMetadataCaller) connectAnyBroker(ctx context.Context) (*kafka.Conn, error) {
	var lastErr error
	for _, addr := range c.brokers {
		dialCtx, cancel := context.WithTimeout(ctx, c.dialTimeout)
		conn, err := c.dialer.DialContext(dialCtx, "tcp", addr)
		cancel()
		if err != nil {
			lastErr = err
			continue
		}
		return conn, nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("metadata_kafka: no brokers provided")
	}
	return nil, fmt.Errorf("metadata_kafka: dial broker failed: %w", lastErr)
}

func (c *kafkaMetadataCaller) groupPartitions(parts []kafka.Partition) map[string][]kafka.Partition {
	topics := make(map[string]map[int]kafka.Partition)
	for _, p := range parts {
		if p.Topic == "" {
			continue
		}
		if !c.includeInternal && strings.HasPrefix(p.Topic, "__") {
			continue
		}
		if len(c.allowedTopics) > 0 {
			if _, ok := c.allowedTopics[p.Topic]; !ok {
				continue
			}
		}

		partitionSet, ok := topics[p.Topic]
		if !ok {
			partitionSet = make(map[int]kafka.Partition)
			topics[p.Topic] = partitionSet
		}
		// 取副本数更多的记录，避免不同 broker 返回重复条目
		if existing, ok := partitionSet[p.ID]; ok {
			if len(existing.Replicas) >= len(p.Replicas) {
				continue
			}
		}
		partitionSet[p.ID] = p
	}

	result := make(map[string][]kafka.Partition, len(topics))
	for topic, partitionSet := range topics {
		list := make([]kafka.Partition, 0, len(partitionSet))
		for _, p := range partitionSet {
			list = append(list, p)
		}
		result[topic] = list
	}
	return result
}

func (c *kafkaMetadataCaller) replicationFactor(parts []kafka.Partition) int {
	if len(parts) == 0 {
		return 0
	}
	max := 0
	for _, p := range parts {
		if len(p.Replicas) > max {
			max = len(p.Replicas)
		}
	}
	return max
}

func (c *kafkaMetadataCaller) fetchPartitionOffsets(ctx context.Context, topic string, partition kafka.Partition) (int64, int64, error) {
	address := net.JoinHostPort(partition.Leader.Host, strconv.Itoa(partition.Leader.Port))
	dialCtx, cancel := context.WithTimeout(ctx, c.dialTimeout)
	defer cancel()

	conn, err := c.dialer.DialLeader(dialCtx, "tcp", address, topic, partition.ID)
	if err != nil {
		return 0, 0, err
	}
	defer conn.Close()

	earliest, err := conn.ReadFirstOffset()
	if err != nil {
		return 0, 0, err
	}
	latest, err := conn.ReadLastOffset()
	if err != nil {
		return earliest, 0, err
	}
	return earliest, latest, nil
}

func copyIntSlice(src []int) []int {
	if len(src) == 0 {
		return nil
	}
	out := make([]int, len(src))
	copy(out, src)
	return out
}

func brokerIDs(brokers []kafka.Broker) []int {
	if len(brokers) == 0 {
		return nil
	}
	ids := make([]int, len(brokers))
	for i, b := range brokers {
		ids[i] = b.ID
	}
	return ids
}

func getStringParam(params map[string]any, key, def string) string {
	if params == nil {
		return def
	}
	if v, ok := params[key]; ok {
		switch vv := v.(type) {
		case string:
			if vv != "" {
				return vv
			}
		}
	}
	return def
}

func getBoolParam(params map[string]any, key string, def bool) bool {
	if params == nil {
		return def
	}
	if v, ok := params[key]; ok {
		switch vv := v.(type) {
		case bool:
			return vv
		case string:
			lower := strings.ToLower(vv)
			if lower == "true" || lower == "1" || lower == "yes" {
				return true
			}
			if lower == "false" || lower == "0" || lower == "no" {
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

func getIntParam(params map[string]any, key string, def int) int {
	if params == nil {
		return def
	}
	if v, ok := params[key]; ok {
		switch vv := v.(type) {
		case int:
			return vv
		case int64:
			return int(vv)
		case float64:
			return int(vv)
		case string:
			if n, err := strconv.Atoi(strings.TrimSpace(vv)); err == nil {
				return n
			}
		}
	}
	return def
}

func toStringSlice(v interface{}) []string {
	switch vv := v.(type) {
	case []string:
		return append([]string(nil), vv...)
	case []any:
		out := make([]string, 0, len(vv))
		for _, item := range vv {
			if s, ok := item.(string); ok {
				s = strings.TrimSpace(s)
				if s == "" {
					continue
				}
				out = append(out, s)
			}
		}
		return out
	case string:
		trimmed := strings.TrimSpace(vv)
		if trimmed == "" {
			return nil
		}
		return []string{trimmed}
	default:
		return nil
	}
}
