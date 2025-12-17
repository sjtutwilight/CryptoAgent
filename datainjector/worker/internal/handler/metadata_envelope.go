package handler

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/types"
)

func init() {
	Register("metadata_envelope", func(cfg map[string]any) (Handler, error) {
		return newMetadataEnvelopeHandler(cfg)
	})
}

type metadataEnvelopeHandler struct {
	entityType string
	platform   string
	status     string
	domain     string
}

type tablePayload struct {
	Cluster      string          `json:"cluster"`
	Domain       string          `json:"domain"`
	Database     string          `json:"database"`
	Schema       string          `json:"schema"`
	Table        string          `json:"table"`
	TableType    string          `json:"table_type"`
	Engine       string          `json:"engine"`
	PartitionKey string          `json:"partition_key"`
	PrimaryKey   json.RawMessage `json:"primary_key"`
	CollectedAt  time.Time       `json:"collected_at"`
	Columns      json.RawMessage `json:"columns"`
}

type metadataEnvelope struct {
	Entity     metadataEntityPayload      `json:"entity"`
	Attributes []metadataAttributePayload `json:"attributes,omitempty"`
	Tags       []metadataTagPayload       `json:"tags,omitempty"`
	Lineage    []metadataLineagePayload   `json:"lineage,omitempty"`
	Quality    *metadataQualityPayload    `json:"quality,omitempty"`
	OccurredAt time.Time                  `json:"occurredAt"`
}

type metadataEntityPayload struct {
	ID              uuid.UUID `json:"id"`
	Type            string    `json:"type"`
	Name            string    `json:"name"`
	Domain          string    `json:"domain"`
	Platform        string    `json:"platform"`
	Locator         string    `json:"locator"`
	Version         string    `json:"version,omitempty"`
	Status          string    `json:"status,omitempty"`
	Protocol        string    `json:"protocol,omitempty"`
	ChainID         string    `json:"chainId,omitempty"`
	ContractAddress string    `json:"contractAddress,omitempty"`
	Cluster         string    `json:"cluster,omitempty"`
	DbName          string    `json:"dbName,omitempty"`
	Topic           string    `json:"topic,omitempty"`
	JobID           string    `json:"jobId,omitempty"`
	Description     string    `json:"description,omitempty"`
}

type metadataAttributePayload struct {
	Key   string `json:"key"`
	Value string `json:"value"`
	Level string `json:"level,omitempty"`
}

type metadataTagPayload struct {
	Value string `json:"value"`
}

type metadataLineagePayload struct {
	UpstreamID   uuid.UUID `json:"upstreamId"`
	DownstreamID uuid.UUID `json:"downstreamId"`
	RelationType string    `json:"relationType"`
	Confidence   float64   `json:"confidence"`
}

type metadataQualityPayload struct {
	EntityID uuid.UUID `json:"entityId"`
	Metric   string    `json:"metric"`
	Value    float64   `json:"value"`
	Status   string    `json:"status"`
}

var metadataNamespace = uuid.MustParse("b1a2dbb0-6f1e-4d4d-8bd9-3d76d481a6f7")

func newMetadataEnvelopeHandler(cfg map[string]any) (*metadataEnvelopeHandler, error) {
	entityType := getString(cfg, "entity_type", "table")
	platform := getString(cfg, "platform", "")
	status := strings.ToUpper(getString(cfg, "status", "ACTIVE"))
	domain := getString(cfg, "domain", "")
	return &metadataEnvelopeHandler{
		entityType: entityType,
		platform:   platform,
		status:     status,
		domain:     domain,
	}, nil
}

func (h *metadataEnvelopeHandler) Handle(msg *types.Message) ([]*types.Message, error) {
	var payload tablePayload
	if err := json.Unmarshal(msg.Payload, &payload); err != nil {
		return nil, fmt.Errorf("metadata_envelope: decode payload: %w", err)
	}

	entityName := buildEntityName(payload.Database, payload.Schema, payload.Table)
	if entityName == "" {
		return nil, fmt.Errorf("metadata_envelope: missing table identifiers")
	}

	domain := h.domain
	if domain == "" {
		domain = payload.Domain
	}
	if domain == "" {
		domain = "metadata"
	}

	platform := h.platform
	if platform == "" {
		platform = domain
	}

	locator := fmt.Sprintf("%s://%s/%s", platform, safeValue(payload.Cluster), entityName)
	idSeed := strings.Join([]string{platform, payload.Cluster, entityName}, "|")
	entityID := uuid.NewSHA1(metadataNamespace, []byte(idSeed))
	version := ""
	occurredAt := time.Now().UTC()
	if !payload.CollectedAt.IsZero() {
		version = payload.CollectedAt.Format(time.RFC3339Nano)
		occurredAt = payload.CollectedAt
	}

	entity := metadataEntityPayload{
		ID:          entityID,
		Type:        h.entityType,
		Name:        entityName,
		Domain:      domain,
		Platform:    platform,
		Locator:     locator,
		Version:     version,
		Status:      h.status,
		Cluster:     payload.Cluster,
		DbName:      payload.Database,
		Description: fmt.Sprintf("%s table %s", strings.ToUpper(platform), entityName),
	}

	attributes := make([]metadataAttributePayload, 0, 4)
	attributes = append(attributes, metadataAttributePayload{
		Key:   "raw_payload",
		Value: string(msg.Payload),
		Level: "table",
	})

	if len(payload.PrimaryKey) > 0 && string(payload.PrimaryKey) != "null" {
		attributes = append(attributes, metadataAttributePayload{
			Key:   "primary_key",
			Value: string(payload.PrimaryKey),
			Level: "table",
		})
	}
	if payload.TableType != "" {
		attributes = append(attributes, metadataAttributePayload{
			Key:   "table_type",
			Value: fmt.Sprintf("%q", payload.TableType),
			Level: "table",
		})
	}
	if payload.Engine != "" {
		attributes = append(attributes, metadataAttributePayload{
			Key:   "engine",
			Value: fmt.Sprintf("%q", payload.Engine),
			Level: "table",
		})
	}
	if payload.PartitionKey != "" {
		attributes = append(attributes, metadataAttributePayload{
			Key:   "partition_key",
			Value: fmt.Sprintf("%q", payload.PartitionKey),
			Level: "table",
		})
	}
	if len(payload.Columns) > 0 && string(payload.Columns) != "null" {
		attributes = append(attributes, metadataAttributePayload{
			Key:   "columns",
			Value: string(payload.Columns),
			Level: "column",
		})
	}

	envelope := metadataEnvelope{
		Entity:     entity,
		Attributes: attributes,
		OccurredAt: occurredAt,
	}

	buf, err := json.Marshal(envelope)
	if err != nil {
		return nil, fmt.Errorf("metadata_envelope: marshal envelope: %w", err)
	}

	return []*types.Message{{
		Metadata: msg.Metadata,
		Payload:  buf,
	}}, nil
}

func buildEntityName(database, schema, table string) string {
	parts := make([]string, 0, 3)
	if database != "" {
		parts = append(parts, database)
	}
	if schema != "" {
		parts = append(parts, schema)
	}
	if table != "" {
		parts = append(parts, table)
	}
	return strings.Join(parts, ".")
}

func safeValue(v string) string {
	if v == "" {
		return "default"
	}
	return v
}
