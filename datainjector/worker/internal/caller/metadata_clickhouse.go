package caller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/types"
)

func init() {
	Register("metadata_clickhouse", func(class string, params map[string]any) (Caller, error) {
		return newClickHouseMetadataCaller(params)
	})
}

type clickHouseMetadataCaller struct {
	endpoint  string
	username  string
	password  string
	cluster   string
	domain    string
	databases []string
	tables    []string
	client    *http.Client
	timeout   time.Duration
}

type clickHouseTableRow struct {
	Database         string `json:"database"`
	Name             string `json:"name"`
	Engine           string `json:"engine"`
	TotalRows        uint64 `json:"total_rows"`
	TotalBytes       uint64 `json:"total_bytes"`
	Comment          string `json:"comment"`
	SortingKey       string `json:"sorting_key"`
	PartitionKey     string `json:"partition_key"`
	PrimaryKey       string `json:"primary_key"`
	CreateTableQuery string `json:"create_table_query"`
}

type clickHouseColumnRow struct {
	Database          string `json:"database"`
	Table             string `json:"table"`
	Name              string `json:"name"`
	Type              string `json:"type"`
	DefaultKind       string `json:"default_kind"`
	DefaultExpression string `json:"default_expression"`
	Comment           string `json:"comment"`
}

type clickHouseTableMetadata struct {
	Cluster      string                     `json:"cluster"`
	Domain       string                     `json:"domain"`
	Database     string                     `json:"database"`
	Table        string                     `json:"table"`
	Engine       string                     `json:"engine"`
	TotalRows    uint64                     `json:"total_rows"`
	TotalBytes   uint64                     `json:"total_bytes"`
	SortingKey   string                     `json:"sorting_key"`
	PartitionKey string                     `json:"partition_key"`
	PrimaryKey   string                     `json:"primary_key"`
	CreateTable  string                     `json:"create_table"`
	Comment      string                     `json:"comment"`
	Columns      []clickHouseColumnMetadata `json:"columns"`
	CollectedAt  time.Time                  `json:"collected_at"`
}

type clickHouseColumnMetadata struct {
	Name        string `json:"name"`
	Type        string `json:"type"`
	DefaultKind string `json:"default_kind,omitempty"`
	DefaultExpr string `json:"default_expression,omitempty"`
	Comment     string `json:"comment,omitempty"`
}

const defaultClickHouseEndpoint = "http://localhost:8123/"

func newClickHouseMetadataCaller(params map[string]any) (*clickHouseMetadataCaller, error) {
	endpoint := resolveStringWithEnv(params, "endpoint")
	if endpoint == "" {
		endpoint = defaultClickHouseEndpoint
	}
	cluster := getStringParam(params, "cluster", "default")
	domain := getStringParam(params, "domain", "clickhouse")
	username := resolveStringWithEnv(params, "username")
	password := resolveStringWithEnv(params, "password")
	if username == "" {
		username = "default"
	}
	timeout := time.Duration(getIntParam(params, "query_timeout_ms", 5000)) * time.Millisecond
	if timeout <= 0 {
		timeout = 5 * time.Second
	}

	dbs := sanitizeIdentifiers(toStringSlice(params["databases"]))
	tables := sanitizeIdentifiers(toStringSlice(params["tables"]))

	return &clickHouseMetadataCaller{
		endpoint:  endpoint,
		username:  username,
		password:  password,
		cluster:   cluster,
		domain:    domain,
		databases: dbs,
		tables:    tables,
		client: &http.Client{
			Timeout: timeout,
		},
		timeout: timeout,
	}, nil
}

func (c *clickHouseMetadataCaller) CallOnce(ctx context.Context, args map[string]any) ([]*types.Message, error) {
	tableRows, err := c.fetchTables(ctx)
	if err != nil {
		return nil, err
	}
	if len(tableRows) == 0 {
		return nil, nil
	}
	columnRows, err := c.fetchColumns(ctx)
	if err != nil {
		return nil, err
	}

	colMap := make(map[string][]clickHouseColumnRow)
	for _, col := range columnRows {
		key := fmt.Sprintf("%s.%s", col.Database, col.Table)
		colMap[key] = append(colMap[key], col)
	}

	collectedAt := time.Now().UTC()
	messages := make([]*types.Message, 0, len(tableRows))
	for _, row := range tableRows {
		key := fmt.Sprintf("%s.%s", row.Database, row.Name)
		cols := colMap[key]
		colMeta := make([]clickHouseColumnMetadata, 0, len(cols))
		for _, col := range cols {
			colMeta = append(colMeta, clickHouseColumnMetadata{
				Name:        col.Name,
				Type:        col.Type,
				DefaultKind: col.DefaultKind,
				DefaultExpr: col.DefaultExpression,
				Comment:     col.Comment,
			})
		}

		meta := clickHouseTableMetadata{
			Cluster:      c.cluster,
			Domain:       c.domain,
			Database:     row.Database,
			Table:        row.Name,
			Engine:       row.Engine,
			TotalRows:    row.TotalRows,
			TotalBytes:   row.TotalBytes,
			SortingKey:   row.SortingKey,
			PartitionKey: row.PartitionKey,
			PrimaryKey:   row.PrimaryKey,
			CreateTable:  row.CreateTableQuery,
			Comment:      row.Comment,
			Columns:      colMeta,
			CollectedAt:  collectedAt,
		}

		payload, err := json.Marshal(meta)
		if err != nil {
			return nil, fmt.Errorf("metadata_clickhouse: marshal %s: %w", key, err)
		}
		msg := &types.Message{
			Metadata: map[string]any{
				"cluster":     c.cluster,
				"domain":      c.domain,
				"entity_type": "clickhouse_table",
				"database":    row.Database,
				"table":       row.Name,
			},
			Payload: payload,
		}
		messages = append(messages, msg)
	}
	return messages, nil
}

func (c *clickHouseMetadataCaller) fetchTables(ctx context.Context) ([]clickHouseTableRow, error) {
	query := `SELECT database, name, engine, total_rows, total_bytes, comment, sorting_key, partition_key, primary_key, create_table_query FROM system.tables WHERE 1=1`
	if len(c.databases) > 0 {
		query += fmt.Sprintf(" AND database IN (%s)", strings.Join(c.databases, ","))
	}
	if len(c.tables) > 0 {
		query += fmt.Sprintf(" AND name IN (%s)", strings.Join(c.tables, ","))
	}
	query += " ORDER BY database, name FORMAT JSON"

	var resp clickHouseQueryResponse[clickHouseTableRow]
	if err := c.execQuery(ctx, query, &resp); err != nil {
		return nil, err
	}
	return resp.Data, nil
}

func (c *clickHouseMetadataCaller) fetchColumns(ctx context.Context) ([]clickHouseColumnRow, error) {
	query := `SELECT database, table, name, type, default_kind, default_expression, comment FROM system.columns WHERE 1=1`
	if len(c.databases) > 0 {
		query += fmt.Sprintf(" AND database IN (%s)", strings.Join(c.databases, ","))
	}
	if len(c.tables) > 0 {
		query += fmt.Sprintf(" AND table IN (%s)", strings.Join(c.tables, ","))
	}
	query += " ORDER BY database, table, position FORMAT JSON"

	var resp clickHouseQueryResponse[clickHouseColumnRow]
	if err := c.execQuery(ctx, query, &resp); err != nil {
		return nil, err
	}
	return resp.Data, nil
}

type clickHouseQueryResponse[T any] struct {
	Data []T `json:"data"`
}

func (c *clickHouseMetadataCaller) execQuery(ctx context.Context, query string, out interface{}) error {
	if err := c.doPostQuery(ctx, query, out); err != nil {
		var httpErr *httpStatusError
		if errors.As(err, &httpErr) && httpErr.Code == http.StatusNotFound {
			return c.doQueryParam(ctx, query, out)
		}
		return err
	}
	return nil
}

func (c *clickHouseMetadataCaller) doPostQuery(ctx context.Context, query string, out interface{}) error {
	req, err := c.newRequest(ctx, http.MethodPost, strings.NewReader(query), "")
	if err != nil {
		return err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return &httpStatusError{Code: resp.StatusCode, Body: strings.TrimSpace(string(body))}
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func (c *clickHouseMetadataCaller) doQueryParam(ctx context.Context, query string, out interface{}) error {
	req, err := c.newRequest(ctx, http.MethodGet, nil, query)
	if err != nil {
		return err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("metadata_clickhouse: query failed: %s (%s)", resp.Status, strings.TrimSpace(string(body)))
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func (c *clickHouseMetadataCaller) newRequest(ctx context.Context, method string, body io.Reader, sqlQuery string) (*http.Request, error) {
	u, err := url.Parse(c.endpoint)
	if err != nil {
		return nil, err
	}
	if u.Path == "" {
		u.Path = "/"
	}
	q := u.Query()
	q.Set("default_format", "JSON")
	if sqlQuery != "" {
		q.Set("query", sqlQuery)
	}
	u.RawQuery = q.Encode()
	req, err := http.NewRequestWithContext(ctx, method, u.String(), body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "text/plain; charset=utf-8")
	if c.username != "" {
		req.SetBasicAuth(c.username, c.password)
	}
	return req, nil
}

type httpStatusError struct {
	Code int
	Body string
}

func (e *httpStatusError) Error() string {
	if e.Body == "" {
		return fmt.Sprintf("metadata_clickhouse: query failed: http %d", e.Code)
	}
	return fmt.Sprintf("metadata_clickhouse: query failed: http %d (%s)", e.Code, e.Body)
}

var identifierRegexp = regexp.MustCompile(`^[A-Za-z0-9_]+$`)

func sanitizeIdentifiers(values []string) []string {
	clean := make([]string, 0, len(values))
	for _, v := range values {
		if identifierRegexp.MatchString(v) {
			clean = append(clean, fmt.Sprintf("'%s'", v))
		}
	}
	return clean
}

func resolveStringWithEnv(params map[string]any, key string) string {
	val := strings.TrimSpace(getStringParam(params, key, ""))
	if val != "" {
		return val
	}
	envKey := strings.TrimSpace(getStringParam(params, key+"_env", ""))
	if envKey == "" {
		return ""
	}
	return strings.TrimSpace(os.Getenv(envKey))
}
