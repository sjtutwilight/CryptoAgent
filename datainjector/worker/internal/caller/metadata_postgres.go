package caller

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/lib/pq"
	_ "github.com/lib/pq"

	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/types"
)

func init() {
	Register("metadata_postgres", func(class string, params map[string]any) (Caller, error) {
		return newPostgresMetadataCaller(params)
	})
}

type postgresMetadataCaller struct {
	db           *sql.DB
	cluster      string
	domain       string
	databaseName string
	schemas      []string
	timeout      time.Duration
}

type postgresTableMetadata struct {
	Cluster      string                            `json:"cluster"`
	Domain       string                            `json:"domain"`
	Database     string                            `json:"database"`
	Schema       string                            `json:"schema"`
	Table        string                            `json:"table"`
	TableType    string                            `json:"table_type"`
	RowEstimate  sql.NullInt64                     `json:"row_estimate"`
	PrimaryKey   []string                          `json:"primary_key"`
	Columns      []postgresColumnMetadata          `json:"columns"`
	CollectedAt  time.Time                         `json:"collected_at"`
	Extra        map[string]any                    `json:"extra,omitempty"`
	ColumnLookup map[string]postgresColumnMetadata `json:"-"`
}

type postgresColumnMetadata struct {
	Name       string `json:"name"`
	DataType   string `json:"data_type"`
	IsNullable bool   `json:"is_nullable"`
	Default    string `json:"default,omitempty"`
	Ordinal    int    `json:"ordinal_position"`
}

const defaultPostgresDSN = "postgres://twilight:twilight123@localhost:5432/twilight?sslmode=disable"

func newPostgresMetadataCaller(params map[string]any) (*postgresMetadataCaller, error) {
	dsn := resolveDSN(params)
	if dsn == "" {
		dsn = defaultPostgresDSN
	}
	cluster := getStringParam(params, "cluster", "default")
	domain := getStringParam(params, "domain", "postgres")
	dbName := strings.TrimSpace(getStringParam(params, "database", ""))
	timeout := time.Duration(getIntParam(params, "query_timeout_ms", 5000)) * time.Millisecond
	if timeout <= 0 {
		timeout = 5 * time.Second
	}

	schemas := toStringSlice(params["schemas"])
	if len(schemas) == 0 {
		schemas = []string{"public"}
	}

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("metadata_postgres: open connection: %w", err)
	}
	db.SetConnMaxLifetime(5 * time.Minute)
	db.SetMaxIdleConns(2)
	db.SetMaxOpenConns(5)

	if dbName == "" {
		if parsed, err := url.Parse(dsn); err == nil {
			if base := strings.Trim(parsed.Path, "/"); base != "" {
				dbName = base
			} else if val := parsed.Query().Get("dbname"); val != "" {
				dbName = val
			}
		}
	}
	if dbName == "" {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		if err := db.QueryRowContext(ctx, "SELECT current_database()").Scan(&dbName); err != nil {
			return nil, fmt.Errorf("metadata_postgres: fetch current database: %w", err)
		}
	}

	return &postgresMetadataCaller{
		db:           db,
		cluster:      cluster,
		domain:       domain,
		databaseName: dbName,
		schemas:      schemas,
		timeout:      timeout,
	}, nil
}

func (c *postgresMetadataCaller) CallOnce(ctx context.Context, args map[string]any) ([]*types.Message, error) {
	queryCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	tables, err := c.fetchTables(queryCtx)
	if err != nil {
		return nil, err
	}
	if len(tables) == 0 {
		return nil, nil
	}

	if err := c.attachColumns(queryCtx, tables); err != nil {
		return nil, err
	}
	if err := c.attachPrimaryKeys(queryCtx, tables); err != nil {
		return nil, err
	}

	collectedAt := time.Now().UTC()
	messages := make([]*types.Message, 0, len(tables))
	for _, tbl := range tables {
		tbl.CollectedAt = collectedAt
		payload, err := json.Marshal(tbl)
		if err != nil {
			return nil, fmt.Errorf("metadata_postgres: marshal %s.%s: %w", tbl.Schema, tbl.Table, err)
		}
		msg := &types.Message{
			Metadata: map[string]any{
				"cluster":     c.cluster,
				"domain":      c.domain,
				"entity_type": "postgres_table",
				"schema":      tbl.Schema,
				"table":       tbl.Table,
				"database":    c.databaseName,
			},
			Payload: payload,
		}
		messages = append(messages, msg)
	}
	return messages, nil
}

func (c *postgresMetadataCaller) fetchTables(ctx context.Context) (map[string]*postgresTableMetadata, error) {
	rows, err := c.db.QueryContext(ctx, `
SELECT table_schema, table_name, table_type
FROM information_schema.tables
WHERE table_schema = ANY($1)
ORDER BY table_schema, table_name`, pq.Array(c.schemas))
	if err != nil {
		return nil, fmt.Errorf("metadata_postgres: query tables: %w", err)
	}
	defer rows.Close()

	result := make(map[string]*postgresTableMetadata)
	for rows.Next() {
		var schema, table, tableType string
		if err := rows.Scan(&schema, &table, &tableType); err != nil {
			return nil, err
		}
		key := fmt.Sprintf("%s.%s", schema, table)
		result[key] = &postgresTableMetadata{
			Cluster:      c.cluster,
			Domain:       c.domain,
			Database:     c.databaseName,
			Schema:       schema,
			Table:        table,
			TableType:    tableType,
			Columns:      []postgresColumnMetadata{},
			PrimaryKey:   []string{},
			ColumnLookup: make(map[string]postgresColumnMetadata),
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func (c *postgresMetadataCaller) attachColumns(ctx context.Context, tables map[string]*postgresTableMetadata) error {
	rows, err := c.db.QueryContext(ctx, `
SELECT table_schema, table_name, column_name, data_type, is_nullable, column_default, ordinal_position
FROM information_schema.columns
WHERE table_schema = ANY($1)
ORDER BY table_schema, table_name, ordinal_position`, pq.Array(c.schemas))
	if err != nil {
		return fmt.Errorf("metadata_postgres: query columns: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var schema, table, column, dataType, isNullable string
		var columnDefault sql.NullString
		var ordinal int
		if err := rows.Scan(&schema, &table, &column, &dataType, &isNullable, &columnDefault, &ordinal); err != nil {
			return err
		}
		key := fmt.Sprintf("%s.%s", schema, table)
		tbl := tables[key]
		if tbl == nil {
			continue
		}
		col := postgresColumnMetadata{
			Name:       column,
			DataType:   dataType,
			IsNullable: isNullable == "YES",
			Ordinal:    ordinal,
		}
		if columnDefault.Valid {
			col.Default = columnDefault.String
		}
		tbl.Columns = append(tbl.Columns, col)
		tbl.ColumnLookup[column] = col
	}
	return rows.Err()
}

func (c *postgresMetadataCaller) attachPrimaryKeys(ctx context.Context, tables map[string]*postgresTableMetadata) error {
	rows, err := c.db.QueryContext(ctx, `
SELECT kcu.table_schema, kcu.table_name, kcu.column_name
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
  ON tc.constraint_name = kcu.constraint_name
  AND tc.table_schema = kcu.table_schema
WHERE tc.constraint_type = 'PRIMARY KEY'
  AND kcu.table_schema = ANY($1)
ORDER BY kcu.table_schema, kcu.table_name, kcu.ordinal_position`, pq.Array(c.schemas))
	if err != nil {
		return fmt.Errorf("metadata_postgres: query primary keys: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var schema, table, column string
		if err := rows.Scan(&schema, &table, &column); err != nil {
			return err
		}
		key := fmt.Sprintf("%s.%s", schema, table)
		tbl := tables[key]
		if tbl == nil {
			continue
		}
		tbl.PrimaryKey = append(tbl.PrimaryKey, column)
	}
	return rows.Err()
}

func resolveDSN(params map[string]any) string {
	dsn := strings.TrimSpace(getStringParam(params, "dsn", ""))
	if dsn != "" {
		return dsn
	}
	envKey := strings.TrimSpace(getStringParam(params, "dsn_env", ""))
	if envKey == "" {
		return ""
	}
	return strings.TrimSpace(os.Getenv(envKey))
}
