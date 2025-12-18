// Licensed under Elastic License 2.0
// See LICENSE.txt for details

package services

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/bytefreezer/goodies/log"
	"github.com/bytefreezer/query/config"
	_ "github.com/marcboeker/go-duckdb"
)

// DuckDBClient handles DuckDB connections and query execution
type DuckDBClient struct {
	config *config.Config
	db     *sql.DB
	mu     sync.Mutex
}

// QueryResult holds the result of a query execution
type QueryResult struct {
	Columns         []string        `json:"columns"`
	Rows            [][]interface{} `json:"rows"`
	RowCount        int             `json:"row_count"`
	ExecutionTimeMs int64           `json:"execution_time_ms"`
	Error           string          `json:"error,omitempty"`
}

// ColumnInfo holds schema information for a column
type ColumnInfo struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// NewDuckDBClient creates a new DuckDB client
func NewDuckDBClient(cfg *config.Config) (*DuckDBClient, error) {
	// Open DuckDB connection (in-memory)
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("failed to open DuckDB: %w", err)
	}

	client := &DuckDBClient{
		config: cfg,
		db:     db,
	}

	// Initialize S3 extension
	if err := client.initS3(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to initialize S3: %w", err)
	}

	log.Info("DuckDB client initialized with S3 support")
	return client, nil
}

// initS3 configures DuckDB for S3 access
func (c *DuckDBClient) initS3() error {
	// Install and load httpfs extension
	statements := []string{
		"INSTALL httpfs",
		"LOAD httpfs",
	}

	// Add S3 configuration
	if c.config.S3.Region != "" {
		statements = append(statements, fmt.Sprintf("SET s3_region = '%s'", c.config.S3.Region))
	}
	if c.config.S3.AccessKey != "" {
		statements = append(statements, fmt.Sprintf("SET s3_access_key_id = '%s'", c.config.S3.AccessKey))
	}
	if c.config.S3.SecretKey != "" {
		statements = append(statements, fmt.Sprintf("SET s3_secret_access_key = '%s'", c.config.S3.SecretKey))
	}

	// MinIO-specific settings
	if c.config.S3.Endpoint != "" {
		statements = append(statements, fmt.Sprintf("SET s3_endpoint = '%s'", c.config.S3.Endpoint))
		if !c.config.S3.SSL {
			statements = append(statements, "SET s3_use_ssl = false")
		}
		if c.config.S3.URLStyle == "path" {
			statements = append(statements, "SET s3_url_style = 'path'")
		}
	}

	for _, stmt := range statements {
		if _, err := c.db.Exec(stmt); err != nil {
			return fmt.Errorf("failed to execute '%s': %w", stmt, err)
		}
	}

	return nil
}

// ExecuteQuery runs a SQL query and returns the results
func (c *DuckDBClient) ExecuteQuery(ctx context.Context, sqlQuery string, timeoutSec int) *QueryResult {
	c.mu.Lock()
	defer c.mu.Unlock()

	start := time.Now()

	if timeoutSec <= 0 {
		timeoutSec = 30
	}

	// Create context with timeout
	queryCtx, cancel := context.WithTimeout(ctx, time.Duration(timeoutSec)*time.Second)
	defer cancel()

	rows, err := c.db.QueryContext(queryCtx, sqlQuery)
	if err != nil {
		return &QueryResult{
			Error:           fmt.Sprintf("Query failed: %v", err),
			ExecutionTimeMs: time.Since(start).Milliseconds(),
		}
	}
	defer rows.Close()

	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		return &QueryResult{
			Error:           fmt.Sprintf("Failed to get columns: %v", err),
			ExecutionTimeMs: time.Since(start).Milliseconds(),
		}
	}

	// Scan rows
	var resultRows [][]interface{}
	for rows.Next() {
		// Create slice of interface{} to hold row values
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return &QueryResult{
				Columns:         columns,
				Error:           fmt.Sprintf("Failed to scan row: %v", err),
				ExecutionTimeMs: time.Since(start).Milliseconds(),
			}
		}

		// Convert values to JSON-friendly types
		row := make([]interface{}, len(columns))
		for i, v := range values {
			row[i] = convertValue(v)
		}
		resultRows = append(resultRows, row)
	}

	if err := rows.Err(); err != nil {
		return &QueryResult{
			Columns:         columns,
			Rows:            resultRows,
			RowCount:        len(resultRows),
			Error:           fmt.Sprintf("Row iteration error: %v", err),
			ExecutionTimeMs: time.Since(start).Milliseconds(),
		}
	}

	return &QueryResult{
		Columns:         columns,
		Rows:            resultRows,
		RowCount:        len(resultRows),
		ExecutionTimeMs: time.Since(start).Milliseconds(),
	}
}

// GetParquetSchema extracts schema from a parquet file
func (c *DuckDBClient) GetParquetSchema(ctx context.Context, s3Path string) ([]ColumnInfo, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	query := fmt.Sprintf("DESCRIBE SELECT * FROM read_parquet('%s', hive_partitioning=true, union_by_name=true) LIMIT 0", s3Path)

	rows, err := c.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to describe parquet: %w", err)
	}
	defer rows.Close()

	var columns []ColumnInfo
	for rows.Next() {
		var name, dtype string
		var null, key, defaultVal, extra interface{}

		// DESCRIBE returns: column_name, column_type, null, key, default, extra
		if err := rows.Scan(&name, &dtype, &null, &key, &defaultVal, &extra); err != nil {
			return nil, fmt.Errorf("failed to scan schema row: %w", err)
		}

		columns = append(columns, ColumnInfo{
			Name: name,
			Type: dtype,
		})
	}

	return columns, nil
}

// Close closes the DuckDB connection
func (c *DuckDBClient) Close() error {
	return c.db.Close()
}

// convertValue converts DuckDB values to JSON-friendly types
func convertValue(v interface{}) interface{} {
	if v == nil {
		return nil
	}

	switch val := v.(type) {
	case []byte:
		return string(val)
	case time.Time:
		return val.Format(time.RFC3339)
	default:
		return val
	}
}
