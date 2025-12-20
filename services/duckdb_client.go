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

// S3Credentials holds S3 credentials for a dataset
type S3Credentials struct {
	Bucket    string
	Region    string
	Endpoint  string
	AccessKey string
	SecretKey string
	UseSSL    bool
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

// initS3 loads the httpfs extension for S3 access
// Credentials are configured per-query via ConfigureS3Credentials
func (c *DuckDBClient) initS3() error {
	// Install and load httpfs extension only
	// S3 credentials are set per-query from dataset configuration
	statements := []string{
		"INSTALL httpfs",
		"LOAD httpfs",
	}

	for _, stmt := range statements {
		if _, err := c.db.Exec(stmt); err != nil {
			return fmt.Errorf("failed to execute '%s': %w", stmt, err)
		}
	}

	return nil
}

// ConfigureS3Credentials sets S3 credentials for the current session
// This must be called before executing queries that access S3
func (c *DuckDBClient) ConfigureS3Credentials(creds *S3Credentials) error {
	if creds == nil {
		return fmt.Errorf("credentials cannot be nil")
	}

	var statements []string

	// Set region (default to us-east-1 if not specified)
	region := creds.Region
	if region == "" {
		region = "us-east-1"
	}
	statements = append(statements, fmt.Sprintf("SET s3_region = '%s'", region))

	// Set credentials
	if creds.AccessKey != "" {
		statements = append(statements, fmt.Sprintf("SET s3_access_key_id = '%s'", creds.AccessKey))
	}
	if creds.SecretKey != "" {
		statements = append(statements, fmt.Sprintf("SET s3_secret_access_key = '%s'", creds.SecretKey))
	}

	// MinIO-specific settings
	if creds.Endpoint != "" {
		statements = append(statements, fmt.Sprintf("SET s3_endpoint = '%s'", creds.Endpoint))
		if !creds.UseSSL {
			statements = append(statements, "SET s3_use_ssl = false")
		}
		// Always use path style for MinIO
		statements = append(statements, "SET s3_url_style = 'path'")
	}

	for _, stmt := range statements {
		if _, err := c.db.Exec(stmt); err != nil {
			return fmt.Errorf("failed to configure S3: %w", err)
		}
	}

	log.Debugf("Configured DuckDB S3: bucket=%s, endpoint=%s, region=%s", creds.Bucket, creds.Endpoint, region)
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

// GetParquetSchema extracts schema from a parquet file or metadata file
func (c *DuckDBClient) GetParquetSchema(ctx context.Context, s3Path string) ([]ColumnInfo, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Use parquet_schema() to get column names and types
	query := fmt.Sprintf("SELECT name, type FROM parquet_schema('%s')", s3Path)

	rows, err := c.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to get parquet schema: %w", err)
	}
	defer rows.Close()

	var columns []ColumnInfo
	for rows.Next() {
		var name string
		var dtype interface{} // type can be NULL for schema group elements

		if err := rows.Scan(&name, &dtype); err != nil {
			return nil, fmt.Errorf("failed to scan schema row: %w", err)
		}

		// Skip internal parquet schema elements (groups with NULL type)
		if dtype == nil || name == "duckdb_schema" {
			continue
		}

		dtypeStr, ok := dtype.(string)
		if !ok {
			continue
		}

		columns = append(columns, ColumnInfo{
			Name: name,
			Type: dtypeStr,
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
