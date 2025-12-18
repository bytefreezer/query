// Licensed under Elastic License 2.0
// See LICENSE.txt for details

package services

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/bytefreezer/goodies/log"
	"github.com/bytefreezer/query/config"
)

// SchemaExtractor extracts and caches schema from parquet files
type SchemaExtractor struct {
	config         *config.Config
	duckdbClient   *DuckDBClient
	datasetService *DatasetService
	cache          map[string]*SchemaCache // keyed by dataset_id
	mu             sync.RWMutex
}

// SchemaCache holds cached schema information
type SchemaCache struct {
	Columns    []ColumnInfo
	Partitions []string
	SamplePath string
	CachedAt   time.Time
}

// SchemaResponse is returned by the /schema endpoint
type SchemaResponse struct {
	DatasetID  string       `json:"dataset_id"`
	Columns    []ColumnInfo `json:"columns"`
	Partitions []string     `json:"partitions"`
	SamplePath string       `json:"sample_path"`
}

// NewSchemaExtractor creates a new schema extractor
func NewSchemaExtractor(cfg *config.Config, duckdb *DuckDBClient, datasetService *DatasetService) *SchemaExtractor {
	return &SchemaExtractor{
		config:         cfg,
		duckdbClient:   duckdb,
		datasetService: datasetService,
		cache:          make(map[string]*SchemaCache),
	}
}

// GetSchema returns the schema for a dataset, using cache if available
func (s *SchemaExtractor) GetSchema(ctx context.Context, datasetID string) (*SchemaResponse, error) {
	s.mu.RLock()
	if cached, ok := s.cache[datasetID]; ok && time.Since(cached.CachedAt) < 5*time.Minute {
		defer s.mu.RUnlock()
		return &SchemaResponse{
			DatasetID:  datasetID,
			Columns:    cached.Columns,
			Partitions: cached.Partitions,
			SamplePath: cached.SamplePath,
		}, nil
	}
	s.mu.RUnlock()

	// Need to refresh cache
	return s.refreshSchema(ctx, datasetID)
}

// refreshSchema extracts fresh schema from parquet files
func (s *SchemaExtractor) refreshSchema(ctx context.Context, datasetID string) (*SchemaResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Get the parquet glob path for this dataset
	s3Path := s.datasetService.GetParquetGlob(datasetID)

	log.Infof("Extracting schema from: %s", s3Path)

	columns, err := s.duckdbClient.GetParquetSchema(ctx, s3Path)
	if err != nil {
		return nil, fmt.Errorf("failed to extract schema: %w", err)
	}

	// Detect partition columns (common patterns)
	partitions := detectPartitions(columns)

	// Update cache
	s.cache[datasetID] = &SchemaCache{
		Columns:    columns,
		Partitions: partitions,
		SamplePath: s3Path,
		CachedAt:   time.Now(),
	}

	log.Infof("Schema extracted for dataset %s: %d columns, %d partitions", datasetID, len(columns), len(partitions))

	return &SchemaResponse{
		DatasetID:  datasetID,
		Columns:    columns,
		Partitions: partitions,
		SamplePath: s3Path,
	}, nil
}

// FormatSchemaForPrompt formats the schema for LLM prompt injection
func (s *SchemaExtractor) FormatSchemaForPrompt(ctx context.Context, datasetID string) (string, error) {
	schema, err := s.GetSchema(ctx, datasetID)
	if err != nil {
		return "", err
	}

	var sb strings.Builder
	sb.WriteString("Table: security_events\n")
	sb.WriteString("Columns:\n")

	for _, col := range schema.Columns {
		sb.WriteString(fmt.Sprintf("  - %s (%s)\n", col.Name, col.Type))
	}

	if len(schema.Partitions) > 0 {
		sb.WriteString(fmt.Sprintf("Partitions: %s\n", strings.Join(schema.Partitions, ", ")))
	}

	sb.WriteString(fmt.Sprintf("\nData location: %s\n", schema.SamplePath))

	return sb.String(), nil
}

// detectPartitions identifies likely partition columns based on naming conventions
func detectPartitions(columns []ColumnInfo) []string {
	partitionNames := []string{"date", "year", "month", "day", "hour", "partition"}
	var partitions []string

	for _, col := range columns {
		colLower := strings.ToLower(col.Name)
		for _, pName := range partitionNames {
			if colLower == pName || strings.HasSuffix(colLower, "_"+pName) {
				partitions = append(partitions, col.Name)
				break
			}
		}
	}

	return partitions
}
