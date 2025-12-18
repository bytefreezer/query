// Licensed under Elastic License 2.0
// See LICENSE.txt for details

package services

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/bytefreezer/goodies/log"
	"github.com/bytefreezer/query/config"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// Dataset represents a dataset available for querying
type Dataset struct {
	ID          string `json:"id"`
	TenantID    string `json:"tenant_id"`
	Name        string `json:"name"`
	Path        string `json:"path"`
	ParquetGlob string `json:"parquet_glob"`
	SizeBytes   int64  `json:"size_bytes"`
	FileCount   int    `json:"file_count"`
}

// DatasetService handles dataset discovery and management
type DatasetService struct {
	config        *config.Config
	minioClient   *minio.Client
	controlClient *ControlClient
}

// NewDatasetService creates a new dataset service
func NewDatasetService(cfg *config.Config) (*DatasetService, error) {
	// Parse endpoint - remove http:// or https:// prefix if present
	endpoint := cfg.S3.Endpoint
	endpoint = strings.TrimPrefix(endpoint, "http://")
	endpoint = strings.TrimPrefix(endpoint, "https://")

	// Create MinIO client
	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.S3.AccessKey, cfg.S3.SecretKey, ""),
		Secure: cfg.S3.SSL,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create S3 client: %w", err)
	}

	// Create control client for shared mode (nil if standalone)
	controlClient := NewControlClient(cfg)

	return &DatasetService{
		config:        cfg,
		minioClient:   client,
		controlClient: controlClient,
	}, nil
}

// IsSharedMode returns true if running in shared mode (with control API)
func (s *DatasetService) IsSharedMode() bool {
	return s.controlClient != nil
}

// GetTenantIDs returns tenant IDs to scan based on mode
// - Standalone mode: returns the single tenant_id from config
// - Shared mode: queries control API for tenants belonging to account_id
func (s *DatasetService) GetTenantIDs(ctx context.Context, accountID string, authToken string) ([]string, error) {
	// Standalone mode - use tenant_id from config
	if !s.IsSharedMode() {
		if s.config.S3.TenantID == "" {
			return nil, fmt.Errorf("tenant_id not configured for standalone mode")
		}
		return []string{s.config.S3.TenantID}, nil
	}

	// Shared mode - lookup tenants for account via control API
	if accountID == "" {
		return nil, fmt.Errorf("account_id required for shared mode")
	}

	tenants, err := s.controlClient.GetTenantsForAccount(ctx, accountID, authToken)
	if err != nil {
		return nil, fmt.Errorf("failed to get tenants for account %s: %w", accountID, err)
	}

	if len(tenants) == 0 {
		return nil, fmt.Errorf("no tenants found for account %s", accountID)
	}

	var tenantIDs []string
	for _, t := range tenants {
		if t.Active {
			tenantIDs = append(tenantIDs, t.ID)
		}
	}

	if len(tenantIDs) == 0 {
		return nil, fmt.Errorf("no active tenants found for account %s", accountID)
	}

	return tenantIDs, nil
}

// ListDatasets returns all datasets available for the given account
// In standalone mode, accountID is ignored and config tenant_id is used
// In shared mode, accountID is used to lookup tenants via control API
func (s *DatasetService) ListDatasets(ctx context.Context, accountID string, authToken string) ([]Dataset, error) {
	// Get tenant IDs based on mode
	tenantIDs, err := s.GetTenantIDs(ctx, accountID, authToken)
	if err != nil {
		return nil, err
	}

	bucket := s.config.S3.Bucket

	// Build a map of dataset names from control API (shared mode only)
	datasetNames := make(map[string]string) // datasetID -> name
	if s.IsSharedMode() {
		for _, tenantID := range tenantIDs {
			datasets, err := s.controlClient.GetDatasetsForTenant(ctx, tenantID, authToken)
			if err != nil {
				log.Warnf("Failed to get datasets for tenant %s from control: %v", tenantID, err)
				continue
			}
			for _, d := range datasets {
				datasetNames[d.ID] = d.Name
			}
		}
	}

	// Use a map to deduplicate datasets (key: tenantID/datasetID)
	type datasetKey struct {
		tenantID  string
		datasetID string
	}
	datasetMap := make(map[datasetKey]bool)

	// Scan S3 for each tenant
	for _, tenantID := range tenantIDs {
		// Structure: {tenant_id}/{dataset_id}/data/parquet/
		prefix := tenantID + "/"

		log.Debugf("Listing datasets for tenant %s in bucket %s with prefix %s", tenantID, bucket, prefix)

		// List objects to discover dataset directories
		objectCh := s.minioClient.ListObjects(ctx, bucket, minio.ListObjectsOptions{
			Prefix:    prefix,
			Recursive: false, // Only get immediate children (dataset directories)
		})

		for object := range objectCh {
			if object.Err != nil {
				log.Warnf("Error listing objects for tenant %s: %v", tenantID, object.Err)
				continue
			}

			// Extract dataset ID from the key
			// Key format: {tenant_id}/{dataset_id}/ or {tenant_id}/{dataset_id}/...
			key := strings.TrimPrefix(object.Key, prefix)
			parts := strings.SplitN(key, "/", 2)
			if len(parts) > 0 && parts[0] != "" {
				datasetID := parts[0]
				datasetMap[datasetKey{tenantID: tenantID, datasetID: datasetID}] = true
			}
		}
	}

	// Convert map to slice and calculate sizes
	var datasets []Dataset
	for key := range datasetMap {
		// Build the parquet glob path for DuckDB
		// Structure: {tenant_id}/{dataset_id}/data/parquet/**/*.parquet
		basePath := fmt.Sprintf("%s/%s/data/parquet", key.tenantID, key.datasetID)
		parquetGlob := s.buildS3Path(basePath + "/**/*.parquet")

		// Calculate dataset size
		sizeBytes, fileCount := s.getDatasetSize(ctx, bucket, basePath)

		// Get display name from control API or fall back to ID
		name := key.datasetID
		if controlName, ok := datasetNames[key.datasetID]; ok && controlName != "" {
			name = controlName
		}

		datasets = append(datasets, Dataset{
			ID:          key.datasetID,
			TenantID:    key.tenantID,
			Name:        name,
			Path:        basePath,
			ParquetGlob: parquetGlob,
			SizeBytes:   sizeBytes,
			FileCount:   fileCount,
		})
	}

	log.Infof("Found %d datasets across %d tenants", len(datasets), len(tenantIDs))
	return datasets, nil
}

// getDatasetSize calculates the total size and file count for a dataset
func (s *DatasetService) getDatasetSize(ctx context.Context, bucket, basePath string) (int64, int) {
	var totalSize int64
	var fileCount int

	objectCh := s.minioClient.ListObjects(ctx, bucket, minio.ListObjectsOptions{
		Prefix:    basePath + "/",
		Recursive: true,
	})

	for object := range objectCh {
		if object.Err != nil {
			log.Warnf("Error listing objects for size: %v", object.Err)
			continue
		}
		// Only count parquet files
		if strings.HasSuffix(object.Key, ".parquet") {
			totalSize += object.Size
			fileCount++
		}
	}

	return totalSize, fileCount
}

// GetDataset returns a specific dataset by ID
// tenantID is required to locate the dataset in S3
func (s *DatasetService) GetDataset(ctx context.Context, tenantID, datasetID string) (*Dataset, error) {
	if tenantID == "" {
		return nil, fmt.Errorf("tenant_id is required")
	}

	// Build the parquet glob path
	basePath := fmt.Sprintf("%s/%s/data/parquet", tenantID, datasetID)
	parquetGlob := s.buildS3Path(basePath + "/**/*.parquet")

	// Verify the dataset exists by checking if the path has any objects
	prefix := fmt.Sprintf("%s/%s/", tenantID, datasetID)
	objectCh := s.minioClient.ListObjects(ctx, s.config.S3.Bucket, minio.ListObjectsOptions{
		Prefix:    prefix,
		MaxKeys:   1,
		Recursive: true,
	})

	// Check if at least one object exists
	object, ok := <-objectCh
	if !ok {
		return nil, fmt.Errorf("dataset %s not found for tenant %s", datasetID, tenantID)
	}
	if object.Err != nil {
		return nil, fmt.Errorf("error checking dataset: %w", object.Err)
	}

	return &Dataset{
		ID:          datasetID,
		TenantID:    tenantID,
		Name:        datasetID,
		Path:        basePath,
		ParquetGlob: parquetGlob,
	}, nil
}

// GetParquetGlob returns the S3 glob path for a dataset's parquet files
func (s *DatasetService) GetParquetGlob(tenantID, datasetID string) string {
	basePath := fmt.Sprintf("%s/%s/data/parquet", tenantID, datasetID)
	return s.buildS3Path(basePath + "/**/*.parquet")
}

// GetMetadataPath returns the S3 path to the _common_metadata file
// This file contains the pre-merged schema from the packer
func (s *DatasetService) GetMetadataPath(tenantID, datasetID string) string {
	basePath := fmt.Sprintf("%s/%s/data/parquet/_common_metadata", tenantID, datasetID)
	return s.buildS3Path(basePath)
}

// buildS3Path constructs the full S3 URL for DuckDB
func (s *DatasetService) buildS3Path(path string) string {
	return fmt.Sprintf("s3://%s/%s", s.config.S3.Bucket, path)
}

// RecentFile represents a parquet file with its metadata
type RecentFile struct {
	Path         string
	Size         int64
	Modified     time.Time
	MinTimestamp *time.Time // From S3 metadata or partition path
	MaxTimestamp *time.Time // From S3 metadata or partition path
	RowCount     int64      // From S3 metadata
	Partition    string     // Partition path (e.g., "year=2025/month=12/day=17/hour=14")
}

// GetRecentFiles returns the N most recent parquet files for a dataset
func (s *DatasetService) GetRecentFiles(ctx context.Context, tenantID, datasetID string, limit int) ([]RecentFile, error) {
	bucket := s.config.S3.Bucket
	prefix := fmt.Sprintf("%s/%s/data/parquet/", tenantID, datasetID)

	var files []RecentFile

	objectCh := s.minioClient.ListObjects(ctx, bucket, minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: true,
	})

	for object := range objectCh {
		if object.Err != nil {
			return nil, fmt.Errorf("error listing objects: %w", object.Err)
		}
		if strings.HasSuffix(object.Key, ".parquet") {
			file := RecentFile{
				Path:     fmt.Sprintf("s3://%s/%s", bucket, object.Key),
				Size:     object.Size,
				Modified: object.LastModified,
			}

			// Extract partition info from path and estimate timestamp range
			file.Partition = extractPartitionPath(object.Key)
			if file.Partition != "" {
				minTS, maxTS := parsePartitionTimestamps(file.Partition)
				if minTS != nil {
					file.MinTimestamp = minTS
					file.MaxTimestamp = maxTS
				}
			}

			files = append(files, file)
		}
	}

	// Sort by partition timestamp (if available) or modification time
	sort.Slice(files, func(i, j int) bool {
		// Prefer partition-based sorting if both have timestamps
		if files[i].MaxTimestamp != nil && files[j].MaxTimestamp != nil {
			return files[i].MaxTimestamp.After(*files[j].MaxTimestamp)
		}
		return files[i].Modified.After(files[j].Modified)
	})

	// Return only the requested number of files
	if len(files) > limit {
		files = files[:limit]
	}

	return files, nil
}

// GetFilesInTimeRange returns parquet files that overlap with the given time range
func (s *DatasetService) GetFilesInTimeRange(ctx context.Context, tenantID, datasetID string, start, end time.Time, limit int) ([]RecentFile, error) {
	allFiles, err := s.GetRecentFiles(ctx, tenantID, datasetID, 1000) // Get more files for filtering
	if err != nil {
		return nil, err
	}

	var matchingFiles []RecentFile
	for _, file := range allFiles {
		// If file has timestamp metadata, check for overlap
		if file.MinTimestamp != nil && file.MaxTimestamp != nil {
			// Check if file's time range overlaps with query range
			if file.MaxTimestamp.Before(start) || file.MinTimestamp.After(end) {
				continue // No overlap
			}
		}
		matchingFiles = append(matchingFiles, file)
		if len(matchingFiles) >= limit {
			break
		}
	}

	return matchingFiles, nil
}

// extractPartitionPath extracts the partition portion from an S3 key
// e.g., "customer-1/dataset-1/data/parquet/year=2025/month=12/day=17/hour=14/file.parquet"
// returns "year=2025/month=12/day=17/hour=14"
func extractPartitionPath(key string) string {
	// Look for partition patterns
	patterns := []string{
		`year=\d+/month=\d+/day=\d+/hour=\d+`, // date_hour scheme
		`year=\d+/month=\d+/day=\d+`,          // hive scheme
		`\d{4}/\d{2}/\d{2}/\d{2}`,             // simple date_hour: 2025/12/17/14
		`\d{4}/\d{2}/\d{2}`,                   // simple date: 2025/12/17
	}

	for _, pattern := range patterns {
		re := regexp.MustCompile(pattern)
		if match := re.FindString(key); match != "" {
			return match
		}
	}
	return ""
}

// parsePartitionTimestamps parses partition path to extract time range
// Returns start and end of the partition period
func parsePartitionTimestamps(partition string) (*time.Time, *time.Time) {
	var year, month, day, hour int
	hasHour := false

	// Try hive-style: year=2025/month=12/day=17/hour=14
	hiveHourRe := regexp.MustCompile(`year=(\d+)/month=(\d+)/day=(\d+)/hour=(\d+)`)
	if matches := hiveHourRe.FindStringSubmatch(partition); len(matches) == 5 {
		year, _ = strconv.Atoi(matches[1])
		month, _ = strconv.Atoi(matches[2])
		day, _ = strconv.Atoi(matches[3])
		hour, _ = strconv.Atoi(matches[4])
		hasHour = true
	} else {
		// Try hive-style without hour: year=2025/month=12/day=17
		hiveDayRe := regexp.MustCompile(`year=(\d+)/month=(\d+)/day=(\d+)`)
		if matches := hiveDayRe.FindStringSubmatch(partition); len(matches) == 4 {
			year, _ = strconv.Atoi(matches[1])
			month, _ = strconv.Atoi(matches[2])
			day, _ = strconv.Atoi(matches[3])
		} else {
			// Try simple date_hour: 2025/12/17/14
			simpleDateHourRe := regexp.MustCompile(`(\d{4})/(\d{2})/(\d{2})/(\d{2})`)
			if matches := simpleDateHourRe.FindStringSubmatch(partition); len(matches) == 5 {
				year, _ = strconv.Atoi(matches[1])
				month, _ = strconv.Atoi(matches[2])
				day, _ = strconv.Atoi(matches[3])
				hour, _ = strconv.Atoi(matches[4])
				hasHour = true
			} else {
				// Try simple date: 2025/12/17
				simpleDateRe := regexp.MustCompile(`(\d{4})/(\d{2})/(\d{2})`)
				if matches := simpleDateRe.FindStringSubmatch(partition); len(matches) == 4 {
					year, _ = strconv.Atoi(matches[1])
					month, _ = strconv.Atoi(matches[2])
					day, _ = strconv.Atoi(matches[3])
				} else {
					return nil, nil
				}
			}
		}
	}

	if year == 0 {
		return nil, nil
	}

	loc := time.UTC
	var minTS, maxTS time.Time

	if hasHour {
		minTS = time.Date(year, time.Month(month), day, hour, 0, 0, 0, loc)
		maxTS = time.Date(year, time.Month(month), day, hour, 59, 59, 999999999, loc)
	} else {
		minTS = time.Date(year, time.Month(month), day, 0, 0, 0, 0, loc)
		maxTS = time.Date(year, time.Month(month), day, 23, 59, 59, 999999999, loc)
	}

	return &minTS, &maxTS
}
