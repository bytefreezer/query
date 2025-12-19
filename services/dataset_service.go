// Licensed under Elastic License 2.0
// See LICENSE.txt for details

package services

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
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
	ID           string `json:"id"`
	TenantID     string `json:"tenant_id"`
	Name         string `json:"name"`
	Path         string `json:"path"`
	ParquetGlob  string `json:"parquet_glob"`
	SizeBytes    int64  `json:"size_bytes"`
	FileCount    int    `json:"file_count"`
	ReadAllowed  bool   `json:"read_allowed"`   // True if S3 credentials allow read access
	ReadError    string `json:"read_error,omitempty"` // Error message if read is not allowed
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

// CreateS3ClientForDataset creates an S3 client using dataset-specific credentials
func (s *DatasetService) CreateS3ClientForDataset(ctx context.Context, tenantID, datasetID, authToken string) (*minio.Client, string, error) {
	// In standalone mode, use the configured credentials
	if !s.IsSharedMode() {
		return s.minioClient, s.config.S3.Bucket, nil
	}

	// Get credentials from control API
	creds, err := s.controlClient.GetDatasetS3Credentials(ctx, tenantID, datasetID, authToken)
	if err != nil {
		return nil, "", fmt.Errorf("failed to get S3 credentials: %w", err)
	}

	// Parse endpoint - remove http:// or https:// prefix if present
	endpoint := creds.Endpoint
	endpoint = strings.TrimPrefix(endpoint, "http://")
	endpoint = strings.TrimPrefix(endpoint, "https://")

	// Create MinIO client with dataset-specific credentials
	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(creds.AccessKey, creds.SecretKey, ""),
		Secure: creds.UseSSL,
	})
	if err != nil {
		return nil, "", fmt.Errorf("failed to create S3 client: %w", err)
	}

	return client, creds.Bucket, nil
}

// TestReadAccess tests if the S3 credentials allow reading from the dataset
// Returns (allowed, errorMessage)
func (s *DatasetService) TestReadAccess(ctx context.Context, tenantID, datasetID, authToken string) (bool, string) {
	client, bucket, err := s.CreateS3ClientForDataset(ctx, tenantID, datasetID, authToken)
	if err != nil {
		return false, fmt.Sprintf("Failed to get S3 credentials: %s. Configure read-enabled credentials in dataset settings.", err.Error())
	}

	// Try to list objects in the dataset path
	prefix := fmt.Sprintf("%s/%s/data/parquet/", tenantID, datasetID)

	objectCh := client.ListObjects(ctx, bucket, minio.ListObjectsOptions{
		Prefix:    prefix,
		MaxKeys:   1,
		Recursive: false,
	})

	// Check if we can read at least one object
	for object := range objectCh {
		if object.Err != nil {
			errMsg := object.Err.Error()
			if strings.Contains(errMsg, "Access Denied") || strings.Contains(errMsg, "AccessDenied") {
				return false, "S3 credentials do not have read access. Update your dataset S3 credentials to include read permissions (s3:GetObject, s3:ListBucket)."
			}
			return false, fmt.Sprintf("S3 read test failed: %s", errMsg)
		}
		// Successfully listed at least one object
		return true, ""
	}

	// No objects found, but no error either - could be empty or no read access
	// Try to do a HEAD request on the bucket to verify access
	_, err = client.BucketExists(ctx, bucket)
	if err != nil {
		errMsg := err.Error()
		if strings.Contains(errMsg, "Access Denied") || strings.Contains(errMsg, "AccessDenied") {
			return false, "S3 credentials do not have read access. Update your dataset S3 credentials to include read permissions (s3:GetObject, s3:ListBucket)."
		}
		return false, fmt.Sprintf("S3 access test failed: %s", errMsg)
	}

	// Bucket accessible but no parquet files yet
	return true, ""
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

	// In shared mode, get datasets from control API and check read access per dataset
	if s.IsSharedMode() {
		return s.listDatasetsSharedMode(ctx, tenantIDs, authToken)
	}

	// Standalone mode - use configured credentials
	return s.listDatasetsStandaloneMode(ctx, tenantIDs)
}

// listDatasetsSharedMode lists datasets using per-dataset S3 credentials from control API
func (s *DatasetService) listDatasetsSharedMode(ctx context.Context, tenantIDs []string, authToken string) ([]Dataset, error) {
	var datasets []Dataset

	for _, tenantID := range tenantIDs {
		// Get datasets for this tenant from control API
		controlDatasets, err := s.controlClient.GetDatasetsForTenant(ctx, tenantID, authToken)
		if err != nil {
			log.Warnf("Failed to get datasets for tenant %s from control: %v", tenantID, err)
			continue
		}

		for _, cd := range controlDatasets {
			// Test read access for this dataset
			readAllowed, readError := s.TestReadAccess(ctx, tenantID, cd.ID, authToken)

			basePath := fmt.Sprintf("%s/%s/data/parquet", tenantID, cd.ID)

			var sizeBytes int64
			var fileCount int

			// Only scan for files if we have read access
			if readAllowed {
				client, bucket, err := s.CreateS3ClientForDataset(ctx, tenantID, cd.ID, authToken)
				if err == nil {
					sizeBytes, fileCount = s.getDatasetSizeWithClient(ctx, client, bucket, basePath)
				}
			}

			// Build parquet glob - use dataset-specific bucket info if available
			parquetGlob := s.buildS3PathForDataset(ctx, tenantID, cd.ID, authToken, basePath+"/**/*.parquet")

			datasets = append(datasets, Dataset{
				ID:          cd.ID,
				TenantID:    tenantID,
				Name:        cd.Name,
				Path:        basePath,
				ParquetGlob: parquetGlob,
				SizeBytes:   sizeBytes,
				FileCount:   fileCount,
				ReadAllowed: readAllowed,
				ReadError:   readError,
			})
		}
	}

	log.Infof("Found %d datasets across %d tenants (shared mode)", len(datasets), len(tenantIDs))
	return datasets, nil
}

// listDatasetsStandaloneMode lists datasets using configured S3 credentials
func (s *DatasetService) listDatasetsStandaloneMode(ctx context.Context, tenantIDs []string) ([]Dataset, error) {
	bucket := s.config.S3.Bucket

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

		datasets = append(datasets, Dataset{
			ID:          key.datasetID,
			TenantID:    key.tenantID,
			Name:        key.datasetID, // In standalone mode, use ID as name
			Path:        basePath,
			ParquetGlob: parquetGlob,
			SizeBytes:   sizeBytes,
			FileCount:   fileCount,
			ReadAllowed: true, // Standalone mode uses configured credentials, assumed to have access
			ReadError:   "",
		})
	}

	log.Infof("Found %d datasets across %d tenants (standalone mode)", len(datasets), len(tenantIDs))
	return datasets, nil
}

// getDatasetSize calculates the total size and file count for a dataset using configured S3 client
func (s *DatasetService) getDatasetSize(ctx context.Context, bucket, basePath string) (int64, int) {
	return s.getDatasetSizeWithClient(ctx, s.minioClient, bucket, basePath)
}

// getDatasetSizeWithClient calculates the total size and file count for a dataset using a specific S3 client
func (s *DatasetService) getDatasetSizeWithClient(ctx context.Context, client *minio.Client, bucket, basePath string) (int64, int) {
	var totalSize int64
	var fileCount int

	objectCh := client.ListObjects(ctx, bucket, minio.ListObjectsOptions{
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

// buildS3PathForDataset builds an S3 path for DuckDB using dataset-specific credentials
func (s *DatasetService) buildS3PathForDataset(ctx context.Context, tenantID, datasetID, authToken string, path string) string {
	// In standalone mode or if we can't get credentials, fall back to default
	if !s.IsSharedMode() {
		return s.buildS3Path(path)
	}

	creds, err := s.controlClient.GetDatasetS3Credentials(ctx, tenantID, datasetID, authToken)
	if err != nil {
		log.Warnf("Failed to get S3 credentials for parquet glob, using default: %v", err)
		return s.buildS3Path(path)
	}

	// Build S3 URL for DuckDB: s3://bucket/path
	return fmt.Sprintf("s3://%s/%s", creds.Bucket, path)
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

// CommonMetadata represents the _common_metadata JSON structure from packer
type CommonMetadata struct {
	Version        int                    `json:"version"`
	Schema         MetadataSchema         `json:"schema"`
	NumRows        int64                  `json:"num_rows"`
	TotalSizeBytes int64                  `json:"total_size_bytes"`
	FileCount      int                    `json:"file_count"`
	GeneratedBy    string                 `json:"generated_by"`
	GeneratedAt    string                 `json:"generated_at"`
}

// MetadataSchema represents the schema structure in _common_metadata
type MetadataSchema struct {
	Type     string                 `json:"type"`
	Fields   []MetadataField        `json:"fields"`
	Metadata map[string]string      `json:"metadata,omitempty"`
}

// MetadataField represents a field in the schema
type MetadataField struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Nullable bool   `json:"nullable"`
}

// GetCommonMetadata fetches and parses the _common_metadata JSON file from S3
// This file contains the pre-merged schema from the packer
func (s *DatasetService) GetCommonMetadata(ctx context.Context, tenantID, datasetID string) (*CommonMetadata, error) {
	bucket := s.config.S3.Bucket
	key := fmt.Sprintf("%s/%s/data/parquet/_common_metadata", tenantID, datasetID)

	log.Debugf("Fetching _common_metadata from s3://%s/%s", bucket, key)

	// Get the object from S3
	obj, err := s.minioClient.GetObject(ctx, bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get _common_metadata object: %w", err)
	}
	defer obj.Close()

	// Read the JSON content
	data, err := io.ReadAll(obj)
	if err != nil {
		return nil, fmt.Errorf("failed to read _common_metadata: %w", err)
	}

	// Parse the JSON
	var metadata CommonMetadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		return nil, fmt.Errorf("failed to parse _common_metadata JSON: %w", err)
	}

	log.Debugf("Parsed _common_metadata: version=%d, schema fields=%d, rows=%d, files=%d",
		metadata.Version, len(metadata.Schema.Fields), metadata.NumRows, metadata.FileCount)

	return &metadata, nil
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
