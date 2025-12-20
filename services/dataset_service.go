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
	ID          string `json:"id"`
	TenantID    string `json:"tenant_id"`
	Name        string `json:"name"`
	Path        string `json:"path"`
	ParquetGlob string `json:"parquet_glob"`
	SizeBytes   int64  `json:"size_bytes"`
	FileCount   int    `json:"file_count"`
	ReadAllowed bool   `json:"read_allowed"`            // True if S3 credentials allow read access
	ReadError   string `json:"read_error,omitempty"`    // Error message if read is not allowed
}

// DatasetService handles dataset discovery and management
// Always uses control API to get S3 credentials per dataset
type DatasetService struct {
	config        *config.Config
	controlClient *ControlClient
}

// NewDatasetService creates a new dataset service
func NewDatasetService(cfg *config.Config) (*DatasetService, error) {
	// Control client is required - S3 credentials come from dataset settings
	controlClient := NewControlClient(cfg)
	if controlClient == nil {
		return nil, fmt.Errorf("control.url is required - query service must connect to control to get dataset S3 credentials")
	}

	return &DatasetService{
		config:        cfg,
		controlClient: controlClient,
	}, nil
}

// CreateS3ClientForDataset creates an S3 client using dataset-specific credentials from control
func (s *DatasetService) CreateS3ClientForDataset(ctx context.Context, tenantID, datasetID string) (*minio.Client, string, error) {
	// Get credentials from control API
	creds, err := s.controlClient.GetDatasetS3Credentials(ctx, tenantID, datasetID)
	if err != nil {
		return nil, "", fmt.Errorf("failed to get S3 credentials from control: %w", err)
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
func (s *DatasetService) TestReadAccess(ctx context.Context, tenantID, datasetID string) (bool, string) {
	client, bucket, err := s.CreateS3ClientForDataset(ctx, tenantID, datasetID)
	if err != nil {
		return false, fmt.Sprintf("Failed to get S3 credentials: %s. Configure S3 credentials in dataset settings.", err.Error())
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
				return false, "S3 credentials do not have read access. Update dataset S3 credentials to include read permissions (s3:GetObject, s3:ListBucket)."
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
			return false, "S3 credentials do not have read access. Update dataset S3 credentials to include read permissions (s3:GetObject, s3:ListBucket)."
		}
		return false, fmt.Sprintf("S3 access test failed: %s", errMsg)
	}

	// Bucket accessible but no parquet files yet
	return true, ""
}

// GetTenantIDs returns tenant IDs for the given account via control API
func (s *DatasetService) GetTenantIDs(ctx context.Context, accountID string) ([]string, error) {
	if accountID == "" {
		return nil, fmt.Errorf("account_id is required")
	}

	tenants, err := s.controlClient.GetTenantsForAccount(ctx, accountID)
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
func (s *DatasetService) ListDatasets(ctx context.Context, accountID string) ([]Dataset, error) {
	tenantIDs, err := s.GetTenantIDs(ctx, accountID)
	if err != nil {
		return nil, err
	}

	var datasets []Dataset

	for _, tenantID := range tenantIDs {
		// Get datasets for this tenant from control API
		controlDatasets, err := s.controlClient.GetDatasetsForTenant(ctx, tenantID)
		if err != nil {
			log.Warnf("Failed to get datasets for tenant %s from control: %v", tenantID, err)
			continue
		}

		for _, cd := range controlDatasets {
			// Test read access for this dataset
			readAllowed, readError := s.TestReadAccess(ctx, tenantID, cd.ID)

			basePath := fmt.Sprintf("%s/%s/data/parquet", tenantID, cd.ID)

			var sizeBytes int64
			var fileCount int

			// Only scan for files if we have read access
			if readAllowed {
				client, bucket, err := s.CreateS3ClientForDataset(ctx, tenantID, cd.ID)
				if err == nil {
					sizeBytes, fileCount = s.getDatasetSize(ctx, client, bucket, basePath)
				}
			}

			// Build parquet glob with correct bucket
			parquetGlob := s.buildS3Path(ctx, tenantID, cd.ID, basePath+"/**/*.parquet")

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

	log.Infof("Found %d datasets across %d tenants", len(datasets), len(tenantIDs))
	return datasets, nil
}

// getDatasetSize calculates the total size and file count for a dataset
func (s *DatasetService) getDatasetSize(ctx context.Context, client *minio.Client, bucket, basePath string) (int64, int) {
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

// buildS3Path builds an S3 path using dataset-specific bucket from control
func (s *DatasetService) buildS3Path(ctx context.Context, tenantID, datasetID string, path string) string {
	creds, err := s.controlClient.GetDatasetS3Credentials(ctx, tenantID, datasetID)
	if err != nil {
		log.Warnf("Failed to get S3 credentials for path: %v", err)
		return fmt.Sprintf("s3://unknown/%s", path)
	}
	return fmt.Sprintf("s3://%s/%s", creds.Bucket, path)
}

// GetS3CredentialsForDuckDB returns S3 credentials in DuckDB format for a dataset
func (s *DatasetService) GetS3CredentialsForDuckDB(ctx context.Context, tenantID, datasetID string) (*S3Credentials, error) {
	creds, err := s.controlClient.GetDatasetS3Credentials(ctx, tenantID, datasetID)
	if err != nil {
		return nil, fmt.Errorf("failed to get S3 credentials from control: %w", err)
	}

	return &S3Credentials{
		Bucket:    creds.Bucket,
		Region:    creds.Region,
		Endpoint:  creds.Endpoint,
		AccessKey: creds.AccessKey,
		SecretKey: creds.SecretKey,
		UseSSL:    creds.UseSSL,
	}, nil
}

// GetParquetGlob returns the S3 glob path for a dataset's parquet files
func (s *DatasetService) GetParquetGlob(ctx context.Context, tenantID, datasetID string) string {
	basePath := fmt.Sprintf("%s/%s/data/parquet/**/*.parquet", tenantID, datasetID)
	return s.buildS3Path(ctx, tenantID, datasetID, basePath)
}

// GetMetadataPath returns the S3 path to the _common_metadata file
func (s *DatasetService) GetMetadataPath(ctx context.Context, tenantID, datasetID string) string {
	basePath := fmt.Sprintf("%s/%s/data/parquet/_common_metadata", tenantID, datasetID)
	return s.buildS3Path(ctx, tenantID, datasetID, basePath)
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
	client, bucket, err := s.CreateS3ClientForDataset(ctx, tenantID, datasetID)
	if err != nil {
		return nil, fmt.Errorf("failed to get S3 client: %w", err)
	}

	prefix := fmt.Sprintf("%s/%s/data/parquet/", tenantID, datasetID)

	var files []RecentFile

	objectCh := client.ListObjects(ctx, bucket, minio.ListObjectsOptions{
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
	allFiles, err := s.GetRecentFiles(ctx, tenantID, datasetID, 1000)
	if err != nil {
		return nil, err
	}

	var matchingFiles []RecentFile
	for _, file := range allFiles {
		if file.MinTimestamp != nil && file.MaxTimestamp != nil {
			if file.MaxTimestamp.Before(start) || file.MinTimestamp.After(end) {
				continue
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
func extractPartitionPath(key string) string {
	patterns := []string{
		`year=\d+/month=\d+/day=\d+/hour=\d+`,
		`year=\d+/month=\d+/day=\d+`,
		`\d{4}/\d{2}/\d{2}/\d{2}`,
		`\d{4}/\d{2}/\d{2}`,
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
	Version        int            `json:"version"`
	Schema         MetadataSchema `json:"schema"`
	NumRows        int64          `json:"num_rows"`
	TotalSizeBytes int64          `json:"total_size_bytes"`
	FileCount      int            `json:"file_count"`
	GeneratedBy    string         `json:"generated_by"`
	GeneratedAt    string         `json:"generated_at"`
}

// MetadataSchema represents the schema structure in _common_metadata
type MetadataSchema struct {
	Type     string            `json:"type"`
	Fields   []MetadataField   `json:"fields"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

// MetadataField represents a field in the schema
type MetadataField struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Nullable bool   `json:"nullable"`
}

// GetCommonMetadata fetches and parses the _common_metadata JSON file from S3
func (s *DatasetService) GetCommonMetadata(ctx context.Context, tenantID, datasetID string) (*CommonMetadata, error) {
	client, bucket, err := s.CreateS3ClientForDataset(ctx, tenantID, datasetID)
	if err != nil {
		return nil, fmt.Errorf("failed to get S3 client: %w", err)
	}

	key := fmt.Sprintf("%s/%s/data/parquet/_common_metadata", tenantID, datasetID)

	log.Debugf("Fetching _common_metadata from s3://%s/%s", bucket, key)

	obj, err := client.GetObject(ctx, bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get _common_metadata: %w", err)
	}
	defer obj.Close()

	data, err := io.ReadAll(obj)
	if err != nil {
		return nil, fmt.Errorf("failed to read _common_metadata: %w", err)
	}

	var metadata CommonMetadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		return nil, fmt.Errorf("failed to parse _common_metadata: %w", err)
	}

	log.Debugf("Parsed _common_metadata: version=%d, fields=%d, rows=%d, files=%d",
		metadata.Version, len(metadata.Schema.Fields), metadata.NumRows, metadata.FileCount)

	return &metadata, nil
}

// parsePartitionTimestamps parses partition path to extract time range
func parsePartitionTimestamps(partition string) (*time.Time, *time.Time) {
	var year, month, day, hour int
	hasHour := false

	hiveHourRe := regexp.MustCompile(`year=(\d+)/month=(\d+)/day=(\d+)/hour=(\d+)`)
	if matches := hiveHourRe.FindStringSubmatch(partition); len(matches) == 5 {
		year, _ = strconv.Atoi(matches[1])
		month, _ = strconv.Atoi(matches[2])
		day, _ = strconv.Atoi(matches[3])
		hour, _ = strconv.Atoi(matches[4])
		hasHour = true
	} else {
		hiveDayRe := regexp.MustCompile(`year=(\d+)/month=(\d+)/day=(\d+)`)
		if matches := hiveDayRe.FindStringSubmatch(partition); len(matches) == 4 {
			year, _ = strconv.Atoi(matches[1])
			month, _ = strconv.Atoi(matches[2])
			day, _ = strconv.Atoi(matches[3])
		} else {
			simpleDateHourRe := regexp.MustCompile(`(\d{4})/(\d{2})/(\d{2})/(\d{2})`)
			if matches := simpleDateHourRe.FindStringSubmatch(partition); len(matches) == 5 {
				year, _ = strconv.Atoi(matches[1])
				month, _ = strconv.Atoi(matches[2])
				day, _ = strconv.Atoi(matches[3])
				hour, _ = strconv.Atoi(matches[4])
				hasHour = true
			} else {
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
