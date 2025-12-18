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
	Name        string `json:"name"`
	Path        string `json:"path"`
	ParquetGlob string `json:"parquet_glob"`
	SizeBytes   int64  `json:"size_bytes"`
	FileCount   int    `json:"file_count"`
}

// DatasetService handles dataset discovery and management
type DatasetService struct {
	config      *config.Config
	minioClient *minio.Client
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

	return &DatasetService{
		config:      cfg,
		minioClient: client,
	}, nil
}

// ListDatasets returns all datasets available for the configured account
func (s *DatasetService) ListDatasets(ctx context.Context) ([]Dataset, error) {
	accountID := s.config.S3.AccountID
	bucket := s.config.S3.Bucket

	// List objects with prefix to find dataset directories
	// Structure: {account_id}/{dataset_id}/data/parquet/
	prefix := accountID + "/"

	log.Debugf("Listing datasets for account %s in bucket %s with prefix %s", accountID, bucket, prefix)

	// Use a map to deduplicate dataset IDs
	datasetMap := make(map[string]bool)

	// List objects to discover dataset directories
	objectCh := s.minioClient.ListObjects(ctx, bucket, minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: false, // Only get immediate children (dataset directories)
	})

	for object := range objectCh {
		if object.Err != nil {
			return nil, fmt.Errorf("error listing objects: %w", object.Err)
		}

		// Extract dataset ID from the key
		// Key format: {account_id}/{dataset_id}/ or {account_id}/{dataset_id}/...
		key := strings.TrimPrefix(object.Key, prefix)
		parts := strings.SplitN(key, "/", 2)
		if len(parts) > 0 && parts[0] != "" {
			datasetID := parts[0]
			datasetMap[datasetID] = true
		}
	}

	// Convert map to slice and calculate sizes
	var datasets []Dataset
	for datasetID := range datasetMap {
		// Build the parquet glob path for DuckDB
		// Structure: {account_id}/{dataset_id}/data/parquet/**/*.parquet
		basePath := fmt.Sprintf("%s/%s/data/parquet", accountID, datasetID)
		parquetGlob := s.buildS3Path(basePath + "/**/*.parquet")

		// Calculate dataset size
		sizeBytes, fileCount := s.getDatasetSize(ctx, bucket, basePath)

		datasets = append(datasets, Dataset{
			ID:          datasetID,
			Name:        datasetID,
			Path:        basePath,
			ParquetGlob: parquetGlob,
			SizeBytes:   sizeBytes,
			FileCount:   fileCount,
		})
	}

	log.Infof("Found %d datasets for account %s", len(datasets), accountID)
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
func (s *DatasetService) GetDataset(ctx context.Context, datasetID string) (*Dataset, error) {
	accountID := s.config.S3.AccountID

	// Build the parquet glob path
	basePath := fmt.Sprintf("%s/%s/data/parquet", accountID, datasetID)
	parquetGlob := s.buildS3Path(basePath + "/**/*.parquet")

	// Verify the dataset exists by checking if the path has any objects
	prefix := fmt.Sprintf("%s/%s/", accountID, datasetID)
	objectCh := s.minioClient.ListObjects(ctx, s.config.S3.Bucket, minio.ListObjectsOptions{
		Prefix:    prefix,
		MaxKeys:   1,
		Recursive: true,
	})

	// Check if at least one object exists
	object, ok := <-objectCh
	if !ok {
		return nil, fmt.Errorf("dataset %s not found", datasetID)
	}
	if object.Err != nil {
		return nil, fmt.Errorf("error checking dataset: %w", object.Err)
	}

	return &Dataset{
		ID:          datasetID,
		Name:        datasetID,
		Path:        basePath,
		ParquetGlob: parquetGlob,
	}, nil
}

// GetParquetGlob returns the S3 glob path for a dataset's parquet files
func (s *DatasetService) GetParquetGlob(datasetID string) string {
	accountID := s.config.S3.AccountID
	basePath := fmt.Sprintf("%s/%s/data/parquet", accountID, datasetID)
	return s.buildS3Path(basePath + "/**/*.parquet")
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
func (s *DatasetService) GetRecentFiles(ctx context.Context, datasetID string, limit int) ([]RecentFile, error) {
	accountID := s.config.S3.AccountID
	bucket := s.config.S3.Bucket
	prefix := fmt.Sprintf("%s/%s/data/parquet/", accountID, datasetID)

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
func (s *DatasetService) GetFilesInTimeRange(ctx context.Context, datasetID string, start, end time.Time, limit int) ([]RecentFile, error) {
	allFiles, err := s.GetRecentFiles(ctx, datasetID, 1000) // Get more files for filtering
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
