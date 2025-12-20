// Licensed under Elastic License 2.0
// See LICENSE.txt for details

package services

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/bytefreezer/goodies/log"
	"github.com/bytefreezer/query/config"
)

// ControlClient handles communication with the ByteFreezer control API
type ControlClient struct {
	baseURL    string
	apiKey     string // Service API key for authentication
	httpClient *http.Client
}

// Tenant represents a tenant from the control API
type Tenant struct {
	ID        string `json:"id"`
	AccountID string `json:"account_id"`
	Name      string `json:"name"`
	Active    bool   `json:"active"`
}

// ControlDataset represents a dataset from the control API
type ControlDataset struct {
	ID       string `json:"id"`
	TenantID string `json:"tenant_id"`
	Name     string `json:"name"`
	Active   bool   `json:"active"`
}

// tenantsResponse represents the response from listing tenants
type tenantsResponse struct {
	Items []Tenant `json:"items"`
	Total int      `json:"total"`
}

// datasetsResponse represents the response from listing datasets
type datasetsResponse struct {
	Items []ControlDataset `json:"items"`
	Total int              `json:"total"`
}

// NewControlClient creates a new control API client
func NewControlClient(cfg *config.Config) *ControlClient {
	if cfg.Control.URL == "" {
		return nil // Standalone mode - no control client needed
	}

	return &ControlClient{
		baseURL: cfg.Control.URL,
		apiKey:  cfg.Control.APIKey,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// GetTenantsForAccount returns all tenants for an account
func (c *ControlClient) GetTenantsForAccount(ctx context.Context, accountID string) ([]Tenant, error) {
	if c == nil {
		return nil, fmt.Errorf("control client not configured (standalone mode)")
	}

	url := fmt.Sprintf("%s/api/v1/accounts/%s/tenants", c.baseURL, accountID)
	log.Debugf("Fetching tenants from control API: %s", url)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Use service API key for authentication
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch tenants: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("control API returned status %d", resp.StatusCode)
	}

	var result tenantsResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	log.Debugf("Found %d tenants for account %s", len(result.Items), accountID)
	return result.Items, nil
}

// GetDatasetsForTenant returns all datasets for a tenant
func (c *ControlClient) GetDatasetsForTenant(ctx context.Context, tenantID string) ([]ControlDataset, error) {
	if c == nil {
		return nil, fmt.Errorf("control client not configured (standalone mode)")
	}

	url := fmt.Sprintf("%s/api/v1/tenants/%s/datasets", c.baseURL, tenantID)
	log.Debugf("Fetching datasets from control API: %s", url)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Use service API key for authentication
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch datasets: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("control API returned status %d", resp.StatusCode)
	}

	var result datasetsResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	log.Debugf("Found %d datasets for tenant %s", len(result.Items), tenantID)
	return result.Items, nil
}

// DatasetS3Credentials represents S3 credentials for a dataset
type DatasetS3Credentials struct {
	Bucket    string `json:"bucket"`
	Region    string `json:"region"`
	Endpoint  string `json:"endpoint,omitempty"`
	AccessKey string `json:"access_key"`
	SecretKey string `json:"secret_key"`
	UseSSL    bool   `json:"use_ssl"`
	Path      string `json:"path"`
}

// GetDatasetS3Credentials returns S3 credentials for a specific dataset
func (c *ControlClient) GetDatasetS3Credentials(ctx context.Context, tenantID, datasetID string) (*DatasetS3Credentials, error) {
	if c == nil {
		return nil, fmt.Errorf("control client not configured (standalone mode)")
	}

	url := fmt.Sprintf("%s/api/v1/tenants/%s/datasets/%s/query-credentials", c.baseURL, tenantID, datasetID)
	log.Debugf("Fetching dataset S3 credentials from control API: %s", url)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Use service API key for authentication
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch S3 credentials: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("control API returned status %d", resp.StatusCode)
	}

	var result DatasetS3Credentials
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	log.Debugf("Got S3 credentials for dataset %s/%s: bucket=%s, endpoint=%s", tenantID, datasetID, result.Bucket, result.Endpoint)
	return &result, nil
}

// QueryErrorReport represents a failed query to report to control
type QueryErrorReport struct {
	AccountID     string `json:"account_id,omitempty"`
	TenantID      string `json:"tenant_id,omitempty"`
	DatasetID     string `json:"dataset_id,omitempty"`
	Question      string `json:"question"`
	GeneratedSQL  string `json:"generated_sql,omitempty"`
	ErrorMessage  string `json:"error_message"`
	ErrorType     string `json:"error_type"` // generation_error, execution_error, validation_error
	SchemaColumns int    `json:"schema_columns,omitempty"`
	LLMProvider   string `json:"llm_provider,omitempty"`
	LLMModel      string `json:"llm_model,omitempty"`
}

// ReportQueryError sends a query error to the control API for debugging
func (c *ControlClient) ReportQueryError(ctx context.Context, report *QueryErrorReport) error {
	if c == nil {
		log.Debug("Skipping query error report (standalone mode)")
		return nil
	}

	url := fmt.Sprintf("%s/api/v1/query-errors", c.baseURL)

	body, err := json.Marshal(report)
	if err != nil {
		return fmt.Errorf("failed to marshal error report: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		log.Warnf("Failed to report query error to control: %v", err)
		return nil // Don't fail the user's request if error reporting fails
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		log.Warnf("Control API returned status %d for query error report", resp.StatusCode)
	} else {
		log.Debugf("Reported query error to control: type=%s", report.ErrorType)
	}

	return nil
}
