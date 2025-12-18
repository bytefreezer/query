// Licensed under Elastic License 2.0
// See LICENSE.txt for details

package services

import (
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
