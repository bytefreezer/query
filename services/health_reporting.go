// Licensed under Elastic License 2.0
// See LICENSE.txt for details

package services

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/bytedance/sonic"
	"github.com/bytefreezer/goodies/log"
)

// HealthReportingService handles health reporting to the control service
type HealthReportingService struct {
	controlURL     string
	accountID      string
	apiKey         string
	serviceType    string
	instanceID     string
	instanceAPI    string
	reportInterval time.Duration
	timeout        time.Duration
	httpClient     *http.Client
	enabled        bool
	stopChan       chan bool
	config         map[string]interface{}
	startTime      time.Time
	uninstallChan  chan struct{}
	upgradeChan    chan string
	queryCount     *int64 // pointer to query counter in main
	errorCount     *int64 // pointer to error counter in main
}

// ServiceRegistrationRequest represents a service registration request
type ServiceRegistrationRequest struct {
	ServiceType   string                 `json:"service_type"`
	InstanceID    string                 `json:"instance_id,omitempty"`
	InstanceAPI   string                 `json:"instance_api"`
	Status        string                 `json:"status,omitempty"`
	Configuration map[string]interface{} `json:"configuration,omitempty"`
}

// ServiceRegistrationResponse represents a service registration response
type ServiceRegistrationResponse struct {
	Success     bool   `json:"success"`
	Message     string `json:"message"`
	InstanceID  string `json:"instance_id"`
	ServiceType string `json:"service_type"`
}

// HealthReportRequest represents a health report request
type HealthReportRequest struct {
	ServiceName   string                 `json:"service_name"`
	ServiceID     string                 `json:"service_id"`
	InstanceAPI   string                 `json:"instance_api"`
	Healthy       bool                   `json:"healthy"`
	Status        string                 `json:"status,omitempty"`
	Configuration map[string]interface{} `json:"configuration,omitempty"`
	Metrics       map[string]interface{} `json:"metrics,omitempty"`
}

// HealthReportResponse represents a health report response from control
type HealthReportResponse struct {
	Success    bool   `json:"success"`
	Message    string `json:"message"`
	Action     string `json:"action,omitempty"`
	UpgradeTag string `json:"upgrade_tag,omitempty"`
}

// NewHealthReportingService creates a new health reporting service
func NewHealthReportingService(controlURL, accountID, apiKey, instanceID, instanceAPI string, reportInterval, timeout time.Duration, config map[string]interface{}) *HealthReportingService {
	if instanceID == "" {
		instanceID, _ = os.Hostname()
		if instanceID == "" {
			instanceID = "unknown"
		}
	}

	return &HealthReportingService{
		controlURL:     controlURL,
		accountID:      accountID,
		apiKey:         apiKey,
		serviceType:    "bytefreezer-query",
		instanceID:     instanceID,
		instanceAPI:    instanceAPI,
		reportInterval: reportInterval,
		timeout:        timeout,
		httpClient: &http.Client{
			Timeout: timeout,
		},
		enabled:       true,
		stopChan:      make(chan bool),
		config:        config,
		startTime:     time.Now(),
		uninstallChan: make(chan struct{}, 1),
		upgradeChan:   make(chan string, 1),
	}
}

// SetQueryCounters sets pointers to query and error counters for metrics
func (h *HealthReportingService) SetQueryCounters(queryCount, errorCount *int64) {
	h.queryCount = queryCount
	h.errorCount = errorCount
}

// Start begins health reporting
func (h *HealthReportingService) Start() {
	if !h.enabled {
		log.Info("Health reporting is disabled")
		return
	}

	log.Infof("Starting health reporting service - reporting to %s every %v", h.controlURL, h.reportInterval)

	if err := h.RegisterService(); err != nil {
		log.Warnf("Failed to register service on startup: %v", err)
	}

	go h.reportingLoop()
}

// Stop stops health reporting and deregisters from control
func (h *HealthReportingService) Stop() {
	if h.enabled {
		close(h.stopChan)
		if err := h.Deregister(); err != nil {
			log.Warnf("Failed to deregister service on shutdown: %v", err)
		}
		log.Info("Health reporting service stopped")
	}
}

// UninstallChan returns a channel that signals when control plane requests uninstall
func (h *HealthReportingService) UninstallChan() <-chan struct{} {
	return h.uninstallChan
}

// UpgradeChan returns a channel that carries the upgrade tag when control plane requests upgrade
func (h *HealthReportingService) UpgradeChan() <-chan string {
	return h.upgradeChan
}

// RegisterService registers this service instance with the control service
func (h *HealthReportingService) RegisterService() error {
	if !h.enabled {
		return nil
	}

	registrationReq := ServiceRegistrationRequest{
		ServiceType:   h.serviceType,
		InstanceID:    h.instanceID,
		InstanceAPI:   h.instanceAPI,
		Status:        "Starting",
		Configuration: h.config,
	}

	reqBody, err := sonic.Marshal(registrationReq)
	if err != nil {
		return fmt.Errorf("failed to marshal registration request: %w", err)
	}

	url := fmt.Sprintf("%s/api/v1/accounts/%s/services/register", h.controlURL, h.accountID)
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(reqBody))
	if err != nil {
		return fmt.Errorf("failed to create registration request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	if h.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+h.apiKey)
	}

	resp, err := h.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to register service: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("service registration failed with status %d", resp.StatusCode)
	}

	var registrationResp ServiceRegistrationResponse
	if err := sonic.ConfigDefault.NewDecoder(resp.Body).Decode(&registrationResp); err != nil {
		return fmt.Errorf("failed to decode registration response: %w", err)
	}

	if !registrationResp.Success {
		return fmt.Errorf("service registration failed: %s", registrationResp.Message)
	}

	log.Infof("Successfully registered service %s with instance ID %s", h.serviceType, registrationResp.InstanceID)
	return nil
}

// Deregister removes this service instance from the control service
func (h *HealthReportingService) Deregister() error {
	if !h.enabled || h.controlURL == "" {
		return nil
	}

	accountID := h.accountID
	if accountID == "" {
		accountID = "system"
	}

	url := fmt.Sprintf("%s/api/v1/accounts/%s/services/%s", h.controlURL, accountID, h.instanceID)
	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		return fmt.Errorf("failed to create deregister request: %w", err)
	}

	if h.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+h.apiKey)
	}

	resp, err := h.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to deregister service: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("service deregistration failed with status %d", resp.StatusCode)
	}

	log.Infof("Successfully deregistered service %s (instance %s)", h.serviceType, h.instanceID)
	return nil
}

// SendHealthReport sends a health report to the control service
func (h *HealthReportingService) SendHealthReport(healthy bool, status string, configuration map[string]interface{}, metrics map[string]interface{}) error {
	if !h.enabled {
		return nil
	}

	healthReq := HealthReportRequest{
		ServiceName:   h.serviceType,
		ServiceID:     h.instanceID,
		InstanceAPI:   h.instanceAPI,
		Healthy:       healthy,
		Status:        status,
		Configuration: configuration,
		Metrics:       metrics,
	}

	reqBody, err := sonic.Marshal(healthReq)
	if err != nil {
		return fmt.Errorf("failed to marshal health report: %w", err)
	}

	url := fmt.Sprintf("%s/api/v1/accounts/%s/services/report", h.controlURL, h.accountID)
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(reqBody))
	if err != nil {
		return fmt.Errorf("failed to create health report request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	if h.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+h.apiKey)
	}

	resp, err := h.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send health report: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("health report failed with status %d", resp.StatusCode)
	}

	// Parse response for pending actions from control plane
	respBody, err := io.ReadAll(resp.Body)
	if err == nil && len(respBody) > 0 {
		var reportResp HealthReportResponse
		if err := sonic.Unmarshal(respBody, &reportResp); err == nil && reportResp.Action != "" {
			log.Warnf("Received action directive from control plane: %s", reportResp.Action)
			if reportResp.Action == "uninstall" {
				select {
				case h.uninstallChan <- struct{}{}:
				default:
				}
			} else if reportResp.Action == "upgrade" && reportResp.UpgradeTag != "" {
				log.Warnf("Upgrade directive received — target tag: %s", reportResp.UpgradeTag)
				select {
				case h.upgradeChan <- reportResp.UpgradeTag:
				default:
				}
			}
		}
	}

	log.Debugf("Successfully sent health report for %s (healthy: %v)", h.serviceType, healthy)
	return nil
}

// reportingLoop runs the periodic health reporting
func (h *HealthReportingService) reportingLoop() {
	ticker := time.NewTicker(h.reportInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			metrics := h.generateMetrics()
			status := "healthy"
			if err := h.SendHealthReport(true, status, h.config, metrics); err != nil {
				log.Warnf("Failed to send health report: %v", err)
			}
		case <-h.stopChan:
			return
		}
	}
}

// generateMetrics generates service runtime metrics
func (h *HealthReportingService) generateMetrics() map[string]interface{} {
	uptimeSeconds := int64(time.Since(h.startTime).Seconds())
	if uptimeSeconds < 1 {
		uptimeSeconds = 1
	}

	metrics := map[string]interface{}{
		"timestamp":         time.Now().Unix(),
		"last_health_check": time.Now().UTC().Format(time.RFC3339),
		"uptime_seconds":    uptimeSeconds,
	}

	// Add query counters if available
	if h.queryCount != nil {
		metrics["queries_executed"] = *h.queryCount
	}
	if h.errorCount != nil {
		metrics["query_errors"] = *h.errorCount
	}

	// Collect system metrics
	sysMetrics := CollectSystemMetrics("/")
	if sysMetrics != nil {
		for k, v := range sysMetrics.ToMap() {
			metrics[k] = v
		}
	}

	return metrics
}
