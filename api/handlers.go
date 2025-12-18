// Licensed under Elastic License 2.0
// See LICENSE.txt for details

package api

import (
	"net/http"

	"github.com/bytedance/sonic"
	"github.com/bytefreezer/goodies/log"
	"github.com/bytefreezer/query/config"
	"github.com/bytefreezer/query/services"
)

// Handlers holds all API handlers
type Handlers struct {
	config          *config.Config
	duckdbClient    *services.DuckDBClient
	schemaExtractor *services.SchemaExtractor
	sqlGenerator    *services.SQLGenerator
	datasetService  *services.DatasetService
}

// NewHandlers creates a new handlers instance
func NewHandlers(cfg *config.Config, duckdb *services.DuckDBClient, schema *services.SchemaExtractor, sqlGen *services.SQLGenerator, datasetSvc *services.DatasetService) *Handlers {
	return &Handlers{
		config:          cfg,
		duckdbClient:    duckdb,
		schemaExtractor: schema,
		sqlGenerator:    sqlGen,
		datasetService:  datasetSvc,
	}
}

// NaturalQueryRequest is the request body for natural language queries
type NaturalQueryRequest struct {
	DatasetID string `json:"dataset_id"`
	Question  string `json:"question"`
}

// SQLQueryRequest is the request body for SQL queries
type SQLQueryRequest struct {
	SQL string `json:"sql"`
}

// QueryResponse is the response for query endpoints
type QueryResponse struct {
	SQL             string          `json:"sql,omitempty"`
	Columns         []string        `json:"columns,omitempty"`
	Rows            [][]interface{} `json:"rows,omitempty"`
	RowCount        int             `json:"row_count"`
	ExecutionTimeMs int64           `json:"execution_time_ms"`
	Error           string          `json:"error,omitempty"`
}

// HealthResponse is the response for health check
type HealthResponse struct {
	Status    string `json:"status"`
	DuckDB    string `json:"duckdb"`
	S3        string `json:"s3"`
	AccountID string `json:"account_id"`
}

// DatasetsResponse is the response for the datasets endpoint
type DatasetsResponse struct {
	AccountID string             `json:"account_id"`
	Datasets  []services.Dataset `json:"datasets"`
	Error     string             `json:"error,omitempty"`
}

// LimitsResponse is the response for query limits
type LimitsResponse struct {
	MaxTimeRangeHours int  `json:"max_time_range_hours"`
	MaxRowLimit       int  `json:"max_row_limit"`
	AllowOrderBy      bool `json:"allow_order_by"`
}

// HandleListDatasets handles GET /api/v1/datasets
func (h *Handlers) HandleListDatasets(w http.ResponseWriter, r *http.Request) {
	accountID := GetAccountIDFromContext(r.Context())
	if accountID == "" {
		writeJSON(w, http.StatusUnauthorized, DatasetsResponse{Error: "account_id not found in context"})
		return
	}

	datasets, err := h.datasetService.ListDatasets(r.Context(), accountID)
	if err != nil {
		log.Warnf("Failed to list datasets: %v", err)
		writeJSON(w, http.StatusInternalServerError, DatasetsResponse{
			AccountID: accountID,
			Error:     err.Error(),
		})
		return
	}

	writeJSON(w, http.StatusOK, DatasetsResponse{
		AccountID: accountID,
		Datasets:  datasets,
	})
}

// HandleGenerateQuery handles POST /api/v1/query/generate - generates SQL without executing
func (h *Handlers) HandleGenerateQuery(w http.ResponseWriter, r *http.Request) {
	accountID := GetAccountIDFromContext(r.Context())
	if accountID == "" {
		writeJSON(w, http.StatusUnauthorized, QueryResponse{Error: "account_id not found in context"})
		return
	}

	var req NaturalQueryRequest
	if err := sonic.ConfigDefault.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, QueryResponse{Error: "Invalid request body"})
		return
	}

	if req.DatasetID == "" {
		writeJSON(w, http.StatusBadRequest, QueryResponse{Error: "dataset_id is required"})
		return
	}

	if req.Question == "" {
		writeJSON(w, http.StatusBadRequest, QueryResponse{Error: "question is required"})
		return
	}

	log.Infof("Generate query for account %s dataset %s: %s", accountID, req.DatasetID, req.Question)

	// Generate SQL from natural language (don't execute)
	sql, err := h.sqlGenerator.GenerateSQL(r.Context(), accountID, req.DatasetID, req.Question)
	if err != nil {
		log.Warnf("SQL generation failed: %v", err)
		writeJSON(w, http.StatusOK, QueryResponse{Error: "Failed to generate SQL: " + err.Error()})
		return
	}

	log.Infof("Generated SQL: %s", sql)

	// Return only the SQL, don't execute
	writeJSON(w, http.StatusOK, QueryResponse{SQL: sql})
}

// HandleNaturalQuery handles POST /api/v1/query/natural - generates and executes
func (h *Handlers) HandleNaturalQuery(w http.ResponseWriter, r *http.Request) {
	accountID := GetAccountIDFromContext(r.Context())
	if accountID == "" {
		writeJSON(w, http.StatusUnauthorized, QueryResponse{Error: "account_id not found in context"})
		return
	}

	var req NaturalQueryRequest
	if err := sonic.ConfigDefault.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, QueryResponse{Error: "Invalid request body"})
		return
	}

	if req.DatasetID == "" {
		writeJSON(w, http.StatusBadRequest, QueryResponse{Error: "dataset_id is required"})
		return
	}

	if req.Question == "" {
		writeJSON(w, http.StatusBadRequest, QueryResponse{Error: "question is required"})
		return
	}

	log.Infof("Natural query on account %s dataset %s: %s", accountID, req.DatasetID, req.Question)

	// Generate SQL from natural language
	sql, err := h.sqlGenerator.GenerateSQL(r.Context(), accountID, req.DatasetID, req.Question)
	if err != nil {
		log.Warnf("SQL generation failed: %v", err)
		writeJSON(w, http.StatusOK, QueryResponse{Error: "Failed to generate SQL: " + err.Error()})
		return
	}

	log.Infof("Generated SQL: %s", sql)

	// Execute the query
	result := h.duckdbClient.ExecuteQuery(r.Context(), sql, 30)

	response := QueryResponse{
		SQL:             sql,
		Columns:         result.Columns,
		Rows:            result.Rows,
		RowCount:        result.RowCount,
		ExecutionTimeMs: result.ExecutionTimeMs,
		Error:           result.Error,
	}

	writeJSON(w, http.StatusOK, response)
}

// HandleSQLQuery handles POST /api/v1/query/sql
func (h *Handlers) HandleSQLQuery(w http.ResponseWriter, r *http.Request) {
	var req SQLQueryRequest
	if err := sonic.ConfigDefault.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, QueryResponse{Error: "Invalid request body"})
		return
	}

	if req.SQL == "" {
		writeJSON(w, http.StatusBadRequest, QueryResponse{Error: "sql is required"})
		return
	}

	log.Infof("SQL query: %s", req.SQL)

	// Execute the query
	result := h.duckdbClient.ExecuteQuery(r.Context(), req.SQL, 30)

	response := QueryResponse{
		Columns:         result.Columns,
		Rows:            result.Rows,
		RowCount:        result.RowCount,
		ExecutionTimeMs: result.ExecutionTimeMs,
		Error:           result.Error,
	}

	writeJSON(w, http.StatusOK, response)
}

// HandleSchema handles GET /api/v1/schema
func (h *Handlers) HandleSchema(w http.ResponseWriter, r *http.Request) {
	accountID := GetAccountIDFromContext(r.Context())
	if accountID == "" {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "account_id not found in context"})
		return
	}

	datasetID := r.URL.Query().Get("dataset_id")
	if datasetID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "dataset_id query parameter is required"})
		return
	}

	schema, err := h.schemaExtractor.GetSchema(r.Context(), accountID, datasetID)
	if err != nil {
		log.Warnf("Schema extraction failed: %v", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, schema)
}

// HandleHealth handles GET /api/v1/health
func (h *Handlers) HandleHealth(w http.ResponseWriter, r *http.Request) {
	// Test DuckDB connection
	result := h.duckdbClient.ExecuteQuery(r.Context(), "SELECT 1", 5)
	duckdbStatus := "connected"
	if result.Error != "" {
		duckdbStatus = "error: " + result.Error
	}

	// Test S3 access by listing datasets (use account_id from context or config)
	accountID := GetAccountIDFromContext(r.Context())
	if accountID == "" {
		accountID = h.config.S3.AccountID
	}

	s3Status := "accessible"
	if accountID != "" {
		_, err := h.datasetService.ListDatasets(r.Context(), accountID)
		if err != nil {
			s3Status = "error: " + err.Error()
		}
	} else {
		s3Status = "not tested (no account_id)"
	}

	response := HealthResponse{
		Status:    "ok",
		DuckDB:    duckdbStatus,
		S3:        s3Status,
		AccountID: accountID,
	}

	if result.Error != "" {
		response.Status = "degraded"
	}

	writeJSON(w, http.StatusOK, response)
}

// HandleLimits handles GET /api/v1/limits
func (h *Handlers) HandleLimits(w http.ResponseWriter, r *http.Request) {
	response := LimitsResponse{
		MaxTimeRangeHours: h.config.Limits.MaxTimeRangeHours,
		MaxRowLimit:       h.config.Limits.MaxRowLimit,
		AllowOrderBy:      h.config.Limits.AllowOrderBy,
	}
	writeJSON(w, http.StatusOK, response)
}

// writeJSON writes a JSON response
func writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := sonic.ConfigDefault.NewEncoder(w).Encode(data); err != nil {
		log.Errorf("Failed to encode response: %v", err)
	}
}
