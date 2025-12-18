// Licensed under Elastic License 2.0
// See LICENSE.txt for details

package services

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/bytedance/sonic"
	"github.com/bytefreezer/goodies/log"
	"github.com/bytefreezer/query/config"
)

// SQLGenerator generates SQL from natural language using LLMs
type SQLGenerator struct {
	config          *config.Config
	schemaExtractor *SchemaExtractor
	datasetService  *DatasetService
	httpClient      *http.Client
	systemPrompt    string
}

// NewSQLGenerator creates a new SQL generator
func NewSQLGenerator(cfg *config.Config, schemaExtractor *SchemaExtractor, datasetService *DatasetService) (*SQLGenerator, error) {
	// Load system prompt from file
	promptPath := filepath.Join("prompts", "sql_system.txt")
	promptBytes, err := os.ReadFile(promptPath)
	if err != nil {
		log.Warnf("Failed to load system prompt from %s: %v, using default", promptPath, err)
		promptBytes = []byte(defaultSystemPrompt)
	}

	return &SQLGenerator{
		config:          cfg,
		schemaExtractor: schemaExtractor,
		datasetService:  datasetService,
		httpClient: &http.Client{
			Timeout: 60 * time.Second,
		},
		systemPrompt: string(promptBytes),
	}, nil
}

// GenerateSQL converts a natural language question to SQL
func (g *SQLGenerator) GenerateSQL(ctx context.Context, tenantID, datasetID, question string) (string, error) {
	// Get schema for prompt
	schemaText, err := g.schemaExtractor.FormatSchemaForPrompt(ctx, tenantID, datasetID)
	if err != nil {
		return "", fmt.Errorf("failed to get schema: %w", err)
	}

	// Get the parquet path for this dataset
	parquetPath := g.datasetService.GetParquetGlob(tenantID, datasetID)

	// Get recent files for fast queries
	recentFiles, err := g.datasetService.GetRecentFiles(ctx, tenantID, datasetID, 5)
	if err != nil {
		log.Warnf("Failed to get recent files: %v", err)
		recentFiles = nil
	}

	// Build the full prompt
	prompt := g.buildPrompt(schemaText, parquetPath, recentFiles, question)

	// Call appropriate LLM provider
	var sql string
	switch g.config.LLM.Provider {
	case "anthropic":
		sql, err = g.callAnthropic(ctx, prompt)
	case "openai":
		sql, err = g.callOpenAI(ctx, prompt)
	case "ollama":
		sql, err = g.callOllama(ctx, prompt)
	default:
		return "", fmt.Errorf("unknown LLM provider: %s", g.config.LLM.Provider)
	}

	if err != nil {
		return "", err
	}

	// Clean up the SQL
	log.Debugf("Raw LLM SQL: %s", sql)
	sql = cleanSQL(sql)
	log.Debugf("Cleaned SQL: %s", sql)

	// Basic validation
	if !strings.Contains(strings.ToUpper(sql), "SELECT") {
		return "", fmt.Errorf("generated SQL doesn't contain SELECT statement")
	}

	return sql, nil
}

// buildPrompt creates the full prompt with schema and question
func (g *SQLGenerator) buildPrompt(schema, parquetPath string, recentFiles []RecentFile, question string) string {
	prompt := g.systemPrompt
	prompt = strings.ReplaceAll(prompt, "{schema}", schema)
	prompt = strings.ReplaceAll(prompt, "{parquet_path}", parquetPath)

	// Extract base path (without the glob pattern)
	parquetBase := strings.TrimSuffix(parquetPath, "/**/*.parquet")
	prompt = strings.ReplaceAll(prompt, "{parquet_base}", parquetBase)

	// Add date placeholders for partition targeting
	now := time.Now()
	yesterday := now.AddDate(0, 0, -1)

	prompt = strings.ReplaceAll(prompt, "{today}", now.Format("2006-01-02"))
	prompt = strings.ReplaceAll(prompt, "{year}", fmt.Sprintf("%d", now.Year()))
	prompt = strings.ReplaceAll(prompt, "{month}", fmt.Sprintf("%d", int(now.Month())))
	prompt = strings.ReplaceAll(prompt, "{day}", fmt.Sprintf("%d", now.Day()))
	prompt = strings.ReplaceAll(prompt, "{yesterday}", fmt.Sprintf("%d", yesterday.Day()))

	// Add recent files list for fast queries
	if len(recentFiles) > 0 {
		var fileList []string
		for _, f := range recentFiles {
			fileList = append(fileList, "'"+f.Path+"'")
		}
		recentFilesList := "[" + strings.Join(fileList, ", ") + "]"
		prompt = strings.ReplaceAll(prompt, "{recent_files}", recentFilesList)
	} else {
		prompt = strings.ReplaceAll(prompt, "{recent_files}", "[]")
	}

	prompt = strings.ReplaceAll(prompt, "{question}", question)
	return prompt
}

// callAnthropic calls the Anthropic API
func (g *SQLGenerator) callAnthropic(ctx context.Context, prompt string) (string, error) {
	reqBody := map[string]interface{}{
		"model":      g.config.LLM.Model,
		"max_tokens": 2048,
		"messages": []map[string]string{
			{"role": "user", "content": prompt},
		},
	}

	body, err := sonic.Marshal(reqBody)
	if err != nil {
		return "", fmt.Errorf("failed to marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", "https://api.anthropic.com/v1/messages", bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-api-key", g.config.LLM.APIKey)
	req.Header.Set("anthropic-version", "2023-06-01")

	resp, err := g.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("API request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("API returned %d: %s", resp.StatusCode, string(respBody))
	}

	var result struct {
		Content []struct {
			Text string `json:"text"`
		} `json:"content"`
	}
	if err := sonic.Unmarshal(respBody, &result); err != nil {
		return "", fmt.Errorf("failed to parse response: %w", err)
	}

	if len(result.Content) == 0 {
		return "", fmt.Errorf("no content in response")
	}

	return result.Content[0].Text, nil
}

// callOpenAI calls the OpenAI API
func (g *SQLGenerator) callOpenAI(ctx context.Context, prompt string) (string, error) {
	reqBody := map[string]interface{}{
		"model": g.config.LLM.Model,
		"messages": []map[string]string{
			{"role": "user", "content": prompt},
		},
		"max_tokens": 2048,
	}

	body, err := sonic.Marshal(reqBody)
	if err != nil {
		return "", fmt.Errorf("failed to marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", "https://api.openai.com/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+g.config.LLM.APIKey)

	resp, err := g.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("API request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("API returned %d: %s", resp.StatusCode, string(respBody))
	}

	var result struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := sonic.Unmarshal(respBody, &result); err != nil {
		return "", fmt.Errorf("failed to parse response: %w", err)
	}

	if len(result.Choices) == 0 {
		return "", fmt.Errorf("no choices in response")
	}

	return result.Choices[0].Message.Content, nil
}

// callOllama calls the Ollama API
func (g *SQLGenerator) callOllama(ctx context.Context, prompt string) (string, error) {
	reqBody := map[string]interface{}{
		"model":  g.config.LLM.Model,
		"prompt": prompt,
		"stream": false,
	}

	body, err := sonic.Marshal(reqBody)
	if err != nil {
		return "", fmt.Errorf("failed to marshal request: %w", err)
	}

	url := g.config.LLM.OllamaHost + "/api/generate"
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := g.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("API request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("API returned %d: %s", resp.StatusCode, string(respBody))
	}

	var result struct {
		Response string `json:"response"`
	}
	if err := sonic.Unmarshal(respBody, &result); err != nil {
		return "", fmt.Errorf("failed to parse response: %w", err)
	}

	return result.Response, nil
}

// cleanSQL extracts and cleans SQL from LLM response
func cleanSQL(sql string) string {
	// Remove markdown code blocks
	sql = strings.TrimPrefix(sql, "```sql")
	sql = strings.TrimPrefix(sql, "```")
	sql = strings.TrimSuffix(sql, "```")

	// Trim whitespace
	sql = strings.TrimSpace(sql)

	// Remove any explanatory text before SELECT
	if idx := strings.Index(strings.ToUpper(sql), "SELECT"); idx > 0 {
		sql = sql[idx:]
	}

	// Fix timestamp parsing if needed (for legacy data without BfTs)
	sql = fixTimestampParsing(sql)

	return sql
}

// fixTimestampParsing ensures time filtering uses BfTs (preferred) or fixes InfoTimestamp parsing
func fixTimestampParsing(sql string) string {
	// If using BfTs, no fix needed - it's a simple Unix milliseconds int64
	if strings.Contains(sql, "BfTs") {
		return sql
	}

	// For legacy data without BfTs, try to fix InfoTimestamp parsing
	// Replace strptime(InfoTimestamp, ...) with split_part approach
	if strings.Contains(sql, "strptime(InfoTimestamp") || strings.Contains(sql, "strptime(regexp_replace(InfoTimestamp") {
		correctExpr := "strptime(split_part(InfoTimestamp, '.', 1), '%a, %b %d %Y %H:%M:%S')"

		// Simple replacement - find and replace strptime calls on InfoTimestamp
		for _, pattern := range []string{"strptime(InfoTimestamp", "strptime(regexp_replace(InfoTimestamp"} {
			if idx := strings.Index(sql, pattern); idx != -1 {
				// Find matching closing paren
				depth := 0
				end := idx
				for end < len(sql) {
					if sql[end] == '(' {
						depth++
					} else if sql[end] == ')' {
						depth--
						if depth == 0 {
							end++
							break
						}
					}
					end++
				}
				if end > idx {
					sql = sql[:idx] + correctExpr + sql[end:]
					log.Debugf("Fixed legacy InfoTimestamp parsing")
				}
				break
			}
		}
	}

	return sql
}

// Default system prompt if file not found
const defaultSystemPrompt = `You are a SQL assistant for security log analysis using DuckDB.

SCHEMA:
{schema}

DATA LOCATION:
{parquet_path}

RULES:
1. Output ONLY valid DuckDB SQL - no explanations, no markdown
2. Use read_parquet() with hive_partitioning=true for partition pruning
3. Always add LIMIT 1000 unless user specifies a different limit
4. For time-based queries, filter on partition columns first (year, month, day) for performance
5. Use timestamp column for precise time filtering after partition pruning
6. Available DuckDB functions: regexp_matches, list_aggregate, unnest, epoch_ms, strftime
7. For JSON fields, use json_extract or -> operator
8. Always alias columns for clarity in output

QUERY PATTERNS:

Time range (last N hours):
SELECT * FROM read_parquet('{parquet_path}', hive_partitioning=true)
WHERE year = EXTRACT(YEAR FROM CURRENT_DATE)
  AND month = EXTRACT(MONTH FROM CURRENT_DATE)
  AND timestamp > now() - INTERVAL {N} HOUR
LIMIT 1000

Specific date:
SELECT * FROM read_parquet('{parquet_path}', hive_partitioning=true)
WHERE year = 2025 AND month = 12 AND day = 17
LIMIT 1000

Count by field:
SELECT {field}, COUNT(*) as count
FROM read_parquet('{parquet_path}', hive_partitioning=true)
WHERE year >= EXTRACT(YEAR FROM CURRENT_DATE - INTERVAL 7 DAY)
GROUP BY {field}
ORDER BY count DESC
LIMIT 100

USER QUESTION:
{question}

SQL:`
