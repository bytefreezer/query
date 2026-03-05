// Licensed under Elastic License 2.0
// See LICENSE.txt for details

package config

import (
	"os"
	"strings"

	"github.com/bytefreezer/goodies/log"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/env"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"
	pkgerrors "github.com/pkg/errors"
)

var k = koanf.New(".")

// Config holds all configuration for the query service
type Config struct {
	App             AppConfig             `mapstructure:"app"`
	Logging         LoggingConfig         `mapstructure:"logging"`
	Server          ServerConfig          `mapstructure:"server"`
	LLM             LLMConfig             `mapstructure:"llm"`
	Limits          LimitsConfig          `mapstructure:"limits"`
	Control         ControlConfig         `mapstructure:"control"`
	HealthReporting HealthReportingConfig `mapstructure:"health_reporting"`
}

// ControlConfig holds control API connection settings
type ControlConfig struct {
	URL       string `mapstructure:"url"`
	APIKey    string `mapstructure:"api_key"`
	AccountID string `mapstructure:"account_id"`
}

// HealthReportingConfig holds health reporting settings
type HealthReportingConfig struct {
	Enabled        bool `mapstructure:"enabled"`
	ReportInterval int  `mapstructure:"report_interval"` // seconds
	TimeoutSeconds int  `mapstructure:"timeout_seconds"`
}

// LimitsConfig holds query limits for demo/production
type LimitsConfig struct {
	MaxTimeRangeHours int  `mapstructure:"max_time_range_hours"`
	MaxRowLimit       int  `mapstructure:"max_row_limit"`
	AllowOrderBy      bool `mapstructure:"allow_order_by"`
}

// AppConfig holds application metadata
type AppConfig struct {
	Name    string `mapstructure:"name"`
	Version string `mapstructure:"version"`
}

// LoggingConfig stores logging configuration
type LoggingConfig struct {
	Level    string `mapstructure:"level"`
	Encoding string `mapstructure:"encoding"`
}

// ServerConfig holds HTTP server settings
type ServerConfig struct {
	Port int `mapstructure:"port"`
}

// LLMConfig holds LLM provider settings
type LLMConfig struct {
	Provider   string `mapstructure:"provider"` // anthropic, openai, ollama, or empty to disable
	APIKey     string `mapstructure:"api_key"`
	Model      string `mapstructure:"model"`
	OllamaHost string `mapstructure:"ollama_host"`
}

// LLMEnabled returns true if an LLM provider is configured and usable
func (c *Config) LLMEnabled() bool {
	if c.LLM.Provider == "" {
		return false
	}
	if c.LLM.Provider == "ollama" {
		return true // ollama doesn't need an API key
	}
	return c.LLM.APIKey != ""
}

// LoadConfig loads configuration from YAML file with environment variable overrides
func LoadConfig(cfgFile, envPrefix string, cfg *Config) error {
	if cfgFile == "" {
		cfgFile = "config.yaml"
	}

	// Load from YAML file
	if err := k.Load(file.Provider(cfgFile), yaml.Parser()); err != nil {
		return pkgerrors.Wrapf(err, "failed to parse %s", cfgFile)
	}

	// Load environment variable overrides
	log.Infof("Loading environment variables with prefix: %s", envPrefix)
	envVars := make(map[string]string)
	for _, e := range os.Environ() {
		if strings.HasPrefix(e, envPrefix) {
			parts := strings.SplitN(e, "=", 2)
			if len(parts) == 2 {
				envVars[parts[0]] = parts[1]
			}
		}
	}
	if len(envVars) > 0 {
		log.Infof("Found environment variables: %+v", envVars)
	}

	if err := k.Load(env.Provider(envPrefix, ".", func(s string) string {
		return strings.Replace(strings.ToLower(strings.TrimPrefix(s, envPrefix)), "_", ".", -1)
	}), nil); err != nil {
		return pkgerrors.Wrapf(err, "error loading config from env")
	}

	// Unmarshal into config struct
	if err := k.UnmarshalWithConf("", cfg, koanf.UnmarshalConf{Tag: "mapstructure"}); err != nil {
		return pkgerrors.Wrapf(err, "failed to unmarshal config")
	}

	// Set defaults
	if cfg.App.Name == "" {
		cfg.App.Name = "bytefreezer-query"
	}
	if cfg.App.Version == "" {
		cfg.App.Version = "1.0.0"
	}
	if cfg.Server.Port == 0 {
		cfg.Server.Port = 8000
	}
	if cfg.LLM.Model == "" && cfg.LLM.Provider != "" {
		switch cfg.LLM.Provider {
		case "anthropic":
			cfg.LLM.Model = "claude-sonnet-4-20250514"
		case "openai":
			cfg.LLM.Model = "gpt-4"
		default:
			cfg.LLM.Model = "llama2"
		}
	}
	if cfg.LLM.OllamaHost == "" {
		cfg.LLM.OllamaHost = "http://localhost:11434"
	}
	if cfg.Logging.Level == "" {
		cfg.Logging.Level = "info"
	}
	if cfg.Limits.MaxTimeRangeHours == 0 {
		cfg.Limits.MaxTimeRangeHours = 720 // 30 days default for on-prem
	}
	if cfg.Limits.MaxRowLimit == 0 {
		cfg.Limits.MaxRowLimit = 10000
	}
	// AllowOrderBy defaults to false (zero value), which is fine for demo
	// On-prem configs should set it to true explicitly
	if cfg.HealthReporting.ReportInterval == 0 {
		cfg.HealthReporting.ReportInterval = 30
	}
	if cfg.HealthReporting.TimeoutSeconds == 0 {
		cfg.HealthReporting.TimeoutSeconds = 10
	}

	return nil
}

// Validate checks that required configuration is present
func (cfg *Config) Validate() error {
	if cfg.Control.URL == "" {
		return pkgerrors.New("control.url is required - query service gets S3 credentials from control API per dataset")
	}

	// LLM is optional — if not configured, NL queries are disabled
	if cfg.LLM.Provider != "" && cfg.LLM.Provider != "ollama" && cfg.LLM.APIKey == "" {
		log.Warn("LLM API key not configured — natural language queries will be disabled. Raw SQL queries still work.")
		cfg.LLM.Provider = "" // Disable LLM
	}

	return nil
}

// ValidateStartup logs warnings about potentially misconfigured values
func (cfg *Config) ValidateStartup() {
	if cfg.Server.Port == 0 {
		log.Warn("server.port is 0 — API will bind to a random port")
	}

	if cfg.HealthReporting.Enabled && cfg.HealthReporting.ReportInterval == 0 {
		log.Warn("health_reporting.report_interval is 0 — no health reports will be sent")
	}

	if cfg.HealthReporting.Enabled && cfg.Control.AccountID == "" {
		log.Warn("control.account_id not set — health reporting requires account_id")
	}

	if cfg.HealthReporting.Enabled && cfg.Control.APIKey == "" {
		log.Warn("control.api_key not set — health reporting requires api_key for authentication")
	}

	if !cfg.LLMEnabled() {
		log.Info("LLM not configured — natural language queries disabled, raw SQL queries available")
	}

	// Log effective config summary
	log.Infof("Config summary: port=%d, control=%s, account=%s, llm=%s, health_reporting=%v (interval=%ds)",
		cfg.Server.Port,
		cfg.Control.URL,
		cfg.Control.AccountID,
		cfg.LLM.Provider,
		cfg.HealthReporting.Enabled,
		cfg.HealthReporting.ReportInterval,
	)
	log.Infof("Query limits: max_rows=%d, max_time_range=%dh, allow_order_by=%v",
		cfg.Limits.MaxRowLimit,
		cfg.Limits.MaxTimeRangeHours,
		cfg.Limits.AllowOrderBy,
	)
}
