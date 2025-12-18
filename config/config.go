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
	App     AppConfig     `mapstructure:"app"`
	Logging LoggingConfig `mapstructure:"logging"`
	Server  ServerConfig  `mapstructure:"server"`
	S3      S3Config      `mapstructure:"s3"`
	LLM     LLMConfig     `mapstructure:"llm"`
	Limits  LimitsConfig  `mapstructure:"limits"`
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

// S3Config holds S3/MinIO connection settings
type S3Config struct {
	Region     string `mapstructure:"region"`
	AccessKey  string `mapstructure:"access_key"`
	SecretKey  string `mapstructure:"secret_key"`
	Endpoint   string `mapstructure:"endpoint"`
	Bucket     string `mapstructure:"bucket"`
	AccountID  string `mapstructure:"account_id"` // Customer/tenant account ID
	SSL        bool   `mapstructure:"ssl"`
	URLStyle   string `mapstructure:"url_style"` // "path" or "vhost"
	UseIAMRole bool   `mapstructure:"use_iam_role"`
}

// LLMConfig holds LLM provider settings
type LLMConfig struct {
	Provider   string `mapstructure:"provider"` // anthropic, openai, ollama
	APIKey     string `mapstructure:"api_key"`
	Model      string `mapstructure:"model"`
	OllamaHost string `mapstructure:"ollama_host"`
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
	if cfg.S3.Region == "" {
		cfg.S3.Region = "us-east-1"
	}
	if cfg.S3.URLStyle == "" {
		cfg.S3.URLStyle = "path"
	}
	if cfg.LLM.Provider == "" {
		cfg.LLM.Provider = "anthropic"
	}
	if cfg.LLM.Model == "" {
		if cfg.LLM.Provider == "anthropic" {
			cfg.LLM.Model = "claude-sonnet-4-20250514"
		} else if cfg.LLM.Provider == "openai" {
			cfg.LLM.Model = "gpt-4"
		} else {
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
		cfg.Limits.MaxTimeRangeHours = 24
	}
	if cfg.Limits.MaxRowLimit == 0 {
		cfg.Limits.MaxRowLimit = 100
	}

	return nil
}

// Validate checks that required configuration is present
func (cfg *Config) Validate() error {
	if cfg.S3.Bucket == "" {
		return pkgerrors.New("s3.bucket is required")
	}
	// Note: s3.account_id is optional - can be provided via X-ByteFreezer-Account-ID header
	// for shared mode (when integrated with ByteFreezer UI)
	if cfg.LLM.APIKey == "" && cfg.LLM.Provider != "ollama" {
		return pkgerrors.Errorf("llm.api_key is required for provider %s", cfg.LLM.Provider)
	}
	return nil
}
