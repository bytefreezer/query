// Licensed under Elastic License 2.0
// See LICENSE.txt for details

package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/bytefreezer/goodies/log"
	"github.com/bytefreezer/query/api"
	"github.com/bytefreezer/query/config"
	"github.com/bytefreezer/query/services"
)

var (
	version   = "dev"
	buildTime = "unknown"
	gitCommit = "unknown"
	envPrefix = "BYTEFREEZER_QUERY_"
)

func main() {
	var (
		cfgFilePath = flag.String("config", "config.yaml", "Path to configuration file")
		showVersion = flag.Bool("version", false, "Show version and exit")
		showHelp    = flag.Bool("help", false, "Show help and exit")
	)

	flag.Parse()

	if *showVersion {
		fmt.Printf("bytefreezer-query version %s (built %s, commit %s)\n", version, buildTime, gitCommit)
		os.Exit(0)
	}

	if *showHelp {
		fmt.Printf("ByteFreezer Query - Natural language query interface for security logs\n\n")
		fmt.Printf("Usage: %s [options]\n\n", os.Args[0])
		fmt.Printf("Options:\n")
		flag.PrintDefaults()
		os.Exit(0)
	}

	log.Info("Starting ByteFreezer Query Service")

	// Load configuration
	var cfg config.Config
	fmt.Printf("Loading configuration from: %s\n", *cfgFilePath)
	if err := config.LoadConfig(*cfgFilePath, envPrefix, &cfg); err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// Validate configuration
	if err := cfg.Validate(); err != nil {
		log.Fatalf("Invalid configuration: %v", err)
	}

	setLogLevel(cfg.Logging.Level)

	// Log startup config validation warnings
	cfg.ValidateStartup()

	// Build instance ID for health reporting
	instanceID := buildInstanceID()

	log.Infof("Configuration loaded: control=%s, LLM provider=%s, instance_id=%s", cfg.Control.URL, cfg.LLM.Provider, instanceID)

	// Query/error counters for metrics
	var queryCount, errorCount int64

	// Initialize DuckDB client
	duckdbClient, err := services.NewDuckDBClient(&cfg)
	if err != nil {
		log.Fatalf("Failed to initialize DuckDB: %v", err)
	}
	defer duckdbClient.Close()

	// Initialize dataset service
	datasetService, err := services.NewDatasetService(&cfg)
	if err != nil {
		log.Fatalf("Failed to initialize dataset service: %v", err)
	}

	// Initialize schema extractor
	schemaExtractor := services.NewSchemaExtractor(&cfg, duckdbClient, datasetService)

	// Initialize SQL generator (only if LLM is configured)
	var sqlGenerator *services.SQLGenerator
	if cfg.LLMEnabled() {
		sqlGenerator, err = services.NewSQLGenerator(&cfg, schemaExtractor, datasetService)
		if err != nil {
			log.Fatalf("Failed to initialize SQL generator: %v", err)
		}
		log.Infof("LLM enabled: provider=%s, model=%s", cfg.LLM.Provider, cfg.LLM.Model)
	} else {
		log.Info("LLM not configured — natural language queries disabled")
	}

	// Initialize control client for error reporting
	controlClient := services.NewControlClient(&cfg)

	// Initialize handlers
	handlers := api.NewHandlers(&cfg, duckdbClient, schemaExtractor, sqlGenerator, datasetService, controlClient, &queryCount, &errorCount)

	// Setup routes
	mux := http.NewServeMux()
	api.SetupRoutes(mux, handlers)

	// Apply middleware
	// Use account_id from config as fallback for on-prem/standalone mode
	handler := api.CORSMiddleware(
		api.AccountIDMiddleware(cfg.Control.AccountID)(
			api.LoggingMiddleware(mux),
		),
	)

	// Create server
	server := &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.Server.Port),
		Handler:      handler,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	// Start health reporting if enabled
	var healthService *services.HealthReportingService
	if cfg.HealthReporting.Enabled && cfg.Control.URL != "" && cfg.Control.AccountID != "" {
		configMap := map[string]interface{}{
			"version":    version,
			"port":       cfg.Server.Port,
			"llm":        cfg.LLM.Provider,
			"llm_model":  cfg.LLM.Model,
			"limits":     map[string]interface{}{"max_row_limit": cfg.Limits.MaxRowLimit, "max_time_range_hours": cfg.Limits.MaxTimeRangeHours, "allow_order_by": cfg.Limits.AllowOrderBy},
		}

		healthService = services.NewHealthReportingService(
			cfg.Control.URL,
			cfg.Control.AccountID,
			cfg.Control.APIKey,
			instanceID,
			fmt.Sprintf("http://localhost:%d", cfg.Server.Port),
			time.Duration(cfg.HealthReporting.ReportInterval)*time.Second,
			time.Duration(cfg.HealthReporting.TimeoutSeconds)*time.Second,
			configMap,
		)
		healthService.SetQueryCounters(&queryCount, &errorCount)
		healthService.Start()
		defer healthService.Stop()
	}

	// Start server in goroutine
	go func() {
		log.Infof("Server listening on port %d", cfg.Server.Port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server failed: %v", err)
		}
	}()

	// Wait for interrupt signal or control plane directive
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	if healthService != nil {
		select {
		case <-sigChan:
			log.Info("Received shutdown signal")
		case <-healthService.UninstallChan():
			log.Warn("Received uninstall directive from control plane")
		case tag := <-healthService.UpgradeChan():
			log.Warnf("Received upgrade directive from control plane — target tag: %s", tag)
		}
	} else {
		<-sigChan
	}

	log.Info("Shutting down server...")
	log.Infof("Final stats: queries=%d, errors=%d", atomic.LoadInt64(&queryCount), atomic.LoadInt64(&errorCount))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		log.Errorf("Server shutdown error: %v", err)
	}

	log.Info("Server stopped")
}

func setLogLevel(levelStr string) {
	switch strings.ToLower(levelStr) {
	case "debug":
		log.SetMinLogLevel(log.MinLevelDebug)
	case "info":
		log.SetMinLogLevel(log.MinLevelInfo)
	case "warn":
		log.SetMinLogLevel(log.MinLevelWarn)
	case "error":
		log.SetMinLogLevel(log.MinLevelError)
	}
}

// buildInstanceID generates a unique instance ID for this service.
// In Docker: uses HOST_HOSTNAME:containerID format.
// On bare metal: uses hostname.
func buildInstanceID() string {
	hostname, _ := os.Hostname()
	if hostname == "" {
		hostname = "unknown"
	}

	hostHostname := os.Getenv("HOST_HOSTNAME")
	if hostHostname != "" && isDockerContainer() {
		return fmt.Sprintf("%s:%s", hostHostname, hostname)
	}

	return hostname
}

// isDockerContainer checks if we're running inside a Docker container
func isDockerContainer() bool {
	_, err := os.Stat("/.dockerenv")
	return err == nil
}
