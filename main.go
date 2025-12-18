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

	// Handle version flag
	if *showVersion {
		fmt.Printf("bytefreezer-query version %s (built %s, commit %s)\n", version, buildTime, gitCommit)
		os.Exit(0)
	}

	// Handle help flag
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

	log.Infof("Configuration loaded: S3 bucket=%s, account_id=%s, LLM provider=%s", cfg.S3.Bucket, cfg.S3.AccountID, cfg.LLM.Provider)

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

	// Initialize SQL generator
	sqlGenerator, err := services.NewSQLGenerator(&cfg, schemaExtractor, datasetService)
	if err != nil {
		log.Fatalf("Failed to initialize SQL generator: %v", err)
	}

	// Initialize handlers
	handlers := api.NewHandlers(&cfg, duckdbClient, schemaExtractor, sqlGenerator, datasetService)

	// Setup routes
	mux := http.NewServeMux()
	api.SetupRoutes(mux, handlers)

	// Apply middleware
	handler := api.CORSMiddleware(api.LoggingMiddleware(mux))

	// Create server
	server := &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.Server.Port),
		Handler:      handler,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	// Start server in goroutine
	go func() {
		log.Infof("Server listening on port %d", cfg.Server.Port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server failed: %v", err)
		}
	}()

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Info("Shutting down server...")

	// Graceful shutdown
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
