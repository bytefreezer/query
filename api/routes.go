// Licensed under Elastic License 2.0
// See LICENSE.txt for details

package api

import (
	"net/http"
	"strings"

	"github.com/bytefreezer/goodies/log"
)

// SetupRoutes configures all API routes
func SetupRoutes(mux *http.ServeMux, handlers *Handlers) {
	// API routes - all under /api/v1/
	// Use method-specific patterns for Go 1.22+ enhanced routing
	mux.HandleFunc("GET /api/v1/datasets", handlers.HandleListDatasets)
	mux.HandleFunc("POST /api/v1/query/generate", handlers.HandleGenerateQuery)
	mux.HandleFunc("POST /api/v1/query/natural", handlers.HandleNaturalQuery)
	mux.HandleFunc("POST /api/v1/query/sql", handlers.HandleSQLQuery)
	mux.HandleFunc("GET /api/v1/schema", handlers.HandleSchema)
	mux.HandleFunc("GET /api/v1/health", handlers.HandleHealth)
	mux.HandleFunc("GET /api/v1/limits", handlers.HandleLimits)

	// Serve static UI files from the ui directory
	fs := http.FileServer(http.Dir("ui"))

	// Catch-all for non-API routes - serve static files
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// Return 404 for API paths that didn't match above
		if strings.HasPrefix(r.URL.Path, "/api/") {
			http.NotFound(w, r)
			return
		}
		// Serve index.html for root path
		if r.URL.Path == "/" {
			http.ServeFile(w, r, "ui/index.html")
			return
		}
		fs.ServeHTTP(w, r)
	})

	log.Info("Routes configured")
}

// LoggingMiddleware logs HTTP requests
func LoggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Infof("%s %s", r.Method, r.URL.Path)
		next.ServeHTTP(w, r)
	})
}

// CORSMiddleware adds CORS headers for development
func CORSMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}
