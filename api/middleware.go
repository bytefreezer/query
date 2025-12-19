// Licensed under Elastic License 2.0
// See LICENSE.txt for details

package api

import (
	"context"
	"net/http"

	"github.com/bytefreezer/goodies/log"
)

// contextKey is used for storing values in context
type contextKey string

const (
	// AccountIDContextKey is the key for storing account_id in context
	AccountIDContextKey = contextKey("account_id")
	// AuthTokenContextKey is the key for storing auth token in context
	AuthTokenContextKey = contextKey("auth_token")
)

// GetAccountIDFromContext extracts account_id from request context
// Returns empty string if not found
func GetAccountIDFromContext(ctx context.Context) string {
	accountID, ok := ctx.Value(AccountIDContextKey).(string)
	if !ok {
		return ""
	}
	return accountID
}

// GetAuthTokenFromContext extracts auth token from request context
// Returns empty string if not found
func GetAuthTokenFromContext(ctx context.Context) string {
	authToken, ok := ctx.Value(AuthTokenContextKey).(string)
	if !ok {
		return ""
	}
	return authToken
}

// isPublicEndpoint checks if the path should bypass account_id requirement
func isPublicEndpoint(path string) bool {
	// Public API endpoints
	publicPaths := []string{
		"/api/v1/health",
		"/api/v1/limits",
	}
	for _, p := range publicPaths {
		if path == p {
			return true
		}
	}
	// Static UI files don't need account_id
	if path == "/" || path == "/favicon.svg" || path == "/index.html" {
		return true
	}
	return false
}

// AccountIDMiddleware extracts account_id from X-ByteFreezer-Account-ID header
// and stores it in the request context. Used in shared mode when integrated with UI.
func AccountIDMiddleware(fallbackAccountID string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Skip account_id requirement for OPTIONS (CORS preflight)
			if r.Method == "OPTIONS" {
				next.ServeHTTP(w, r)
				return
			}

			// Skip account_id requirement for public endpoints
			if isPublicEndpoint(r.URL.Path) {
				next.ServeHTTP(w, r)
				return
			}

			// Check header for account_id
			accountID := r.Header.Get("X-ByteFreezer-Account-ID")

			// Fall back to config account_id if header not present
			if accountID == "" {
				accountID = fallbackAccountID
			}

			// If we still don't have an account_id, reject the request
			if accountID == "" {
				log.Warn("No account_id provided in header or config")
				http.Error(w, "X-ByteFreezer-Account-ID header required", http.StatusUnauthorized)
				return
			}

			log.Debugf("Request account_id: %s (from header: %v)", accountID, r.Header.Get("X-ByteFreezer-Account-ID") != "")

			// Add account_id to context
			ctx := context.WithValue(r.Context(), AccountIDContextKey, accountID)

			// Also store the auth token for forwarding to control API
			authToken := r.Header.Get("Authorization")
			if authToken != "" {
				ctx = context.WithValue(ctx, AuthTokenContextKey, authToken)
			}

			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}
