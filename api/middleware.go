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

// AccountIDMiddleware extracts account_id from X-ByteFreezer-Account-ID header
// and stores it in the request context. Used in shared mode when integrated with UI.
func AccountIDMiddleware(fallbackAccountID string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}
