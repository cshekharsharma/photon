package middleware

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/cshekharsharma/photon/coordination/concurrency/ratelimiter"
	"github.com/cshekharsharma/photon/core/logger"
	"github.com/cshekharsharma/photon/utils/rest"
	"github.com/cshekharsharma/photon/utils/rest/apiresponse"
)

// RateLimitMiddleware returns an HTTP middleware that applies rate limiting using the provided limiter and logger.
// It extracts the rate limit key from the request (e.g., IP address or header).
func RateLimitMiddleware(limiter ratelimiter.Limiter, log logger.Logger, timeout time.Duration) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key := extractClientKey(r)
			ctx, cancel := context.WithTimeout(r.Context(), timeout)
			defer cancel()

			result, err := limiter.Allow(ctx, key)
			if err != nil {
				log.Error("Rate limiter error: %v", err)
				httpcode := apiresponse.InternalServerError
				apiresponse.New(false, httpcode, nil, "").Send(w, http.StatusInternalServerError)
				return
			}

			if !result.Allowed {
				if result.RetryAfter > 0 {
					retryAfter := (result.RetryAfter + time.Second - 1) / time.Second
					w.Header().Set("Retry-After", fmt.Sprintf("%d", retryAfter))
				}
				httpcode := apiresponse.TooManyRequests
				apiresponse.New(false, httpcode, nil, "").Send(w, http.StatusTooManyRequests)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

// extractClientKey determines the rate limit key for a given request.
// You can customize this to use headers like X-Forwarded-For, Authorization token, etc.
func extractClientKey(r *http.Request) string {
	ip := rest.GetClientIP(r)
	return fmt.Sprintf("ratelimit:http:%s", ip)
}
