// Package ratelimiter provides a pluggable interface and implementations for
// rate limiting strategies, including Redis-backed token bucket limiters.
// This package allows configuring rate limits dynamically based on backends
// and policy options.
package ratelimiter

import (
	"context"
	"fmt"

	"github.com/cshekharsharma/photon/storage/redis"
)

// Limiter defines the interface that all rate limiter implementations must satisfy.
// The core method is Allow, which determines whether a request is permitted based on
// the key (e.g., IP address, user ID, etc.) and current rate limiting state.
type Limiter interface {
	// Allow checks whether a request is allowed for the given key.
	// It returns the decision plus retry metadata for rate-limited callers.
	// An error is returned if the underlying backend or operation fails.
	Allow(ctx context.Context, key string) (Result, error)
}

// NewRateLimiter creates a rate limiter instance based on the provided options.
// The type specified in opts.Type determines the backend strategy used.
// For example, if opts.Type is RateLimiterTypeRedis, the function expects
// opts.Client to be a *redis.Client.

// Parameters:
//   - opts: Options struct containing configuration for the rate limiter.
//
// returns:
//   - Limiter: an instance of the Limiter interface, configured according to opts.
//   - error: an error if the options are invalid or if the rate limiter cannot be created.
func NewRateLimiter(opts *Options) (Limiter, error) {
	if opts == nil {
		return nil, fmt.Errorf("options cannot be nil")
	}

	switch opts.Type {
	case RateLimiterTypeRedis:
		if err := validateRateLimitConfig(opts.MaxTokens, opts.Interval); err != nil {
			return nil, err
		}
		client, ok := opts.Client.(redis.RedisInterface)
		if !ok {
			return nil, fmt.Errorf("invalid client type for Redis rate limiter")
		}
		return NewRedisLimiter(client, opts.MaxTokens, opts.Interval), nil

	default:
		return nil, fmt.Errorf("unsupported rate limiter type: %s", opts.Type)
	}
}
