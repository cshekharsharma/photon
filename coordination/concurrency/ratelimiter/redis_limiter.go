package ratelimiter

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/cshekharsharma/photon/storage/redis"
	redisv9 "github.com/redis/go-redis/v9"
)

// Result describes a rate-limit decision with useful retry metadata.
type Result struct {
	Allowed    bool
	Remaining  int64
	RetryAfter time.Duration
}

// RedisLimiter implements a token bucket rate limiter using Redis and Lua.
// It supports high-throughput, distributed rate limiting by storing token
// bucket state in Redis hashes and executing all logic atomically via Lua.
type RedisLimiter struct {
	client    redis.RedisInterface // Redis client instance
	maxTokens int                  // Maximum number of tokens in the bucket
	interval  time.Duration        // Time interval to refill one token
	script    RedisScript          // Precompiled Lua script used to apply rate limiting atomically
}

// NewRedisLimiter constructs a RedisLimiter with the specified token capacity and refill interval.
// The limiter stores token counts in Redis and uses Lua scripting to ensure atomic updates.
//
// Parameters:
//   - client: a valid go-redis client connected to the Redis server
//   - maxTokens: the maximum burst capacity (tokens) per key
//   - interval: the duration after which a new token is added to the bucket
//
// Returns:
//   - a *RedisLimiter instance that implements the Limiter interface
func NewRedisLimiter(client redis.RedisInterface, maxTokens int, interval time.Duration) *RedisLimiter {
	limiter := &RedisLimiter{
		client:    client,
		maxTokens: maxTokens,
		interval:  interval,
	}
	limiter.script = limiter.getScript()
	return limiter
}

// Allow determines whether a request is allowed and returns retry metadata.
//
// Parameters:
//   - ctx: context for cancellation and timeout
//   - key: unique identifier for the rate limit bucket (e.g., user ID or IP)
//
// Returns:
//   - result containing allow/deny state, remaining tokens, and retry delay
//   - error if Redis interaction or script execution fails
func (r *RedisLimiter) Allow(ctx context.Context, key string) (Result, error) {
	if ctx == nil {
		return Result{}, fmt.Errorf("context cannot be nil")
	}
	if err := validateRateLimitConfig(r.maxTokens, r.interval); err != nil {
		return Result{}, err
	}
	if strings.TrimSpace(key) == "" {
		return Result{}, fmt.Errorf("rate limit key cannot be empty")
	}

	result, err := r.script.Run(
		ctx,
		r.client.GetRawClient(),
		[]string{key}, r.maxTokens,
		r.interval.Milliseconds(),
	).Result()

	if err != nil {
		return Result{}, fmt.Errorf("redis script error: %w", err)
	}

	return parseRateLimitResult(result)
}

func validateRateLimitConfig(maxTokens int, interval time.Duration) error {
	if maxTokens <= 0 {
		return fmt.Errorf("rate limiter max tokens must be positive")
	}
	if interval <= 0 {
		return fmt.Errorf("rate limiter interval must be positive")
	}
	return nil
}

// getScript returns the precompiled Lua script used for token bucket logic.
// The script ensures atomic access to the Redis key and performs the following:
//
//   - Initializes bucket if missing
//   - Refills tokens based on elapsed time since last refill
//   - Consumes a token if available
//   - Updates token count and last refill timestamp in Redis
//   - Sets a TTL (time to live) to auto-expire unused buckets
//
// Returns:
//   - a *redis.Script that can be executed via script.Run()
func (r *RedisLimiter) getScript() RedisScript {
	return redisv9.NewScript(`
	local key = KEYS[1]
	local maxTokens = tonumber(ARGV[1])
	local refillInterval = tonumber(ARGV[2])
	local nowParts = redis.call("TIME")
	local now = tonumber(nowParts[1]) * 1000 + math.floor(tonumber(nowParts[2]) / 1000)
	local bucket = redis.call("HMGET", key, "tokens", "last")
	local tokens = tonumber(bucket[1])
	local lastRefill = tonumber(bucket[2])

	if tokens == nil then
		tokens = maxTokens
		lastRefill = now
	end

	local delta = math.max(0, now - lastRefill)
	local refill = math.floor(delta / refillInterval)

	if refill > 0 then
		tokens = math.min(maxTokens, tokens + refill)
		lastRefill = now
	end

	local allowed = 0
	if tokens > 0 then
		allowed = 1
		tokens = tokens - 1
	end

	local retryAfter = 0
	if allowed == 0 then
		retryAfter = math.max(1, refillInterval - math.max(0, now - lastRefill))
	end

	redis.call("HMSET", key, "tokens", tokens, "last", lastRefill)
	redis.call("PEXPIRE", key, refillInterval * math.max(2, maxTokens))
	return {allowed, tokens, retryAfter}
	`)
}

func parseRateLimitResult(value any) (Result, error) {
	values, ok := value.([]interface{})
	if !ok {
		return Result{}, fmt.Errorf("unexpected script return type: %T", value)
	}
	if len(values) != 3 {
		return Result{}, fmt.Errorf("unexpected script return length: %d", len(values))
	}

	allowed, err := redisInt64(values[0])
	if err != nil {
		return Result{}, fmt.Errorf("invalid allowed value: %w", err)
	}
	remaining, err := redisInt64(values[1])
	if err != nil {
		return Result{}, fmt.Errorf("invalid remaining value: %w", err)
	}
	retryAfterMillis, err := redisInt64(values[2])
	if err != nil {
		return Result{}, fmt.Errorf("invalid retry-after value: %w", err)
	}
	if retryAfterMillis < 0 {
		return Result{}, fmt.Errorf("invalid retry-after value: negative duration")
	}

	const maxDurationMillis = int64(1<<63-1) / int64(time.Millisecond)
	if retryAfterMillis > maxDurationMillis {
		return Result{}, fmt.Errorf("invalid retry-after value: duration overflow")
	}

	return Result{
		Allowed:    allowed == 1,
		Remaining:  remaining,
		RetryAfter: time.Duration(retryAfterMillis) * time.Millisecond,
	}, nil
}

func redisInt64(value any) (int64, error) {
	n, ok := value.(int64)
	if !ok {
		return 0, fmt.Errorf("unexpected type %T", value)
	}
	return n, nil
}

// RedisScript defines the interface for executing Lua scripts in Redis.
type RedisScript interface {
	Run(ctx context.Context, client redisv9.Scripter, keys []string, args ...interface{}) *redisv9.Cmd
}
