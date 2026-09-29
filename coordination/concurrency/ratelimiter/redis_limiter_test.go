package ratelimiter

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/cshekharsharma/photon/storage/redis"

	redisv9 "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type mockScript struct {
	mock.Mock
}

func (m *mockScript) Run(ctx context.Context, client redisv9.Scripter, keys []string, args ...interface{}) *redisv9.Cmd {
	call := m.Called(ctx, client, keys, args)
	cmd := redisv9.NewCmd(ctx)
	if val, ok := call.Get(0).(error); ok {
		cmd.SetErr(val)
	} else {
		cmd.SetVal(call.Get(0))
	}
	return cmd
}

type mockRedisInterface struct {
	mock.Mock
}

func (m *mockRedisInterface) GetClient() redis.RedisClientInterface {
	args := m.Called()
	return args.Get(0).(redis.RedisClientInterface)
}

func (m *mockRedisInterface) GetRawClient() *redisv9.Client {
	args := m.Called()
	return args.Get(0).(*redisv9.Client)
}

func (m *mockRedisInterface) SetClient(client redis.RedisClientInterface) {
	m.Called(client)
}

func (m *mockRedisInterface) Close() error {
	args := m.Called()
	return args.Error(0)
}

func TestRedisLimiter_Allow_WithMockedScript(t *testing.T) {
	ctx := context.Background()
	mockRedis := new(mockRedisInterface)
	mockScript := new(mockScript)

	mockRedis.On("GetRawClient").Return(&redisv9.Client{})

	limiter := NewRedisLimiter(mockRedis, 3, time.Second)
	limiter.script = &redisv9.Script{}

	limiter.script = mockScript
	mockScript.On("Run", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]interface{}{int64(1), int64(2), int64(0)})

	result, err := limiter.Allow(ctx, "ratelimit:unit:test")
	assert.NoError(t, err)
	assert.True(t, result.Allowed)
	mockRedis.AssertExpectations(t)
	mockScript.AssertExpectations(t)
}

func TestRedisLimiter_Allow_ReturnsMetadata(t *testing.T) {
	ctx := context.Background()
	mockRedis := new(mockRedisInterface)
	mockScript := new(mockScript)

	mockRedis.On("GetRawClient").Return(&redisv9.Client{})

	limiter := NewRedisLimiter(mockRedis, 3, time.Second)
	limiter.script = mockScript

	mockScript.On("Run", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return([]interface{}{int64(0), int64(0), int64(750)})

	result, err := limiter.Allow(ctx, "ratelimit:unit:limited")
	assert.NoError(t, err)
	assert.False(t, result.Allowed)
	assert.Equal(t, int64(0), result.Remaining)
	assert.Equal(t, 750*time.Millisecond, result.RetryAfter)
	mockRedis.AssertExpectations(t)
	mockScript.AssertExpectations(t)
}

func TestRedisLimiter_Allow_Validation(t *testing.T) {
	mockRedis := new(mockRedisInterface)
	limiter := NewRedisLimiter(mockRedis, 3, time.Second)

	var nilContext context.Context
	_, err := limiter.Allow(nilContext, "ratelimit:unit:nil-context")
	assert.EqualError(t, err, "context cannot be nil")

	_, err = limiter.Allow(context.Background(), " \n\t")
	assert.EqualError(t, err, "rate limit key cannot be empty")

	limiter.maxTokens = 0
	_, err = limiter.Allow(context.Background(), "ratelimit:unit:bad-config")
	assert.EqualError(t, err, "rate limiter max tokens must be positive")

	limiter.maxTokens = 1
	limiter.interval = 0
	_, err = limiter.Allow(context.Background(), "ratelimit:unit:bad-config")
	assert.EqualError(t, err, "rate limiter interval must be positive")
}

func TestRedisLimiter_Allow_ParseErrors(t *testing.T) {
	tests := []struct {
		name    string
		value   any
		message string
	}{
		{name: "wrong type", value: int64(1), message: "unexpected script return type: int64"},
		{name: "wrong length", value: []interface{}{int64(1)}, message: "unexpected script return length: 1"},
		{name: "bad allowed", value: []interface{}{"yes", int64(0), int64(0)}, message: "invalid allowed value: unexpected type string"},
		{name: "bad remaining", value: []interface{}{int64(1), "0", int64(0)}, message: "invalid remaining value: unexpected type string"},
		{name: "bad retry after", value: []interface{}{int64(1), int64(0), "0"}, message: "invalid retry-after value: unexpected type string"},
		{name: "negative retry after", value: []interface{}{int64(1), int64(0), int64(-1)}, message: "invalid retry-after value: negative duration"},
		{name: "overflow retry after", value: []interface{}{int64(1), int64(0), int64(1<<63 - 1)}, message: "invalid retry-after value: duration overflow"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parseRateLimitResult(tt.value)
			assert.EqualError(t, err, tt.message)
		})
	}
}

func TestRedisLimiter_Allow_ScriptError(t *testing.T) {
	ctx := context.Background()
	mockRedis := new(mockRedisInterface)
	rds := redisv9.NewClient(&redisv9.Options{Addr: "localhost:16379"})
	mockRedis.On("GetRawClient").Return(rds)

	limiter := NewRedisLimiter(mockRedis, 3, time.Second)
	mockScript := &mockScript{}
	mockScript.On("Run", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("redis unavailable"))

	limiter.script = mockScript

	_, err := limiter.Allow(ctx, "ratelimit:unit:err")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "redis script error")
	mockRedis.AssertExpectations(t)
}

func TestRedisLimiter_Allow_RunReturnsError(t *testing.T) {
	ctx := context.Background()
	mockRedis := new(mockRedisInterface)
	mockScript := new(mockScript)

	mockRedis.On("GetRawClient").Return(&redisv9.Client{})

	limiter := NewRedisLimiter(mockRedis, 3, time.Second)
	limiter.script = mockScript

	mockScript.
		On("Run", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(errors.New("lua exploded"))

	result, err := limiter.Allow(ctx, "ratelimit:unit:script-err")
	assert.False(t, result.Allowed)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "redis script error")
	mockRedis.AssertExpectations(t)
	mockScript.AssertExpectations(t)
}
