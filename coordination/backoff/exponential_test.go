package backoff

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/cshekharsharma/photon/core/logger"
)

func TestWithMinDelay(t *testing.T) {
	b := NewBackoff(WithMinDelay(500 * time.Millisecond))
	if b.MinDelay != 500*time.Millisecond {
		t.Errorf("expected MinDelay to be 500ms, got %v", b.MinDelay)
	}
}

func TestWithMaxDelay(t *testing.T) {
	b := NewBackoff(WithMaxDelay(2 * time.Second))
	if b.MaxDelay != 2*time.Second {
		t.Errorf("expected MaxDelay to be 2s, got %v", b.MaxDelay)
	}
}

func TestWithMaxRetries(t *testing.T) {
	b := NewBackoff(WithMaxRetries(7))
	if b.MaxRetries != 7 {
		t.Errorf("expected MaxRetries to be 7, got %d", b.MaxRetries)
	}
}

func TestWithFactor(t *testing.T) {
	b := NewBackoff(WithFactor(3.0))
	if b.Factor != 3.0 {
		t.Errorf("expected Factor to be 3.0, got %f", b.Factor)
	}
}

func TestWithJitter(t *testing.T) {
	b := NewBackoff(WithJitter(false))
	if b.Jitter != false {
		t.Errorf("expected Jitter to be false, got %v", b.Jitter)
	}
}

func TestWithLogger(t *testing.T) {
	log := logger.Init(&logger.LoggerConfig{
		Name:     "testLogger1",
		Type:     logger.LoggerTypeStdout,
		Provider: logger.LoggerProviderZerolog,
	})
	b := NewBackoff(WithLogger(log))
	if b.Logger != log {
		t.Error("expected Logger to be set")
	}
}

func TestWithRetryIf(t *testing.T) {
	predicate := func(err error) bool { return true }
	b := NewBackoff(WithRetryIf(predicate))
	if b.RetryIf == nil {
		t.Error("expected RetryIf to be set")
	}
}

func TestWithMetrics(t *testing.T) {
	var called bool
	hook := func(ctx RetryContext) { called = true }

	log := logger.Init(&logger.LoggerConfig{
		Name:     "testLogger1Metrics",
		Type:     logger.LoggerTypeStdout,
		Provider: logger.LoggerProviderZerolog,
	})

	b := NewBackoff(
		WithMaxRetries(1),
		WithDeterministic(true),
		WithLogger(log),
		WithMetrics(hook),
	)

	_, err := b.Retry(context.Background(), func(ctx context.Context, args ...any) (any, error) {
		return nil, errors.New("force retry")
	})

	if err == nil {
		t.Error("expected error after retries")
	}

	if b.Metrics == nil {
		t.Error("expected Metrics hook to be set")
	}

	if !called {
		t.Error("expected Metrics hook to be called")
	}
}

func TestWithDelayStrategy(t *testing.T) {
	strategy := func(attempt int) time.Duration { return time.Second }
	b := NewBackoff(WithDelayStrategy(strategy))
	if b.Strategy == nil {
		t.Error("expected Strategy to be set")
	}
}

func TestWithPerAttemptTimeout(t *testing.T) {
	b := NewBackoff(WithPerAttemptTimeout(time.Second))
	if b.PerAttemptTimeout != time.Second {
		t.Errorf("expected PerAttemptTimeout to be 1s, got %v", b.PerAttemptTimeout)
	}
}

func TestWithDeterministic(t *testing.T) {
	b := NewBackoff(WithDeterministic(true))
	if b.Deterministic != true {
		t.Errorf("expected Deterministic to be true, got %v", b.Deterministic)
	}
}

func TestNewBackoff(t *testing.T) {
	b := NewBackoff()
	if b.MinDelay != MinimumDelay || b.MaxDelay != MaximumDelay || b.Factor != ExponentialFactor {
		t.Error("default values not set correctly in NewBackoff")
	}
}

func TestReset(t *testing.T) {
	b := NewBackoff()
	b.Reset()
}

func TestCryptoFloat64_ReadError(t *testing.T) {
	originalRead := cryptoRandRead
	cryptoRandRead = func([]byte) (int, error) {
		return 0, errors.New("entropy unavailable")
	}
	t.Cleanup(func() { cryptoRandRead = originalRead })

	if got := cryptoFloat64(); got != 1 {
		t.Fatalf("expected fallback value 1, got %v", got)
	}
}

func TestGetDelay_CappedByMaxDelay(t *testing.T) {
	b := NewBackoff(
		WithMinDelay(2*time.Second),
		WithMaxDelay(3*time.Second),
		WithFactor(2.0),
		WithJitter(false),
	)

	delay := b.getDelay(2)
	if delay != 3*time.Second {
		t.Errorf("expected delay to be capped at max delay, got %v", delay)
	}
}

func TestRetry(t *testing.T) {
	log := logger.Init(&logger.LoggerConfig{
		Name:     "testLogger2",
		Type:     logger.LoggerTypeStdout,
		Provider: logger.LoggerProviderZerolog,
	})

	var metricCalled bool

	b := NewBackoff(
		WithMaxRetries(3),
		WithDeterministic(true),
		WithLogger(log),
		WithMetrics(func(ctx RetryContext) { metricCalled = true }),
		WithDelayStrategy(func(attempt int) time.Duration { return 100 * time.Millisecond }),
		WithPerAttemptTimeout(10*time.Millisecond),
	)

	// Success after retries
	attempts := 0
	val, err := b.Retry(context.Background(), func(ctx context.Context, args ...any) (any, error) {
		attempts++
		limit := args[0].(int)
		if attempts < limit {
			return nil, errors.New("fail")
		}
		return "success", nil
	}, 2)

	if err != nil {
		t.Errorf("expected success, got %v", err)
	}
	if val.(string) != "success" {
		t.Errorf("expected success value, got %v", val)
	}
	if !metricCalled {
		t.Error("expected metrics hook to be called")
	}

	// Test RetryIf returns false
	b = NewBackoff(
		WithMaxRetries(3),
		WithRetryIf(func(err error) bool { return false }),
		WithLogger(log),
	)
	_, err = b.Retry(context.Background(), func(ctx context.Context, args ...any) (any, error) {
		return nil, errors.New("non-retryable")
	})
	if err == nil {
		t.Error("expected error when RetryIf stops retries")
	}

	// Test context cancellation before function starts
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = b.Retry(ctx, func(ctx context.Context, args ...any) (any, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			return nil, errors.New("some error")
		}
	})
	if err != context.Canceled {
		t.Errorf("expected context.Canceled, got %v", err)
	}

	// Test context cancel during sleep delay (to cover select case <-ctx.Done())
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()

	b = NewBackoff(
		WithMaxRetries(3),
		WithLogger(log),
		WithDelayStrategy(func(attempt int) time.Duration { return 500 * time.Millisecond }), // longer delay to allow cancel
	)

	go func() {
		time.Sleep(100 * time.Millisecond) // Give it time to start sleeping
		cancel2()
	}()

	val, err = b.Retry(ctx2, func(ctx context.Context, args ...any) (any, error) {
		return nil, errors.New("fail always")
	})
	if err != context.Canceled {
		t.Errorf("expected context.Canceled during delay, got %v", err)
	}
	if val != nil {
		t.Errorf("expected nil value when context canceled during delay, got %v", val)
	}

	// Test exhausting max retries
	b = NewBackoff(
		WithMaxRetries(1),
		WithLogger(log),
		WithPerAttemptTimeout(5*time.Millisecond),
	)
	val, err = b.Retry(context.Background(), func(ctx context.Context, args ...any) (any, error) {
		return nil, errors.New("always fail")
	})
	if err != ErrMaxRetriesExceeded {
		t.Errorf("expected ErrMaxRetriesExceeded, got %v", err)
	}
	if val != nil {
		t.Errorf("expected nil value when retries exhausted, got %v", val)
	}
}

func TestRetryDoesNotInvokeCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	called := false

	_, err := NewBackoff().Retry(ctx, func(context.Context, ...any) (any, error) {
		called = true
		return nil, nil
	})

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if called {
		t.Fatal("retry function should not run after context cancellation")
	}
}
