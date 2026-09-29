// Package backoff provides a configurable, production-grade exponential backoff implementation
// with support for jitter, selective retries, per-attempt timeout, metrics hooks, and pluggable strategies.
package backoff

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/binary"
	"errors"
	"math"
	"sync"
	"time"

	"github.com/cshekharsharma/photon/core/logger"
)

var (
	// ErrMaxRetriesExceeded is returned when all retry attempts have been exhausted.
	ErrMaxRetriesExceeded = errors.New("max retries exceeded")
	ErrNilBackoff         = errors.New("backoff cannot be nil")
	ErrNilContext         = errors.New("context cannot be nil")
	ErrNilFunction        = errors.New("retry function cannot be nil")

	MinimumDelay        = 100 * time.Millisecond // Default minimum delay for backoff
	MaximumDelay        = 10 * time.Second       // Default maximum delay for backoff
	MaximumRetries      = 5                      // Default maximum number of retry attempts
	ExponentialFactor   = 2.0                    // Default exponential factor for delay growth
	DeterministicJitter = true                   // Default deterministic mode for testing
)

var cryptoRandRead = cryptorand.Read

// RetryContext holds metadata about a retry attempt, including the attempt number,
// delay used, and the error returned by the function.
type RetryContext struct {
	Attempt   int
	DelayUsed time.Duration
	Error     error
}

// DelayStrategy defines a pluggable delay calculation function for backoff.
type DelayStrategy func(attempt int) time.Duration

// RetryPredicate determines whether an error is retryable.
type RetryPredicate func(err error) bool

// MetricsHook defines a callback function to report retry metrics.
type MetricsHook func(ctx RetryContext)

// Backoff represents a robust and configurable exponential backoff strategy.
// It supports jitter, delay customization, metrics, retry conditions, and more.
type Backoff struct {
	MinDelay          time.Duration  // Minimum delay between retries
	MaxDelay          time.Duration  // Maximum delay between retries
	MaxRetries        int            // Maximum number of retry attempts
	Factor            float64        // Exponential factor for delay growth
	Jitter            bool           // Enables jitter to randomize delay
	PerAttemptTimeout time.Duration  // Optional timeout for each retry attempt
	Logger            logger.Logger  // Optional logger hook
	RetryIf           RetryPredicate // Optional predicate to filter retryable errors
	Metrics           MetricsHook    // Optional metrics callback
	Strategy          DelayStrategy  // Optional custom delay strategy
	Deterministic     bool           // Enables deterministic mode for testing

	mutex sync.Mutex
}

// Option is a functional option type for configuring a Backoff instance.
type Option func(*Backoff)

// WithMinDelay sets the minimum backoff delay.
func WithMinDelay(d time.Duration) Option {
	return func(b *Backoff) {
		b.MinDelay = d
	}
}

// WithMaxDelay sets the maximum backoff delay.
func WithMaxDelay(d time.Duration) Option {
	return func(b *Backoff) {
		b.MaxDelay = d
	}
}

// WithMaxRetries sets the maximum number of retry attempts.
func WithMaxRetries(n int) Option {
	return func(b *Backoff) {
		b.MaxRetries = n
	}
}

// WithFactor sets the exponential growth factor for delays.
func WithFactor(f float64) Option {
	return func(b *Backoff) {
		b.Factor = f
	}
}

// WithJitter enables or disables jitter for backoff delays.
func WithJitter(enabled bool) Option {
	return func(b *Backoff) {
		b.Jitter = enabled
	}
}

// WithLogger attaches a logger function to observe retry attempts.
func WithLogger(logger logger.Logger) Option {
	return func(b *Backoff) {
		b.Logger = logger
	}
}

// WithRetryIf specifies a predicate to determine retryable errors.
func WithRetryIf(predicate RetryPredicate) Option {
	return func(b *Backoff) {
		b.RetryIf = predicate
	}
}

// WithMetrics attaches a metrics reporting hook to retry attempts.
func WithMetrics(hook MetricsHook) Option {
	return func(b *Backoff) {
		b.Metrics = hook
	}
}

// WithDelayStrategy sets a custom delay calculation strategy.
func WithDelayStrategy(strategy DelayStrategy) Option {
	return func(b *Backoff) {
		b.Strategy = strategy
	}
}

// WithPerAttemptTimeout sets a timeout for each individual retry attempt.
func WithPerAttemptTimeout(timeout time.Duration) Option {
	return func(b *Backoff) {
		b.PerAttemptTimeout = timeout
	}
}

// WithDeterministic enables deterministic delay behavior for reproducible tests.
func WithDeterministic(enabled bool) Option {
	return func(b *Backoff) {
		b.Deterministic = enabled
	}
}

// NewBackoff creates a new Backoff instance with the provided options.
func NewBackoff(opts ...Option) *Backoff {
	b := &Backoff{
		MinDelay:   MinimumDelay,
		MaxDelay:   MaximumDelay,
		MaxRetries: MaximumRetries,
		Factor:     ExponentialFactor,
		Jitter:     DeterministicJitter,
	}

	for _, opt := range opts {
		opt(b)
	}

	b.normalize()
	return b
}

func (b *Backoff) normalize() {
	if b.MinDelay <= 0 {
		b.MinDelay = MinimumDelay
	}
	if b.MaxDelay <= 0 {
		b.MaxDelay = MaximumDelay
	}
	if b.MaxDelay < b.MinDelay {
		b.MaxDelay = b.MinDelay
	}
	if b.MaxRetries < 0 {
		b.MaxRetries = 0
	}
	if b.Factor <= 1 {
		b.Factor = ExponentialFactor
	}
	if b.PerAttemptTimeout < 0 {
		b.PerAttemptTimeout = 0
	}
}

// RetryTransient retries ordinary errors while respecting caller cancellation.
func RetryTransient(err error) bool {
	return err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded)
}

// RetryIfErrorIs retries errors matching target.
func RetryIfErrorIs(target error) RetryPredicate {
	return func(err error) bool {
		return errors.Is(err, target)
	}
}

// RetryIfAny retries when any predicate marks the error retryable.
func RetryIfAny(predicates ...RetryPredicate) RetryPredicate {
	return func(err error) bool {
		for _, predicate := range predicates {
			if predicate != nil && predicate(err) {
				return true
			}
		}
		return false
	}
}

// getDelay computes the delay for a given retry attempt using exponential backoff logic.
func (b *Backoff) getDelay(attempt int) time.Duration {
	if b.Strategy != nil {
		return b.Strategy(attempt)
	}

	delay := float64(b.MinDelay) * math.Pow(b.Factor, float64(attempt-1))

	if b.Jitter {
		jitter := 0.75 + b.randFloat()*0.5
		delay *= jitter
	}

	if delay > float64(b.MaxDelay) {
		delay = float64(b.MaxDelay)
	}

	return time.Duration(delay)
}

// randFloat safely generates a random float64 between 0 and 1.
func (b *Backoff) randFloat() float64 {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	if b.Deterministic {
		return 1.0
	}

	return cryptoFloat64()
}

// Retry executes a given function with retry logic using exponential backoff.
//
// The function `fn` should accept a context and a variadic list of arguments (any types).
// It must return a result of type `any` and an `error`. The retry mechanism only checks the error
// to determine if the function should be retried.
//
// If the function succeeds (returns `nil` error), the value is returned immediately.
// If the function returns an error, it will be retried up to `MaxRetries` times,
// with delays between attempts determined by the backoff configuration.
//
// The optional fields in Backoff (e.g., Logger, Metrics, RetryIf) can be used to control behavior.
//
// Parameters:
//   - ctx: Context for cancellation or timeout of the overall retry operation.
//   - fn: Function to execute. Must be of the form: func(ctx context.Context, args ...any) (any, error).
//   - args: Variadic list of arguments to pass to fn.
//
// Returns:
//   - Value of type `any` if successful.
//   - Error if retries are exhausted or context is canceled.
//
// Example:
//
//	val, err := b.Retry(ctx, func(ctx context.Context, args ...any) (any, error) {
//	    x := args[0].(int)
//	    y := args[1].(string)
//	    if x < 5 {
//	        return nil, errors.New("x too small")
//	    }
//	    return fmt.Sprintf(\"Processed %d and %s\", x, y), nil
//	}, 10, \"hello\")
//
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Println(val)
func (b *Backoff) Retry(ctx context.Context, fn func(context.Context, ...any) (any, error), args ...any) (any, error) {
	if b == nil {
		return nil, ErrNilBackoff
	}
	if ctx == nil {
		return nil, ErrNilContext
	}
	if fn == nil {
		return nil, ErrNilFunction
	}

	for attempt := 0; attempt <= b.MaxRetries; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		innerCtx := ctx
		var cancel context.CancelFunc

		if b.PerAttemptTimeout > 0 {
			innerCtx, cancel = context.WithTimeout(ctx, b.PerAttemptTimeout)
		}

		val, err := fn(innerCtx, args...)

		if cancel != nil {
			cancel()
		}

		if err == nil {
			return val, nil
		}

		if b.RetryIf != nil && !b.RetryIf(err) {
			return val, err
		}

		retryCtx := RetryContext{
			Attempt: attempt + 1,
			Error:   err,
		}

		if attempt < b.MaxRetries {
			delay := b.getDelay(attempt + 1)
			retryCtx.DelayUsed = delay

			if b.Logger != nil {
				b.Logger.Error("Retry attempt %d failed: %v, retrying in %s",
					retryCtx.Attempt, retryCtx.Error, delay)
			}

			if b.Metrics != nil {
				b.Metrics(retryCtx)
			}

			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, ctx.Err()
			case <-timer.C:
			}
		}
	}

	return nil, ErrMaxRetriesExceeded
}

// Reset is retained for backwards compatibility. The crypto random source is stateless.
func (b *Backoff) Reset() {
	b.mutex.Lock()
	defer b.mutex.Unlock()
}

func cryptoFloat64() float64 {
	var buf [8]byte
	if _, err := cryptoRandRead(buf[:]); err != nil {
		return 1
	}
	return float64(binary.BigEndian.Uint64(buf[:])) / float64(^uint64(0))
}
