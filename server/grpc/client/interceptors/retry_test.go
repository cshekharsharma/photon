package interceptors

import (
	"context"
	cryptorand "crypto/rand"
	"errors"
	"io"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRetryInterceptor(t *testing.T) {
	tests := []struct {
		name            string
		maxRetries      int
		errorSequence   []error // sequence of errors returned by invoker
		expectedErrCode codes.Code
		expectSuccess   bool
	}{
		{
			name:            "NoRetriesNeeded",
			maxRetries:      3,
			errorSequence:   []error{nil},
			expectedErrCode: codes.OK,
			expectSuccess:   true,
		},
		{
			name:            "RetryOnUnavailable",
			maxRetries:      2,
			errorSequence:   []error{status.Error(codes.Unavailable, "transient"), nil},
			expectedErrCode: codes.OK,
			expectSuccess:   true,
		},
		{
			name:       "RetryOnDeadlineExceeded",
			maxRetries: 2,
			errorSequence: []error{
				status.Error(codes.DeadlineExceeded, "timeout"),
				status.Error(codes.DeadlineExceeded, "timeout"),
				status.Error(codes.DeadlineExceeded, "timeout"),
			},
			expectedErrCode: codes.DeadlineExceeded,
			expectSuccess:   false,
		},
		{
			name:            "NonRetryableError",
			maxRetries:      2,
			errorSequence:   []error{status.Error(codes.InvalidArgument, "bad input")},
			expectedErrCode: codes.InvalidArgument,
			expectSuccess:   false,
		},
		{
			name:            "GenericError_NotGRPC",
			maxRetries:      1,
			errorSequence:   []error{errors.New("some error")},
			expectedErrCode: codes.Unknown,
			expectSuccess:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			callCount := 0
			interceptor := RetryInterceptor(tt.maxRetries)

			invoker := func(ctx context.Context, method string, req, reply interface{},
				cc *grpc.ClientConn, opts ...grpc.CallOption) error {
				defer func() { callCount++ }()
				if callCount < len(tt.errorSequence) {
					return tt.errorSequence[callCount]
				}
				return nil
			}

			err := interceptor(
				context.Background(),
				"/test.Service/Method",
				nil,
				nil,
				nil,
				invoker,
			)

			if tt.expectSuccess {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
				st, _ := status.FromError(err)
				assert.Equal(t, tt.expectedErrCode, st.Code())
			}
		})
	}
}

func TestRetryInterceptorStopsBackoffOnContextCancel(t *testing.T) {
	interceptor := RetryInterceptor(3)
	ctx, cancel := context.WithCancel(context.Background())
	callCount := 0

	invoker := func(ctx context.Context, method string, req, reply interface{},
		cc *grpc.ClientConn, opts ...grpc.CallOption) error {
		callCount++
		cancel()
		return status.Error(codes.Unavailable, "transient")
	}

	start := time.Now()
	err := interceptor(
		ctx,
		"/test.Service/Method",
		nil,
		nil,
		nil,
		invoker,
	)

	assert.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, 1, callCount)
	assert.Less(t, time.Since(start), 90*time.Millisecond)
}

func TestRetryInterceptorStopsWhenContextCancelsDuringBackoff(t *testing.T) {
	orig := cryptoRandInt
	cryptoRandInt = func(_ io.Reader, _ *big.Int) (*big.Int, error) {
		return big.NewInt(20_000_000), nil
	}
	defer func() { cryptoRandInt = orig }()

	interceptor := RetryInterceptor(3)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	callCount := 0
	invoker := func(ctx context.Context, method string, req, reply interface{},
		cc *grpc.ClientConn, opts ...grpc.CallOption) error {
		callCount++
		go func() {
			time.Sleep(5 * time.Millisecond)
			cancel()
		}()
		return status.Error(codes.Unavailable, "transient")
	}

	err := interceptor(
		ctx,
		"/test.Service/Method",
		nil,
		nil,
		nil,
		invoker,
	)

	assert.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, 1, callCount)
}

func TestRetryInterceptorDoesNotInvokeCanceledContext(t *testing.T) {
	interceptor := RetryInterceptor(1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	callCount := 0

	err := interceptor(
		ctx,
		"/test.Service/Method",
		nil,
		nil,
		nil,
		func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
			callCount++
			return nil
		},
	)

	assert.ErrorIs(t, err, context.Canceled)
	assert.Zero(t, callCount)
}

func TestRetryInterceptorNegativeRetries(t *testing.T) {
	interceptor := RetryInterceptor(-1)
	callCount := 0

	err := interceptor(
		context.Background(),
		"/test.Service/Method",
		nil,
		nil,
		nil,
		func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
			callCount++
			return status.Error(codes.Unavailable, "transient")
		},
	)

	assert.Error(t, err)
	assert.Equal(t, 1, callCount)
}

func TestRetryHelpers(t *testing.T) {
	assert.True(t, isRetryableGRPCError(context.Background(), status.Error(codes.ResourceExhausted, "slow down")))
	assert.False(t, isRetryableGRPCError(context.Background(), errors.New("plain error")))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	assert.False(t, isRetryableGRPCError(ctx, status.Error(codes.Unavailable, "transient")))

	assert.Equal(t, 200*time.Millisecond, nextRetryDelay(100*time.Millisecond))
	assert.Equal(t, 2*time.Second, nextRetryDelay(2*time.Second))
}

func TestJitterRetryDelay(t *testing.T) {
	orig := cryptoRandInt
	defer func() { cryptoRandInt = orig }()

	assert.Equal(t, time.Nanosecond, jitterRetryDelay(time.Nanosecond))

	cryptoRandInt = func(_ io.Reader, _ *big.Int) (*big.Int, error) {
		return nil, errors.New("entropy unavailable")
	}
	assert.Equal(t, 100*time.Millisecond, jitterRetryDelay(100*time.Millisecond))

	cryptoRandInt = cryptorand.Int
	got := jitterRetryDelay(100 * time.Millisecond)
	assert.GreaterOrEqual(t, got, 80*time.Millisecond)
	assert.LessOrEqual(t, got, 120*time.Millisecond)
}
