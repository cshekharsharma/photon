package interceptors

import (
	"context"
	cryptorand "crypto/rand"
	"math/big"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var cryptoRandInt = cryptorand.Int

// RetryInterceptor retries gRPC requests on transient errors with exponential backoff.
func RetryInterceptor(maxRetries int) grpc.UnaryClientInterceptor {
	if maxRetries < 0 {
		maxRetries = 0
	}

	return func(
		ctx context.Context,
		method string,
		req interface{},
		reply interface{},
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		var err error
		backoffDelay := 100 * time.Millisecond

		for i := 0; i <= maxRetries; i++ {
			if ctx.Err() != nil {
				return ctx.Err()
			}

			err = invoker(ctx, method, req, reply, cc, opts...)
			if err == nil {
				return nil
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}

			if !isRetryableGRPCError(ctx, err) {
				return err
			}

			if i < maxRetries {
				timer := time.NewTimer(jitterRetryDelay(backoffDelay))
				select {
				case <-ctx.Done():
					timer.Stop()
					return ctx.Err()
				case <-timer.C:
				}
				backoffDelay = nextRetryDelay(backoffDelay)
				continue
			}
		}

		return err
	}
}

func isRetryableGRPCError(ctx context.Context, err error) bool {
	if ctx.Err() != nil {
		return false
	}

	st, ok := status.FromError(err)
	if !ok {
		return false
	}

	switch st.Code() {
	case codes.Unavailable, codes.ResourceExhausted:
		return true
	case codes.DeadlineExceeded:
		return ctx.Err() == nil
	default:
		return false
	}
}

func nextRetryDelay(delay time.Duration) time.Duration {
	const maxDelay = 2 * time.Second
	delay *= 2
	if delay > maxDelay {
		return maxDelay
	}
	return delay
}

func jitterRetryDelay(delay time.Duration) time.Duration {
	span := delay / 5
	if span <= 0 {
		return delay
	}

	n, err := cryptoRandInt(cryptorand.Reader, big.NewInt(int64(2*span+1)))
	if err != nil {
		return delay
	}
	return delay + time.Duration(n.Int64()) - span
}
