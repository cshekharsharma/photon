// Package grpc provides a production-grade gRPC client wrapper with support
// for TLS, retry, backoff, interceptors, load balancing, and testability.
package client

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/cshekharsharma/photon/server/grpc/client/interceptors"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
)

var grpcNewClientFn = grpc.NewClient

const DefaultMinConnectTimeout = 5 * time.Second

var defaultMinConnectTimeoutNanos atomic.Int64

func init() {
	SetDefaultMinConnectTimeout(DefaultMinConnectTimeout)
}

// SetDefaultMinConnectTimeout configures the package default used when
// ClientOptions.MinConnectTimeout is unset.
func SetDefaultMinConnectTimeout(timeout time.Duration) {
	if timeout <= 0 {
		timeout = DefaultMinConnectTimeout
	}
	defaultMinConnectTimeoutNanos.Store(int64(timeout))
}

// GetDefaultMinConnectTimeout returns the current package default min connect timeout.
func GetDefaultMinConnectTimeout() time.Duration {
	return time.Duration(defaultMinConnectTimeoutNanos.Load())
}

// Client defines the interface for GRPCClient, making it testable.
type Client interface {
	Conn() *grpc.ClientConn
	HealthCheck(ctx context.Context) error
	Close() error
}

// GRPCClient is a production-grade wrapper over grpc.ClientConn.
type GRPCClient struct {
	conn *grpc.ClientConn
	opts *ClientOptions
}

// NewGRPCClient creates a new GRPCClient instance.
func NewGRPCClient(opts *ClientOptions) (Client, error) {
	if opts == nil {
		return nil, errors.New("client options are required")
	}

	if opts.Target == "" {
		return nil, errors.New("target address is required")
	}

	backoffConfig := constructBackoffConfig(opts)
	if opts.MinConnectTimeout <= 0 {
		opts.MinConnectTimeout = GetDefaultMinConnectTimeout()
	}

	dialOptions := []grpc.DialOption{
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff:           backoffConfig,
			MinConnectTimeout: opts.MinConnectTimeout,
		}),
		grpc.WithChainUnaryInterceptor(
			interceptors.LoggingInterceptor(opts.Logger),
			interceptors.RetryInterceptor(opts.MaxRetries),
		),
	}

	if opts.TLSConfig != nil {
		creds := credentials.NewTLS(opts.TLSConfig)
		dialOptions = append(dialOptions, grpc.WithTransportCredentials(creds))
	} else if opts.Insecure {
		dialOptions = append(dialOptions, grpc.WithTransportCredentials(insecure.NewCredentials()))
	} else {
		return nil, errors.New("either TLSConfig or Insecure must be set")
	}

	if opts.EnableLoadBalancing {
		dialOptions = append(
			dialOptions, grpc.WithDefaultServiceConfig(
				`{"loadBalancingPolicy":"round_robin"}`,
			),
		)
	}

	if opts.ContextDialer != nil {
		dialOptions = append(dialOptions, grpc.WithContextDialer(opts.ContextDialer))
	}
	if len(opts.DialOptions) > 0 {
		dialOptions = append(dialOptions, opts.DialOptions...)
	}

	conn, err := grpcNewClientFn(opts.Target, dialOptions...)
	if err != nil {
		return nil, fmt.Errorf("failed to dial gRPC: %w", err)
	}

	return &GRPCClient{conn: conn, opts: opts}, nil
}

// Conn returns the underlying gRPC connection.
func (c *GRPCClient) Conn() *grpc.ClientConn {
	return c.conn
}

// Close closes the gRPC client connection.
func (c *GRPCClient) Close() error {
	return c.conn.Close()
}

// HealthCheck checks if the gRPC server is healthy.
func (c *GRPCClient) HealthCheck(ctx context.Context) error {
	ctx, cancel := c.healthCheckContext(ctx)
	defer cancel()

	hc := grpc_health_v1.NewHealthClient(c.conn)
	resp, err := hc.Check(ctx, &grpc_health_v1.HealthCheckRequest{Service: ""})

	if err != nil {
		return fmt.Errorf("health check failed: %w", err)
	}

	if resp.GetStatus() != grpc_health_v1.HealthCheckResponse_SERVING {
		return fmt.Errorf("service not healthy: %s", resp.GetStatus())
	}

	return nil
}

func (c *GRPCClient) healthCheckContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, hasDeadline := ctx.Deadline(); hasDeadline {
		return ctx, func() {}
	}

	timeout := GetDefaultMinConnectTimeout()
	if c != nil && c.opts != nil && c.opts.MinConnectTimeout > 0 {
		timeout = c.opts.MinConnectTimeout
	}
	return context.WithTimeout(ctx, timeout)
}

func constructBackoffConfig(opts *ClientOptions) backoff.Config {
	defaultBackoffConfig := backoff.Config{
		BaseDelay:  100 * time.Millisecond,
		Multiplier: 1.6,
		Jitter:     0.2,
		MaxDelay:   5 * time.Second,
	}

	if opts.BackoffConfig != nil {
		if opts.BackoffConfig.BaseDelay != 0 {
			defaultBackoffConfig.BaseDelay = opts.BackoffConfig.BaseDelay
		}

		if opts.BackoffConfig.Multiplier != 0 {
			defaultBackoffConfig.Multiplier = opts.BackoffConfig.Multiplier
		}

		if opts.BackoffConfig.Jitter != 0 {
			defaultBackoffConfig.Jitter = opts.BackoffConfig.Jitter
		}

		if opts.BackoffConfig.MaxDelay != 0 {
			defaultBackoffConfig.MaxDelay = opts.BackoffConfig.MaxDelay
		}
	}

	return defaultBackoffConfig
}
