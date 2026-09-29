package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/cshekharsharma/photon/core/logger"
	"github.com/cshekharsharma/photon/server/grpc/server/interceptors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
)

const networkTCP = "tcp"

var grpcServeFn = func(server *grpc.Server, lis net.Listener) error {
	return server.Serve(lis)
}

type stoppableServer interface {
	GracefulStop()
	Stop()
}

// StartGRPCServer starts a gRPC server using the provided context and ServerOptions. It supports TLS or
// insecure mode, health check service, reflection for debugging, chained interceptors,
// and graceful shutdown when ctx is canceled. If RegisterFunc is provided, it is used
// to register the application-specific services. Any missing options are populated with defaults.
func StartGRPCServer(ctx context.Context, opts *ServerOptions) error {
	if ctx == nil {
		ctx = context.Background()
	}
	opts = DefaultServerOptions(opts)

	if opts.TLSConfig == nil && !opts.Insecure {
		return errors.New("gRPC server requires TLSConfig unless Insecure is true")
	}
	if opts.EnableReflection &&
		strings.EqualFold(strings.TrimSpace(opts.Environment), ProductionEnvironment) &&
		!opts.AllowReflectionInProduction {
		return errors.New("gRPC reflection requires AllowReflectionInProduction in production")
	}

	listenAddr := fmt.Sprintf(":%d", opts.Port)
	lis, err := net.Listen(networkTCP, listenAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", listenAddr, err)
	}

	var grpcOpts []grpc.ServerOption

	// set TLS or insecure credentials
	if opts.TLSConfig != nil {
		grpcOpts = append(grpcOpts, grpc.Creds(credentials.NewTLS(opts.TLSConfig)))
	} else if opts.Insecure {
		grpcOpts = append(grpcOpts, grpc.Creds(insecure.NewCredentials()))
	}

	// append required interceptors (both in-build & user-defined)
	unaryInterceptors := make([]grpc.UnaryServerInterceptor, 0)

	if len(opts.UnaryInterceptors) > 0 {
		unaryInterceptors = append(unaryInterceptors, opts.UnaryInterceptors...)
	}

	unaryInterceptors = append(unaryInterceptors,
		interceptors.RequestIDInterceptor(),
		interceptors.LoggingInterceptor(opts.ServerLogger),
		interceptors.RecoverInterceptor(opts.ServerLogger),
	)
	grpcOpts = append(grpcOpts, grpc.ChainUnaryInterceptor(unaryInterceptors...))

	streamInterceptors := make([]grpc.StreamServerInterceptor, 0, len(opts.StreamInterceptors)+3)
	streamInterceptors = append(streamInterceptors, opts.StreamInterceptors...)
	streamInterceptors = append(streamInterceptors,
		interceptors.StreamRequestIDInterceptor(),
		interceptors.StreamLoggingInterceptor(opts.ServerLogger),
		interceptors.StreamRecoverInterceptor(opts.ServerLogger),
	)
	grpcOpts = append(grpcOpts, grpc.ChainStreamInterceptor(streamInterceptors...))

	// Set message size limits
	grpcOpts = append(grpcOpts,
		grpc.MaxRecvMsgSize(opts.MaxRecvMsgSize),
		grpc.MaxSendMsgSize(opts.MaxSendMsgSize),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    opts.KeepaliveTime,
			Timeout: opts.KeepaliveTimeout,
		}),
	)

	grpcServer := grpc.NewServer(grpcOpts...)

	healthServer := health.NewServer()
	healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	grpc_health_v1.RegisterHealthServer(grpcServer, healthServer)

	if opts.RegisterFunc != nil {
		opts.RegisterFunc(grpcServer)
	}

	if opts.EnableReflection {
		reflection.Register(grpcServer)
	}

	errChan := make(chan error, 1)
	go func() {
		opts.ServerLogger.Info("Starting gRPC server on %s", listenAddr)
		errChan <- grpcServeFn(grpcServer, lis)
	}()

	select {
	case <-ctx.Done():
		opts.ServerLogger.Info("gRPC server shutdown requested")
		if opts.ShutdownHook != nil {
			opts.ShutdownHook()
		}
		stopGracefully(grpcServer, opts.ShutdownTimeout, opts.ServerLogger)
		return nil
	case err := <-errChan:
		if err == nil || errors.Is(err, grpc.ErrServerStopped) {
			return nil
		}
		return fmt.Errorf("gRPC server error: %w", err)
	}
}

// stopGracefully attempts a graceful shutdown of the provided gRPC server
// within the specified timeout duration. If the server fails to stop within
// the allotted time, it forcefully stops it. Shutdown events are logged using
// the provided logger. This function is intended to be called internally during
// the termination flow of StartGRPCServer.
func stopGracefully(s stoppableServer, timeout time.Duration, log logger.Logger) {
	done := make(chan struct{})
	go func() {
		s.GracefulStop()
		close(done)
	}()

	select {
	case <-done:
		log.Info("gRPC server shut down gracefully")
	case <-time.After(timeout):
		log.Warn("Graceful shutdown timed out, forcing stop")
		s.Stop()
	}
}
