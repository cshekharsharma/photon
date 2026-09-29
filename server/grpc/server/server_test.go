package server

import (
	"context"
	"errors"
	"net"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/cshekharsharma/photon/core/logger"
	"github.com/cshekharsharma/photon/utils/system"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
)

type fakeBlockingServer struct {
	stopped bool
}

func (f *fakeBlockingServer) GracefulStop() {
	time.Sleep(1 * time.Second) // simulate long delay
}

func (f *fakeBlockingServer) Stop() {
	f.stopped = true
}

func dummyUnaryInterceptor(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	return handler(ctx, req)
}

func dummyStreamInterceptor(
	srv interface{},
	ss grpc.ServerStream,
	info *grpc.StreamServerInfo,
	handler grpc.StreamHandler,
) error {
	return handler(srv, ss)
}

func getLogger(name string) logger.Logger {
	return logger.Init(&logger.LoggerConfig{
		Provider: logger.LoggerProviderZerolog,
		Name:     name,
		Type:     logger.LoggerTypeStdout,
	})
}

func TestStartGRPCServer_ShutdownSignal(t *testing.T) {
	port := 12345
	var shutdownCalled atomic.Bool

	tlsCert, tlsKey := generateSelfSignedTLS(t)
	tlsConfig, err := system.LoadTLSCredentials(tlsCert, tlsKey, tlsCert)
	assert.NoError(t, err)

	opts := &ServerOptions{
		Port:               port,
		TLSConfig:          tlsConfig,
		UnaryInterceptors:  []grpc.UnaryServerInterceptor{dummyUnaryInterceptor},
		StreamInterceptors: []grpc.StreamServerInterceptor{dummyStreamInterceptor},
		EnableReflection:   true,
		ShutdownTimeout:    500 * time.Millisecond,
		ShutdownHook: func() {
			shutdownCalled.Store(true)
		},
		ServerLogger: getLogger("TestStartGRPCServer_ShutdownSignal"),
		RegisterFunc: func(s *grpc.Server) {},
	}

	done := make(chan struct{})
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT)
	defer stop()
	go func() {
		defer close(done)
		_ = StartGRPCServer(ctx, opts)
	}()

	time.Sleep(200 * time.Millisecond) // ensure server starts

	_ = syscall.Kill(syscall.Getpid(), syscall.SIGINT)
	<-done
	stop()

	assert.True(t, shutdownCalled.Load(), "ShutdownHook should have been called")

	// non-TLS workflow
	shutdownCalled.Store(false)
	opts = &ServerOptions{
		Port:               port,
		Insecure:           true,
		UnaryInterceptors:  []grpc.UnaryServerInterceptor{dummyUnaryInterceptor},
		StreamInterceptors: []grpc.StreamServerInterceptor{dummyStreamInterceptor},
		EnableReflection:   true,
		ShutdownTimeout:    500 * time.Millisecond,
		ShutdownHook: func() {
			shutdownCalled.Store(true)
		},
		ServerLogger: getLogger("TestStartGRPCServer_ShutdownSignal_Insecure"),
		RegisterFunc: func(s *grpc.Server) {},
	}

	done = make(chan struct{})
	ctx, stop = signal.NotifyContext(context.Background(), syscall.SIGINT)
	defer stop()
	go func() {
		defer close(done)
		_ = StartGRPCServer(ctx, opts)
	}()

	time.Sleep(200 * time.Millisecond) // ensure server starts

	_ = syscall.Kill(syscall.Getpid(), syscall.SIGINT)
	<-done

	assert.True(t, shutdownCalled.Load(), "ShutdownHook should have been called")
}

func TestStartGRPCServer_BindError(t *testing.T) {
	l, err := net.Listen(networkTCP, ":12346")
	assert.NoError(t, err)
	defer func() {
		assert.NoError(t, l.Close())
	}()

	opts := &ServerOptions{
		Insecure:     true,
		Port:         12346,
		ServerLogger: getLogger("TestStartGRPCServer_BindError"),
	}

	err = StartGRPCServer(context.Background(), opts)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to listen")
}

func TestStartGRPCServer_RejectsAccidentalInsecureDefault(t *testing.T) {
	err := StartGRPCServer(context.Background(), &ServerOptions{
		Port:         12348,
		ServerLogger: getLogger("TestStartGRPCServer_RejectsAccidentalInsecureDefault"),
	})
	assert.EqualError(t, err, "gRPC server requires TLSConfig unless Insecure is true")
}

func TestStartGRPCServer_RejectsProductionReflection(t *testing.T) {
	err := StartGRPCServer(context.TODO(), &ServerOptions{
		Port:             12349,
		Insecure:         true,
		EnableReflection: true,
		Environment:      ProductionEnvironment,
		ServerLogger:     getLogger("TestStartGRPCServer_RejectsProductionReflection"),
	})
	assert.EqualError(t, err, "gRPC reflection requires AllowReflectionInProduction in production")
}

func TestStartGRPCServer_NilContext(t *testing.T) {
	origServe := grpcServeFn
	defer func() { grpcServeFn = origServe }()

	grpcServeFn = func(server *grpc.Server, lis net.Listener) error {
		assert.NoError(t, lis.Close())
		return errors.New("serve failed")
	}

	var nilCtx context.Context
	err := StartGRPCServer(nilCtx, &ServerOptions{
		Port:         0,
		Insecure:     true,
		ServerLogger: getLogger("TestStartGRPCServer_NilContext"),
	})
	assert.EqualError(t, err, "gRPC server error: serve failed")
}

func TestStartGRPCServer_AllowsProductionReflectionWithOptIn(t *testing.T) {
	origServe := grpcServeFn
	defer func() { grpcServeFn = origServe }()

	grpcServeFn = func(server *grpc.Server, lis net.Listener) error {
		assert.NoError(t, lis.Close())
		return errors.New("serve failed")
	}

	err := StartGRPCServer(context.Background(), &ServerOptions{
		Port:                        12350,
		Insecure:                    true,
		EnableReflection:            true,
		Environment:                 " production ",
		AllowReflectionInProduction: true,
		ServerLogger:                getLogger("TestStartGRPCServer_AllowsProductionReflectionWithOptIn"),
	})
	assert.EqualError(t, err, "gRPC server error: serve failed")
}

func TestStartGRPCServer_ContextShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var shutdownCalled atomic.Bool

	done := make(chan error, 1)
	go func() {
		done <- StartGRPCServer(ctx, &ServerOptions{
			Port:            12351,
			Insecure:        true,
			ShutdownTimeout: 500 * time.Millisecond,
			ShutdownHook: func() {
				shutdownCalled.Store(true)
			},
			ServerLogger: getLogger("TestStartGRPCServer_ContextShutdown"),
		})
	}()

	time.Sleep(200 * time.Millisecond)
	cancel()

	assert.NoError(t, <-done)
	assert.True(t, shutdownCalled.Load(), "ShutdownHook should have been called")
}

func TestStopGracefully_TimesOut(t *testing.T) {
	server := &fakeBlockingServer{}
	log := getLogger("TestStopGracefully_TimesOut")

	start := time.Now()
	stopGracefully(server, 50*time.Millisecond, log)
	duration := time.Since(start)

	assert.True(t, server.stopped, "Stop should have been called due to timeout")
	assert.Less(t, duration.Milliseconds(), int64(200), "Should not block longer than timeout")
}

func TestStartGRPCServer_ReturnsServeError(t *testing.T) {
	origServe := grpcServeFn
	defer func() { grpcServeFn = origServe }()

	grpcServeFn = func(server *grpc.Server, lis net.Listener) error {
		return errors.New("serve failed")
	}

	opts := &ServerOptions{
		Insecure:     true,
		Port:         12347,
		ServerLogger: getLogger("TestStartGRPCServer_ReturnsServeError"),
	}

	err := StartGRPCServer(context.Background(), opts)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "gRPC server error: serve failed")
}

func TestStartGRPCServer_IgnoresServerStopped(t *testing.T) {
	origServe := grpcServeFn
	defer func() { grpcServeFn = origServe }()

	grpcServeFn = func(server *grpc.Server, lis net.Listener) error {
		assert.NoError(t, lis.Close())
		return grpc.ErrServerStopped
	}

	err := StartGRPCServer(context.Background(), &ServerOptions{
		Insecure:     true,
		Port:         12352,
		ServerLogger: getLogger("TestStartGRPCServer_IgnoresServerStopped"),
	})
	assert.NoError(t, err)
}

func generateSelfSignedTLS(t *testing.T) (certPath, keyPath string) {
	t.Helper()

	certFile, err := os.CreateTemp("", "cert.pem")
	assert.NoError(t, err)
	_, err = certFile.Write([]byte(validCert))
	assert.NoError(t, err)
	_ = certFile.Close()

	keyFile, err := os.CreateTemp("", "key.pem")
	assert.NoError(t, err)
	_, err = keyFile.Write([]byte(validKey))
	assert.NoError(t, err)
	_ = keyFile.Close()

	return certFile.Name(), keyFile.Name()
}

const validCert = `-----BEGIN CERTIFICATE-----
MIIC9TCCAd2gAwIBAgIUDBHhwJIWjEGVebd7wKLbPDpBTsQwDQYJKoZIhvcNAQEL
BQAwFDESMBAGA1UEAwwJbG9jYWxob3N0MCAXDTI1MDUxNDA5NTQ1OFoYDzIxMjUw
NDIwMDk1NDU4WjAUMRIwEAYDVQQDDAlsb2NhbGhvc3QwggEiMA0GCSqGSIb3DQEB
AQUAA4IBDwAwggEKAoIBAQC2CFFvEkrYYF/73XVMM4ZNwJYSY27uoJRD+9mth5Ju
iQaCO01IVNWvdnAK/RCHUfGBIpDvaOYY/HMRT93GdG11Rq8KTTZQsgPCy/rcxrWZ
CEqOg+VZNbPFg3RjX995vYPzJwqsSDS20adw+BXOpg8n0kDSXVYr5jx4nRAT03Qy
g2FaYk5/MIjeUkN8GqcFyZupk2oGbKzMVA9PhrpTrZN89JgNQNrjQ3EXkh0cYySr
clT0oytbXFWDSPwIxdfedJ012Lj0mnbRVDx4gvZrwfdywnIrGzzXqvnRxnkHHgfi
M0czzmrkNSGP6LLTXpTlVBzZ9O8r/83iIKOWLxsHmNYPAgMBAAGjPTA7MBoGA1Ud
EQQTMBGCCWxvY2FsaG9zdIcEfwAAATAdBgNVHQ4EFgQUGpPq16irqmsD2UWM5f1Y
jQkgpu8wDQYJKoZIhvcNAQELBQADggEBAF59wJt4BSDq4RJMsu8GKNcU7lLHkgWr
9UGdvf9rt6I02Vz1e2BE9ceKhCxRCp3UHEb9DDgJHZ0ADwDNQioAUzY1x2Po91Nv
v+rsJ0JPIX0KrShv9HF8VonJvYlz3L9b5c1j5tutCVGl8Vf39EI3iMHpWpH6X3rr
XoqH5zjAolKInnlM2whSfTmJMjVddK2EG5iz4OfxKv6IL0hlnf2+v08FusDamCL9
Eoj5ZgMtqpEHUSDLZzxO1hvHct9KRU5aY+UhSOshu3GR6Fugf+gB7e3wq3ezKxQ0
pY1Yi5MFsEY5UsO4pI7bAj6vgKqrlsoYIiQ6qs264PRfHe3ZOJC/93Y=
-----END CERTIFICATE-----`

const validKey = `-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC2CFFvEkrYYF/7
3XVMM4ZNwJYSY27uoJRD+9mth5JuiQaCO01IVNWvdnAK/RCHUfGBIpDvaOYY/HMR
T93GdG11Rq8KTTZQsgPCy/rcxrWZCEqOg+VZNbPFg3RjX995vYPzJwqsSDS20adw
+BXOpg8n0kDSXVYr5jx4nRAT03Qyg2FaYk5/MIjeUkN8GqcFyZupk2oGbKzMVA9P
hrpTrZN89JgNQNrjQ3EXkh0cYySrclT0oytbXFWDSPwIxdfedJ012Lj0mnbRVDx4
gvZrwfdywnIrGzzXqvnRxnkHHgfiM0czzmrkNSGP6LLTXpTlVBzZ9O8r/83iIKOW
LxsHmNYPAgMBAAECggEACPo+YJQjin9aTIF+tvbgO/ZwZaYaGFJlbY2UDemz8EKk
G7PRWxdgAIrT+h7CpsHaVMw/yfw/mOIzye9JQ0UxCXR7zoNsrIHTC1OPDZNj7REK
cvPl/EaDQA2nJagv99Y/wIlfjovzGZnGax0OdPDIqjtFhM/N9P/3ty/GgAvQ3RV7
FaXAMKOkO8Fy6e9V1Xoj1FP/qNojm3wnwmk/zoaNedz98hZUV7nEFLiAPdbzQMRY
SjXQgSk7gzYaFxBeevGVyDyC7Nj7AZk5Rmy5XcqDxUYhnu8yaY2mC0EHURhP+8UJ
R0zrRO8XyX9Z6lLaWwSjzGUVrTHxVGUgmPnS54OtdQKBgQD/SKlz/5LTbLIeDmks
AYhg+KFyJxZshM7JmWGsFeQj9WRcUZJpGPZ1IGAAMEPc3lLmowrKMQUJZSEllcIE
/7uxiVDT+CFbcoKpjfYeBMLB6uyzSP+GrIlDRxzoFJRYQqdG5rSvS98xBTS/3Klb
5yPoQ8GHfjA5cTIZFYFiOVAw6wKBgQC2iwyLlKGxNu8tn0FR3zPissEMMzQNK3zv
wcK4UrHR0KGo/RGc/dcychm1PnPc4qHPJBxbEZIvUYYAGOx+o2t+gW8T4tTqbepI
RJ5Co874Fc1pQjksoznts55CxtH+L9rOIBIY6KjIU1QXyvdUl6wpJvfgriUGj3Ef
UYg6dkuGbQKBgAutkULjMB5H3KYPVrRSpaB5/zivnRD9yk/imls67SLP+PVYLfBs
2ellv76CdrhF21j9oGK7d1WEsM19WlDMOhPXCkGIGk6KoHuNKPMamKYyTv2smzPX
9LeFK0damaan9esCZsWWHPGrIUydlYnEuxnG77V5Ck+2Y+pN14tcv9RdAoGAatiI
x0qAOhJFfRayTRGwdQjcJh/yX6MMxelL6Ee+/Wh4t0kpfhK2WzieA5BCkQ+2VmB0
mHl4b2nwXS45fwZ4bNumAKXMqksbzqEbYTYwdtWMHgg9HvuLdK6l+8AUOgwYrn3n
Gd1UrazYk/ShQEpm4s+EV2aXFXfwZrx6WH3VRyECgYEAhjIzUY38xMAvuc82oJ/F
J1jY7sQWsn5l3n8PDCGNeFra8N+diJijHGABqQPkPegIevRxMCTSb/hY29rvLgNY
nqvW3VWm02eMSViV1cSvk1Qi0lgUs1fftNxk9ZhKpevsPvXT+JUQ0FxhHsKXB0GA
9h4z1uc2WgRXIrflm7USuho=
-----END PRIVATE KEY-----`
