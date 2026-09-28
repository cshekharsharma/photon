package client

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"net"

	"github.com/cshekharsharma/photon/core/logger"
	"github.com/cshekharsharma/photon/utils/system"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/test/bufconn"
)

func getLogger(name string) logger.Logger {
	return logger.Init(&logger.LoggerConfig{
		Provider: logger.LoggerProviderZerolog,
		Name:     name,
		Type:     logger.LoggerTypeStdout,
	})
}

const testTarget = "passthrough:///bufnet"

func startMockGRPCServer(t *testing.T) (*bufconn.Listener, *grpc.Server) {
	lis := bufconn.Listen(1024 * 1024)

	s := grpc.NewServer()
	grpc_health_v1.RegisterHealthServer(s, health.NewServer())

	go func() {
		_ = s.Serve(lis)
	}()

	return lis, s
}

func startMockGRPCServerWithTLS(t *testing.T, certFile, keyFile string) (*bufconn.Listener, *grpc.Server) {
	t.Helper()
	creds, err := credentials.NewServerTLSFromFile(certFile, keyFile)
	assert.NoError(t, err)

	lis := bufconn.Listen(1024 * 1024)

	s := grpc.NewServer(grpc.Creds(creds))
	healthServer := health.NewServer()
	healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	grpc_health_v1.RegisterHealthServer(s, healthServer)

	go func() {
		_ = s.Serve(lis)
	}()

	return lis, s
}

func bufDialer(lis *bufconn.Listener) func(context.Context, string) (net.Conn, error) {
	return func(ctx context.Context, _ string) (net.Conn, error) {
		return lis.DialContext(ctx)
	}
}

func TestNewGRPCClient_WithInsecure(t *testing.T) {
	lis, server := startMockGRPCServer(t)
	defer func() {
		assert.NoError(t, lis.Close())
	}()
	defer server.Stop()

	opts := &ClientOptions{
		Target:            testTarget,
		Insecure:          true,
		ContextDialer:     bufDialer(lis),
		MinConnectTimeout: 300 * time.Millisecond,
		MaxRetries:        2,
		Logger:            getLogger("TestNewGRPCClient_WithInsecure"),
	}

	client, err := NewGRPCClient(opts)
	assert.NoError(t, err)
	assert.NotNil(t, client)

	err = client.HealthCheck(context.Background())
	assert.NoError(t, err)

	assert.NoError(t, client.Close())
}

func TestNewGRPCClient_WithoutTLSOrInsecure(t *testing.T) {
	opts := &ClientOptions{
		Target:            "localhost:50051",
		MinConnectTimeout: 300 * time.Millisecond,
		Logger:            getLogger("TestNewGRPCClient_WithoutTLSOrInsecure"),
	}

	client, err := NewGRPCClient(opts)
	assert.Nil(t, client)
	assert.EqualError(t, err, "either TLSConfig or Insecure must be set")
}

func TestNewGRPCClient_WithoutTarget(t *testing.T) {
	opts := &ClientOptions{
		Insecure:          true,
		MinConnectTimeout: 300 * time.Millisecond,
		Logger:            getLogger("TestNewGRPCClient_WithoutTarget"),
	}

	client, err := NewGRPCClient(opts)
	assert.Nil(t, client)
	assert.EqualError(t, err, "target address is required")
}

func TestNewGRPCClient_NilOptions(t *testing.T) {
	client, err := NewGRPCClient(nil)
	assert.Nil(t, client)
	assert.EqualError(t, err, "client options are required")
}

func TestNewGRPCClient_EnableLoadBalancing(t *testing.T) {
	lis, server := startMockGRPCServer(t)
	defer func() {
		assert.NoError(t, lis.Close())
	}()
	defer server.Stop()

	opts := &ClientOptions{
		Target:              testTarget,
		Insecure:            true,
		ContextDialer:       bufDialer(lis),
		MinConnectTimeout:   300 * time.Millisecond,
		MaxRetries:          2,
		EnableLoadBalancing: true,
		Logger:              getLogger("TestNewGRPCClient_EnableLoadBalancing"),
	}

	client, err := NewGRPCClient(opts)
	assert.NoError(t, err)
	assert.NotNil(t, client)
	assert.NoError(t, client.Close())
}

func TestNewGRPCClient_WithAdditionalDialOptions(t *testing.T) {
	lis, server := startMockGRPCServer(t)
	defer func() {
		assert.NoError(t, lis.Close())
	}()
	defer server.Stop()

	opts := &ClientOptions{
		Target:            testTarget,
		Insecure:          true,
		ContextDialer:     bufDialer(lis),
		MinConnectTimeout: 300 * time.Millisecond,
		MaxRetries:        2,
		Logger:            getLogger("TestNewGRPCClient_WithAdditionalDialOptions"),
		DialOptions:       []grpc.DialOption{grpc.WithUserAgent("photon-test-client")},
	}

	client, err := NewGRPCClient(opts)
	assert.NoError(t, err)
	assert.NotNil(t, client)
	assert.NoError(t, client.Close())
}

func TestNewGRPCClient_FailedToDial(t *testing.T) {
	opts := &ClientOptions{
		Target:            "nonexistenthost:9999", // unreachable
		Insecure:          true,
		MinConnectTimeout: 300 * time.Millisecond,
		Logger:            getLogger("TestNewGRPCClient_FailedToDial"),
		MaxRetries:        1,
	}

	client, err := NewGRPCClient(opts)
	assert.NoError(t, err)
	assert.NotNil(t, client)

	ctxWithDeadline, cancel := context.WithDeadline(context.Background(), time.Now().Add(1*time.Second))
	defer cancel()

	err = client.HealthCheck(ctxWithDeadline)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "health check failed")
}

func TestNewGRPCClient_DefaultMinConnectTimeout(t *testing.T) {
	SetDefaultMinConnectTimeout(250 * time.Millisecond)
	t.Cleanup(func() { SetDefaultMinConnectTimeout(DefaultMinConnectTimeout) })

	opts := &ClientOptions{
		Target:   "passthrough:///test",
		Insecure: true,
		Logger:   getLogger("TestNewGRPCClient_DefaultMinConnectTimeout"),
	}

	client, err := NewGRPCClient(opts)
	assert.NoError(t, err)
	assert.NotNil(t, client)
	assert.Equal(t, 250*time.Millisecond, opts.MinConnectTimeout)
	assert.Equal(t, 250*time.Millisecond, GetDefaultMinConnectTimeout())
	assert.NoError(t, client.Close())
}

func TestSetDefaultMinConnectTimeout_InvalidResetsDefault(t *testing.T) {
	SetDefaultMinConnectTimeout(0)
	t.Cleanup(func() { SetDefaultMinConnectTimeout(DefaultMinConnectTimeout) })

	assert.Equal(t, DefaultMinConnectTimeout, GetDefaultMinConnectTimeout())
}

func TestNewGRPCClient_NewClientError(t *testing.T) {
	orig := grpcNewClientFn
	defer func() { grpcNewClientFn = orig }()

	grpcNewClientFn = func(target string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
		return nil, errors.New("dial ctor failed")
	}

	opts := &ClientOptions{
		Target:            "passthrough:///test",
		Insecure:          true,
		MinConnectTimeout: 300 * time.Millisecond,
		Logger:            getLogger("TestNewGRPCClient_NewClientError"),
	}

	client, err := NewGRPCClient(opts)
	assert.Nil(t, client)
	assert.EqualError(t, err, "failed to dial gRPC: dial ctor failed")
}

func Test_constructBackoffConfig_Custom(t *testing.T) {
	cfg := &ClientOptions{
		BackoffConfig: &BackoffConfig{
			BaseDelay:  time.Second,
			Multiplier: 2,
			Jitter:     0.5,
			MaxDelay:   1 * time.Second,
		},
	}

	result := constructBackoffConfig(cfg)
	assert.Equal(t, time.Second, result.BaseDelay)
	assert.Equal(t, 2.0, result.Multiplier)
	assert.Equal(t, 0.5, result.Jitter)
	assert.Equal(t, 1*time.Second, result.MaxDelay)
}

func Test_constructBackoffConfig_Default(t *testing.T) {
	cfg := &ClientOptions{}
	result := constructBackoffConfig(cfg)
	assert.Equal(t, 100*time.Millisecond, result.BaseDelay)
	assert.Equal(t, 1.6, result.Multiplier)
	assert.Equal(t, 0.2, result.Jitter)
	assert.Equal(t, 5*time.Second, result.MaxDelay)
}

func TestNewGRPCClient_WithTLSConfig(t *testing.T) {
	certFile := writeTempFile(t, validCert)
	keyFile := writeTempFile(t, validKey)
	caFile := writeTempFile(t, validCert)
	defer func() {
		assert.NoError(t, os.Remove(certFile))
	}()
	defer func() {
		assert.NoError(t, os.Remove(keyFile))
	}()
	defer func() {
		assert.NoError(t, os.Remove(caFile))
	}()

	tlsCfg, err := system.LoadTLSCredentials(certFile, keyFile, caFile)
	assert.NoError(t, err)
	tlsCfg.ServerName = "localhost"

	lis, server := startMockGRPCServerWithTLS(t, certFile, keyFile)
	defer func() {
		assert.NoError(t, lis.Close())
	}()
	defer server.Stop()

	opts := &ClientOptions{
		Target:            testTarget,
		TLSConfig:         tlsCfg,
		ContextDialer:     bufDialer(lis),
		MinConnectTimeout: 1 * time.Second,
		Logger:            getLogger("TestNewGRPCClient_WithTLSConfig"),
	}

	client, err := NewGRPCClient(opts)
	assert.NoError(t, err)
	assert.NotNil(t, client)
	assert.NoError(t, client.HealthCheck(context.Background()))
	assert.NoError(t, client.Close())
}

func TestGRPCClient_Conn(t *testing.T) {
	lis, server := startMockGRPCServer(t)
	defer func() {
		assert.NoError(t, lis.Close())
	}()
	defer server.Stop()

	opts := &ClientOptions{
		Target:            testTarget,
		Insecure:          true,
		ContextDialer:     bufDialer(lis),
		MinConnectTimeout: 1 * time.Second,
		Logger:            getLogger("TestGRPCClient_Conn"),
	}

	clientIface, err := NewGRPCClient(opts)
	assert.NoError(t, err)
	client := clientIface.(*GRPCClient)
	assert.NotNil(t, client.Conn())
	assert.NoError(t, client.Close())
}

func TestHealthCheck_Error(t *testing.T) {
	opts := &ClientOptions{
		Target:            "localhost:65535",
		Insecure:          true,
		MinConnectTimeout: 200 * time.Millisecond,
		Logger:            getLogger("TestHealthCheck_Error"),
		MaxRetries:        1,
	}

	client, err := NewGRPCClient(opts)
	assert.NoError(t, err)
	assert.NotNil(t, client)

	ctxWithDeadline, cancel := context.WithDeadline(context.Background(), time.Now().Add(1*time.Second))
	defer cancel()

	err = client.HealthCheck(ctxWithDeadline)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "health check failed")
}

func TestHealthCheck_NotServing(t *testing.T) {
	lis := bufconn.Listen(1024 * 1024)
	defer func() {
		assert.NoError(t, lis.Close())
	}()

	s := grpc.NewServer()
	h := health.NewServer()
	h.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)
	grpc_health_v1.RegisterHealthServer(s, h)
	go func() { _ = s.Serve(lis) }()
	defer s.Stop()

	opts := &ClientOptions{
		Target:            testTarget,
		Insecure:          true,
		ContextDialer:     bufDialer(lis),
		MinConnectTimeout: 1 * time.Second,
		Logger:            getLogger("TestHealthCheck_NotServing"),
	}

	clientIface, err := NewGRPCClient(opts)
	assert.NoError(t, err)
	client := clientIface.(*GRPCClient)

	err = client.HealthCheck(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "service not healthy")
	assert.NoError(t, client.Close())
}

func TestGRPCClient_HealthCheckContext(t *testing.T) {
	client := &GRPCClient{opts: &ClientOptions{MinConnectTimeout: 25 * time.Millisecond}}

	ctx, cancel := client.healthCheckContext(context.Background())
	defer cancel()
	deadline, ok := ctx.Deadline()
	assert.True(t, ok)
	assert.LessOrEqual(t, time.Until(deadline), 25*time.Millisecond)

	parent, parentCancel := context.WithTimeout(context.Background(), time.Hour)
	defer parentCancel()
	parentDeadline, ok := parent.Deadline()
	assert.True(t, ok)

	ctx, cancel = client.healthCheckContext(parent)
	defer cancel()
	gotDeadline, ok := ctx.Deadline()
	assert.True(t, ok)
	assert.Equal(t, parentDeadline, gotDeadline)

	SetDefaultMinConnectTimeout(30 * time.Millisecond)
	t.Cleanup(func() { SetDefaultMinConnectTimeout(DefaultMinConnectTimeout) })

	ctx, cancel = (&GRPCClient{}).healthCheckContext(context.Background())
	defer cancel()
	deadline, ok = ctx.Deadline()
	assert.True(t, ok)
	assert.LessOrEqual(t, time.Until(deadline), 30*time.Millisecond)
}

func writeTempFile(t *testing.T, content string) string {
	t.Helper()
	tmpfile, err := os.CreateTemp("", "tls-test-*.pem")
	assert.NoError(t, err)

	_, err = tmpfile.Write([]byte(content))
	assert.NoError(t, err)
	assert.NoError(t, tmpfile.Close())

	return tmpfile.Name()
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
