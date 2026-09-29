package http

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"io"
	stdlog "log"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/cshekharsharma/photon/core/logger"
	"github.com/cshekharsharma/photon/core/router"
	"github.com/cshekharsharma/photon/core/session"
	"github.com/cshekharsharma/photon/middleware"
	"github.com/cshekharsharma/photon/utils/rest"
	"github.com/cshekharsharma/photon/utils/rest/minifier"
	"github.com/cshekharsharma/photon/workers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type mockRouter struct {
	mock.Mock
	routes map[string]string
	router.Router
}

func (m *mockRouter) Use(mw ...func(http.Handler) http.Handler) {
	m.Called(mw)
}

func (m *mockRouter) HandleFunc(path string, handler http.HandlerFunc) {
	if m.routes == nil {
		m.routes = make(map[string]string)
	}
	m.routes[path] = "HandleFunc"
}

func (m *mockRouter) Get(path string, handler http.HandlerFunc) {
	if m.routes == nil {
		m.routes = make(map[string]string)
	}
	m.routes[path] = http.MethodGet
}

func (m *mockRouter) Post(path string, handler http.HandlerFunc) {
	if m.routes == nil {
		m.routes = make(map[string]string)
	}
	m.routes[path] = http.MethodPost
}

func (m *mockRouter) Put(path string, handler http.HandlerFunc) {
	if m.routes == nil {
		m.routes = make(map[string]string)
	}
	m.routes[path] = http.MethodPut
}

func (m *mockRouter) Delete(path string, handler http.HandlerFunc) {
	if m.routes == nil {
		m.routes = make(map[string]string)
	}
	m.routes[path] = http.MethodDelete
}

func (m *mockRouter) Options(path string, handler http.HandlerFunc) {
	if m.routes == nil {
		m.routes = make(map[string]string)
	}
	m.routes[path] = http.MethodOptions
}

func (m *mockRouter) Head(path string, handler http.HandlerFunc) {
	if m.routes == nil {
		m.routes = make(map[string]string)
	}
	m.routes[path] = http.MethodHead
}

type MockWorkerOverseer struct {
	mock.Mock
}

func (m *MockWorkerOverseer) Init(workers []workers.WorkerConfig) {
	m.Called(workers)
}

type mockLogger struct {
	fatalCalls atomic.Int64
	errorCalls atomic.Int64
}

func (m *mockLogger) With(fields map[string]interface{}) logger.Logger { return m }
func (m *mockLogger) Trace(message string, args ...interface{})        {}
func (m *mockLogger) Debug(message string, args ...interface{})        {}
func (m *mockLogger) Info(message string, args ...interface{})         {}
func (m *mockLogger) Warn(message string, args ...interface{})         {}
func (m *mockLogger) Error(message string, args ...interface{})        { m.errorCalls.Add(1) }
func (m *mockLogger) Fatal(message string, args ...interface{})        { m.fatalCalls.Add(1) }
func (m *mockLogger) Panic(message string, args ...interface{})        {}
func (m *mockLogger) Log(level logger.LogLevel, message string, args ...interface{}) {
}
func (m *mockLogger) TraceWithFields(fields map[string]interface{}, message string, args ...interface{}) {
}
func (m *mockLogger) DebugWithFields(fields map[string]interface{}, message string, args ...interface{}) {
}
func (m *mockLogger) InfoWithFields(fields map[string]interface{}, message string, args ...interface{}) {
}
func (m *mockLogger) WarnWithFields(fields map[string]interface{}, message string, args ...interface{}) {
}
func (m *mockLogger) ErrorWithFields(fields map[string]interface{}, message string, args ...interface{}) {
	m.errorCalls.Add(1)
}
func (m *mockLogger) FatalWithFields(fields map[string]interface{}, message string, args ...interface{}) {
	m.fatalCalls.Add(1)
}
func (m *mockLogger) PanicWithFields(fields map[string]interface{}, message string, args ...interface{}) {
}
func (m *mockLogger) LogWithFields(level logger.LogLevel, fields map[string]interface{}, message string, args ...interface{}) {
}

func TestStartHttpServer(t *testing.T) {
	minimumGoVersion = "go1.0"

	originalStartHttpServer := startHttpServerFn
	defer func() { startHttpServerFn = originalStartHttpServer }()

	var called bool
	startHttpServerFn = func(s *HTTPServer) {
		called = true
	}

	handler := func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}

	cfg := &ServerConfig{
		RouteProvider: router.RouterCHI,
		ErrorLogger:   logger.Init(&logger.LoggerConfig{Name: "httpserver-test", Provider: logger.LoggerProviderZerolog, Type: logger.LoggerTypeStdout}),
		AccessLogger:  logger.Init(&logger.LoggerConfig{Name: "httpserver-test", Provider: logger.LoggerProviderZerolog, Type: logger.LoggerTypeStdout}),
		HttpRoutes: []*HttpRoute{
			{
				RequestMethod: http.MethodGet,
				UrlRoute:      "/health",
				HttpHandler:   handler,
			},
		},
	}

	StartHttpServer(cfg)

	assert.True(t, called, "startHttpServer should have been called")
}

func TestStartHttpServer_AppliesSafeDefaults(t *testing.T) {
	origMin := minimumGoVersion
	minimumGoVersion = "go1.0"
	defer func() { minimumGoVersion = origMin }()

	origStart := startHttpServerFn
	defer func() { startHttpServerFn = origStart }()

	var captured *ServerConfig
	startHttpServerFn = func(s *HTTPServer) {
		captured = s.config
	}

	cfg := &ServerConfig{}
	StartHttpServer(cfg)

	require.NotNil(t, captured)
	assert.Equal(t, router.RouterCHI, captured.RouteProvider)
	assert.Equal(t, DefaultServerPort, captured.ServerPort)
	assert.Equal(t, DefaultReadTimeout, captured.ReadTimeout)
	assert.Equal(t, DefaultReadHeaderTimeout, captured.ReadHeaderTimeout)
	assert.Equal(t, DefaultWriteTimeout, captured.WriteTimeout)
	assert.Equal(t, DefaultIdleTimeout, captured.IdleTimeout)
	assert.Equal(t, DefaultShutdownTimeout, captured.ShutdownTimeout)
	assert.Equal(t, DefaultMaxHeaderBytes, captured.MaxHeaderBytes)
	assert.NotNil(t, captured.AccessLogger)
	assert.NotNil(t, captured.ErrorLogger)
	assert.NotNil(t, captured.ServerLogger)
}

func TestNewHTTPServerExposesLifecycleAndProductionKnobs(t *testing.T) {
	origMin := minimumGoVersion
	minimumGoVersion = "go1.0"
	defer func() { minimumGoVersion = origMin }()

	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
	errorLog := stdlog.New(io.Discard, "http: ", 0)
	baseContext := func(_ net.Listener) context.Context {
		return context.Background()
	}
	connContext := func(ctx context.Context, _ net.Conn) context.Context {
		return ctx
	}

	cfg := &ServerConfig{
		ServerPort:        8082,
		ReadTimeout:       3 * time.Second,
		ReadHeaderTimeout: 4 * time.Second,
		WriteTimeout:      5 * time.Second,
		IdleTimeout:       6 * time.Second,
		ShutdownTimeout:   7 * time.Second,
		MaxHeaderBytes:    2048,
		TLSConfig:         tlsConfig,
		BaseContext:       baseContext,
		ConnContext:       connContext,
		ErrorLog:          errorLog,
		HttpRoutes: []*HttpRoute{
			{
				RequestMethod: http.MethodGet,
				UrlRoute:      "/health",
				HttpHandler: func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusOK)
				},
			},
		},
	}

	server, err := NewHTTPServer(cfg)
	require.NoError(t, err)
	require.NotNil(t, server)

	stdServer := server.Server()
	require.NotNil(t, stdServer)
	assert.Equal(t, ":8082", server.Addr())
	assert.NotNil(t, server.Router())
	assert.Equal(t, 3*time.Second, stdServer.ReadTimeout)
	assert.Equal(t, 4*time.Second, stdServer.ReadHeaderTimeout)
	assert.Equal(t, 5*time.Second, stdServer.WriteTimeout)
	assert.Equal(t, 6*time.Second, stdServer.IdleTimeout)
	assert.Equal(t, 2048, stdServer.MaxHeaderBytes)
	assert.Same(t, tlsConfig, stdServer.TLSConfig)
	assert.Same(t, errorLog, stdServer.ErrorLog)
	assert.NotNil(t, stdServer.BaseContext)
	assert.NotNil(t, stdServer.ConnContext)
}

func TestHTTPServerListenAndServeUsesTLSWhenConfigured(t *testing.T) {
	origListen := listenAndServeFn
	origListenTLS := listenAndServeTLSFn
	defer func() {
		listenAndServeFn = origListen
		listenAndServeTLSFn = origListenTLS
	}()

	listenAndServeFn = func(s *http.Server) error {
		return errors.New("plain listen should not be used")
	}

	var gotCertFile string
	var gotKeyFile string
	listenAndServeTLSFn = func(s *http.Server, certFile, keyFile string) error {
		gotCertFile = certFile
		gotKeyFile = keyFile
		return nil
	}

	server := &HTTPServer{
		server: &http.Server{},
		config: &ServerConfig{
			TLSCertFile: "server.crt",
			TLSKeyFile:  "server.key",
		},
	}

	require.NoError(t, server.ListenAndServe())
	assert.Equal(t, "server.crt", gotCertFile)
	assert.Equal(t, "server.key", gotKeyFile)
}

func TestDefaultListenAndServeTLS_ReturnsErrorOnInvalidAddr(t *testing.T) {
	srv := &http.Server{
		Addr:    "invalid-address",
		Handler: http.NewServeMux(),
	}

	err := defaultListenAndServeTLS(srv, "missing.crt", "missing.key")
	assert.Error(t, err)
}

func TestNewHTTPServer_EnvironmentAndRouterErrors(t *testing.T) {
	t.Run("EnvironmentError", func(t *testing.T) {
		origMin := minimumGoVersion
		minimumGoVersion = "go99.99"
		defer func() { minimumGoVersion = origMin }()

		server, err := NewHTTPServer(&ServerConfig{})
		assert.Nil(t, server)
		assert.Error(t, err)
	})

	t.Run("RouterNil", func(t *testing.T) {
		origMin := minimumGoVersion
		origRouter := newRouterFn
		minimumGoVersion = "go1.0"
		newRouterFn = func(string) router.Router { return nil }
		defer func() {
			minimumGoVersion = origMin
			newRouterFn = origRouter
		}()

		server, err := NewHTTPServer(&ServerConfig{RouteProvider: router.RouterCHI})
		assert.Nil(t, server)
		assert.ErrorContains(t, err, "unsupported route provider")
	})
}

func TestHTTPServerLifecycle_NilAndPlainBranches(t *testing.T) {
	var server *HTTPServer
	var nilCtx context.Context
	assert.ErrorContains(t, server.Start(), "not initialized")
	assert.ErrorContains(t, server.ListenAndServe(), "not initialized")
	assert.ErrorContains(t, server.Shutdown(nilCtx), "not initialized")
	assert.Equal(t, "", server.Addr())
	assert.Nil(t, server.Router())
	assert.Nil(t, server.Server())
	assert.Nil(t, server.SessionManager())

	origListen := listenAndServeFn
	defer func() { listenAndServeFn = origListen }()
	listenErr := errors.New("listen plain")
	listenAndServeFn = func(*http.Server) error {
		return listenErr
	}

	startServer := &HTTPServer{
		server: &http.Server{},
		config: &ServerConfig{
			ServerLogger: &mockLogger{},
		},
		sessionManager: &session.Manager{},
	}
	assert.ErrorIs(t, startServer.Start(), listenErr)
	assert.Same(t, startServer.sessionManager, startServer.SessionManager())
	assert.NoError(t, closeSessionManagerFn(&session.Manager{}))
}

func TestHTTPServerShutdown_NilContextAndOwnedSession(t *testing.T) {
	origShutdown := shutdownFn
	origCloseSession := closeSessionManagerFn
	defer func() {
		shutdownFn = origShutdown
		closeSessionManagerFn = origCloseSession
	}()

	shutdownErr := errors.New("shutdown failed")
	sessionErr := errors.New("session close failed")
	var closeCalls atomic.Int32

	shutdownFn = func(*http.Server, context.Context) error {
		return shutdownErr
	}
	closeSessionManagerFn = func(*session.Manager) error {
		closeCalls.Add(1)
		return sessionErr
	}

	server := &HTTPServer{
		server:             &http.Server{},
		config:             &ServerConfig{},
		sessionManager:     &session.Manager{},
		ownsSessionManager: true,
	}

	var nilCtx context.Context
	err := server.Shutdown(nilCtx)
	assert.ErrorIs(t, err, shutdownErr)
	assert.ErrorIs(t, err, sessionErr)

	err = server.closeOwnedSession()
	assert.ErrorIs(t, err, sessionErr)
	assert.Equal(t, int32(1), closeCalls.Load())
}

func TestGoVersionLess_FallbackStringCompare(t *testing.T) {
	assert.True(t, goVersionLess("devel-a", "devel-b"))
}

func TestStartHttpServer_InvalidConfigPanics(t *testing.T) {
	origMin := minimumGoVersion
	minimumGoVersion = "go1.0"
	defer func() { minimumGoVersion = origMin }()

	cfg := &ServerConfig{
		RouteProvider: router.RouterCHI,
		ServerPort:    -1,
	}

	assert.PanicsWithValue(t, "invalid HTTP server configuration: server port must be between 1 and 65535, got -1", func() {
		StartHttpServer(cfg)
	})
}

func TestServerConfigNormalizeRejectsNilConfig(t *testing.T) {
	var cfg *ServerConfig

	assert.EqualError(t, cfg.Normalize(), "server config cannot be nil")
}

func TestServerConfigValidateRejectsNilConfig(t *testing.T) {
	var cfg *ServerConfig

	assert.EqualError(t, cfg.Validate(), "server config cannot be nil")
}

func TestServerConfigValidateRejectsInvalidFields(t *testing.T) {
	validHandler := func(w http.ResponseWriter, r *http.Request) {}

	tests := []struct {
		name    string
		mutate  func(*ServerConfig)
		wantErr string
	}{
		{
			name: "unsupported route provider",
			mutate: func(cfg *ServerConfig) {
				cfg.RouteProvider = "unknown"
			},
			wantErr: `unsupported route provider "unknown"`,
		},
		{
			name: "negative read timeout",
			mutate: func(cfg *ServerConfig) {
				cfg.ReadTimeout = -time.Second
			},
			wantErr: "read timeout cannot be negative",
		},
		{
			name: "negative write timeout",
			mutate: func(cfg *ServerConfig) {
				cfg.WriteTimeout = -time.Second
			},
			wantErr: "write timeout cannot be negative",
		},
		{
			name: "negative read header timeout",
			mutate: func(cfg *ServerConfig) {
				cfg.ReadHeaderTimeout = -time.Second
			},
			wantErr: "read header timeout cannot be negative",
		},
		{
			name: "negative idle timeout",
			mutate: func(cfg *ServerConfig) {
				cfg.IdleTimeout = -time.Second
			},
			wantErr: "idle timeout cannot be negative",
		},
		{
			name: "negative shutdown timeout",
			mutate: func(cfg *ServerConfig) {
				cfg.ShutdownTimeout = -time.Second
			},
			wantErr: "shutdown timeout cannot be negative",
		},
		{
			name: "negative max header bytes",
			mutate: func(cfg *ServerConfig) {
				cfg.MaxHeaderBytes = -1
			},
			wantErr: "max header bytes cannot be negative",
		},
		{
			name: "tls cert without key",
			mutate: func(cfg *ServerConfig) {
				cfg.TLSCertFile = "server.crt"
			},
			wantErr: "tls cert file and key file must be configured together",
		},
		{
			name: "session config with session manager",
			mutate: func(cfg *ServerConfig) {
				cfg.SessionConfig = &session.Config{}
				cfg.SessionManager = &session.Manager{}
			},
			wantErr: "session config and session manager cannot both be configured",
		},
		{
			name: "nil access logger",
			mutate: func(cfg *ServerConfig) {
				cfg.AccessLogger = nil
			},
			wantErr: "access logger cannot be nil",
		},
		{
			name: "nil error logger",
			mutate: func(cfg *ServerConfig) {
				cfg.ErrorLogger = nil
			},
			wantErr: "error logger cannot be nil",
		},
		{
			name: "nil server logger",
			mutate: func(cfg *ServerConfig) {
				cfg.ServerLogger = nil
			},
			wantErr: "server logger cannot be nil",
		},
		{
			name: "nil route",
			mutate: func(cfg *ServerConfig) {
				cfg.HttpRoutes = []*HttpRoute{nil}
			},
			wantErr: "http route at index 0 cannot be nil",
		},
		{
			name: "missing route path",
			mutate: func(cfg *ServerConfig) {
				cfg.HttpRoutes = []*HttpRoute{{HttpHandler: validHandler}}
			},
			wantErr: "http route at index 0 must have a url route",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &ServerConfig{
				RouteProvider:     router.RouterCHI,
				ServerPort:        8080,
				ReadTimeout:       time.Second,
				ReadHeaderTimeout: time.Second,
				WriteTimeout:      time.Second,
				IdleTimeout:       time.Second,
				ShutdownTimeout:   time.Second,
				MaxHeaderBytes:    1024,
				AccessLogger:      &mockLogger{},
				ErrorLogger:       &mockLogger{},
				ServerLogger:      &mockLogger{},
			}
			tt.mutate(cfg)

			assert.EqualError(t, cfg.Validate(), tt.wantErr)
		})
	}
}

func TestServerConfigValidateRejectsWildcardCorsWithCredentials(t *testing.T) {
	cfg := &ServerConfig{
		RouteProvider: router.RouterCHI,
		ServerPort:    8080,
		ReadTimeout:   time.Second,
		WriteTimeout:  time.Second,
		IdleTimeout:   time.Second,
		AccessLogger:  &mockLogger{},
		ErrorLogger:   &mockLogger{},
		ServerLogger:  &mockLogger{},
		CorsOptions: &CorsOptions{
			AllowedOrigins:   []string{"*"},
			AllowCredentials: true,
		},
	}

	assert.EqualError(t, cfg.Validate(), "cors cannot allow credentials with wildcard origin")
}

func TestServerConfigValidateRejectsBadRoute(t *testing.T) {
	cfg := &ServerConfig{
		RouteProvider: router.RouterCHI,
		ServerPort:    8080,
		ReadTimeout:   time.Second,
		WriteTimeout:  time.Second,
		IdleTimeout:   time.Second,
		AccessLogger:  &mockLogger{},
		ErrorLogger:   &mockLogger{},
		ServerLogger:  &mockLogger{},
		HttpRoutes: []*HttpRoute{
			{UrlRoute: "/missing-handler"},
		},
	}

	assert.EqualError(t, cfg.Validate(), `http route "/missing-handler" must have a handler`)
}

func TestStartHttpServer_HandleRoutesErrorPanics(t *testing.T) {
	origMin := minimumGoVersion
	minimumGoVersion = "go1.0"
	defer func() { minimumGoVersion = origMin }()

	origStart := startHttpServerFn
	startHttpServerFn = func(s *HTTPServer) {}
	defer func() { startHttpServerFn = origStart }()

	cfg := &ServerConfig{
		RouteProvider: router.RouterCHI,
		ErrorLogger: logger.Init(&logger.LoggerConfig{
			Name:     "err",
			Provider: logger.LoggerProviderZerolog,
			Type:     logger.LoggerTypeStdout,
		}),
		AccessLogger: logger.Init(&logger.LoggerConfig{
			Name:     "access",
			Provider: logger.LoggerProviderZerolog,
			Type:     logger.LoggerTypeStdout,
		}),
		SessionConfig: &session.Config{Store: session.StoreConfig{Type: session.StoreType("invalid")}},
	}

	assert.Panics(t, func() {
		StartHttpServer(cfg)
	})
}

func Test_validateEnvironment(t *testing.T) {
	t.Run("CurrentVersionOK", func(t *testing.T) {
		original := minimumGoVersion
		minimumGoVersion = "go1.0"
		defer func() { minimumGoVersion = original }()

		require.NotPanics(t, func() {
			validateEnvironment()
		})
	})

	t.Run("CurrentVersionTooLow", func(t *testing.T) {
		original := minimumGoVersion
		minimumGoVersion = "go99.99" // deliberately higher
		defer func() { minimumGoVersion = original }()

		require.PanicsWithValue(t,
			"Go runtime version is too old. Please update to at least go99.99",
			func() { validateEnvironment() },
		)
	})
}

func TestValidateEnvironmentErrorUsesSemanticGoVersions(t *testing.T) {
	assert.True(t, goVersionLess("go1.27.0", "go1.27.1"))
	assert.False(t, goVersionLess("go1.27.1", "go1.27.1"))
}

func TestStartHttpServer_GracefulShutdown(t *testing.T) {
	origSignalNotify := signalNotifyFn
	origListen := listenAndServeFn
	origShutdown := shutdownFn
	defer func() {
		signalNotifyFn = origSignalNotify
		listenAndServeFn = origListen
		shutdownFn = origShutdown
	}()

	listenDone := make(chan struct{})
	listenAndServeFn = func(s *http.Server) error {
		defer close(listenDone)
		return http.ErrServerClosed
	}
	signalNotifyFn = func(ch chan<- os.Signal, _ ...os.Signal) {
		ch <- syscall.SIGTERM
	}

	oldStderr := os.Stderr
	r, w, _ := os.Pipe()
	os.Stderr = w

	log := logger.Init(&logger.LoggerConfig{
		Name:     "test-logger",
		Provider: logger.LoggerProviderZerolog,
		Type:     logger.LoggerTypeStdout, // still writes to stderr internally
		Level:    logger.LogLevelDebug,
	})

	shutdownCalled := false
	config := &ServerConfig{
		ServerPort:   5055,
		ReadTimeout:  1 * time.Second,
		WriteTimeout: 1 * time.Second,
		IdleTimeout:  1 * time.Second,
		ServerLogger: log,
		ShutdownHook: func() {
			shutdownCalled = true
		},
	}

	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		startHttpServer(newHTTPServer(&mockRouter{}, config, nil, false))
	}()

	<-serverDone
	<-listenDone

	_ = w.Close()
	os.Stderr = oldStderr

	var buf bytes.Buffer
	_, _ = buf.ReadFrom(r)
	output := buf.String()

	if !shutdownCalled {
		t.Error("Expected shutdown hook to be called")
	}
	if !strings.Contains(output, "Starting go server on port :5055") {
		t.Errorf("Expected start log not found. Got:\n%s", output)
	}
	if !strings.Contains(output, "Shutting down the server due to signal") {
		t.Errorf("Expected shutdown signal log not found. Got:\n%s", output)
	}
	if !strings.Contains(output, "Server gracefully stopped.") {
		t.Errorf("Expected final stop log not found. Got:\n%s", output)
	}
}

func TestStartHttpServer_NilServerPanicsThroughHook(t *testing.T) {
	origPanic := panicFn
	defer func() { panicFn = origPanic }()

	var got interface{}
	panicFn = func(v interface{}) {
		got = v
	}

	startHttpServer(nil)
	assert.Equal(t, "HTTP server is not initialized", got)
}

func TestStartHttpServer_ReturnsWhenListenEndsCleanly(t *testing.T) {
	origSignalNotify := signalNotifyFn
	origListen := listenAndServeFn
	defer func() {
		signalNotifyFn = origSignalNotify
		listenAndServeFn = origListen
	}()

	signalNotifyFn = func(ch chan<- os.Signal, _ ...os.Signal) {}
	listenAndServeFn = func(*http.Server) error {
		return nil
	}

	startHttpServer(newHTTPServer(router.NewRouter(router.RouterCHI), &ServerConfig{
		ServerPort:   8083,
		ServerLogger: &mockLogger{},
	}, nil, false))
}

func TestStartHttpServer_ListenErrorClosesOwnedSession(t *testing.T) {
	origSignalNotify := signalNotifyFn
	origListen := listenAndServeFn
	origPanic := panicFn
	origCloseSession := closeSessionManagerFn
	defer func() {
		signalNotifyFn = origSignalNotify
		listenAndServeFn = origListen
		panicFn = origPanic
		closeSessionManagerFn = origCloseSession
	}()

	signalNotifyFn = func(ch chan<- os.Signal, _ ...os.Signal) {}
	listenAndServeFn = func(*http.Server) error {
		return errors.New("listen failed")
	}
	panicFn = func(interface{}) {}
	closeSessionManagerFn = func(*session.Manager) error {
		return errors.New("close failed")
	}

	log := &mockLogger{}
	startHttpServer(newHTTPServer(router.NewRouter(router.RouterCHI), &ServerConfig{
		ServerPort:   8084,
		ServerLogger: log,
	}, &session.Manager{}, true))
	assert.GreaterOrEqual(t, log.errorCalls.Load(), int64(1))
}

func TestStartHttpServer_ShutdownErrorLogsError(t *testing.T) {
	origSignalNotify := signalNotifyFn
	origListen := listenAndServeFn
	origShutdown := shutdownFn
	defer func() {
		signalNotifyFn = origSignalNotify
		listenAndServeFn = origListen
		shutdownFn = origShutdown
	}()

	listenStarted := make(chan struct{})
	listenBlock := make(chan struct{})
	listenAndServeFn = func(*http.Server) error {
		close(listenStarted)
		<-listenBlock
		return http.ErrServerClosed
	}
	signalNotifyFn = func(ch chan<- os.Signal, _ ...os.Signal) {
		go func() {
			<-listenStarted
			ch <- syscall.SIGTERM
			close(listenBlock)
		}()
	}
	shutdownFn = func(*http.Server, context.Context) error {
		return errors.New("shutdown failed")
	}

	log := &mockLogger{}
	startHttpServer(newHTTPServer(router.NewRouter(router.RouterCHI), &ServerConfig{
		ServerPort:   8085,
		ServerLogger: log,
	}, nil, false))
	assert.GreaterOrEqual(t, log.errorCalls.Load(), int64(1))
}

func TestHandleMiddlewares(t *testing.T) {
	corsValues := []*CorsOptions{nil,
		{
			MaxAge:         600,
			AllowedOrigins: []string{"*"},
			AllowedMethods: []string{"GET", "POST"},
			AllowedHeaders: []string{rest.HeaderContentType},
		},
	}

	for _, cors := range corsValues {
		mockRouter := new(mockRouter)

		// Set up Use to accept any call many times
		mockRouter.On("Use", mock.Anything).Return().Maybe()

		serverConfig := &ServerConfig{
			CorsOptions:  cors,
			ErrorLogger:  logger.Init(&logger.LoggerConfig{Name: "httpserver-test", Provider: logger.LoggerProviderZerolog, Type: logger.LoggerTypeStdout}),
			AccessLogger: logger.Init(&logger.LoggerConfig{Name: "httpserver-test", Provider: logger.LoggerProviderZerolog, Type: logger.LoggerTypeStdout}),
			Middlewares:  []func(http.Handler) http.Handler{},
			HttpRoutes: []*HttpRoute{
				{UrlRoute: "/x", EnableMinify: true},
			},
		}

		result := handleMiddlewares(mockRouter, serverConfig)
		assert.Equal(t, mockRouter, result)

		mockRouter.AssertExpectations(t)
	}
}

func TestHandleMiddlewares_ApiMinifierRegisteredOnce(t *testing.T) {
	useCallCount := func(routes []*HttpRoute) int {
		mockRouter := new(mockRouter)
		mockRouter.On("Use", mock.Anything).Return().Maybe()

		handleMiddlewares(mockRouter, &ServerConfig{
			ErrorLogger:  logger.Init(&logger.LoggerConfig{Name: "httpserver-test", Provider: logger.LoggerProviderZerolog, Type: logger.LoggerTypeStdout}),
			AccessLogger: logger.Init(&logger.LoggerConfig{Name: "httpserver-test", Provider: logger.LoggerProviderZerolog, Type: logger.LoggerTypeStdout}),
			HttpRoutes:   routes,
		})

		return len(mockRouter.Calls)
	}

	oneRouteCalls := useCallCount([]*HttpRoute{
		{UrlRoute: "/one", EnableMinify: true},
	})
	manyRouteCalls := useCallCount([]*HttpRoute{
		{UrlRoute: "/one", EnableMinify: true},
		{UrlRoute: "/two", EnableMinify: false},
		{UrlRoute: "/three", EnableMinify: true},
	})

	assert.Equal(t, oneRouteCalls, manyRouteCalls)
}

func TestHandleMiddlewares_CustomMiddlewares(t *testing.T) {
	mockRouter := new(mockRouter)
	mockRouter.On("Use", mock.Anything).Return().Maybe()

	custom := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			next.ServeHTTP(w, r)
		})
	}

	serverConfig := &ServerConfig{
		ErrorLogger:  logger.Init(&logger.LoggerConfig{Name: "httpserver-test", Provider: logger.LoggerProviderZerolog, Type: logger.LoggerTypeStdout}),
		AccessLogger: logger.Init(&logger.LoggerConfig{Name: "httpserver-test", Provider: logger.LoggerProviderZerolog, Type: logger.LoggerTypeStdout}),
		Middlewares:  []func(http.Handler) http.Handler{custom},
		HttpRoutes: []*HttpRoute{
			{UrlRoute: "/x", EnableMinify: true},
		},
	}

	handleMiddlewares(mockRouter, serverConfig)

	found := false
	for _, c := range mockRouter.Calls {
		if len(c.Arguments) == 1 {
			if mws, ok := c.Arguments.Get(0).([]func(http.Handler) http.Handler); ok && len(mws) == 1 {
				if reflect.ValueOf(mws[0]).Pointer() == reflect.ValueOf(custom).Pointer() {
					found = true
					break
				}
			}
		}
	}

	assert.True(t, found, "expected custom middleware to be applied")
}

func Test_handleHttpRoutes_withSessionAndRoutes(t *testing.T) {
	mockRouter := &mockRouter{}
	testHandler := func(w http.ResponseWriter, r *http.Request) {}

	cfg := &ServerConfig{
		SessionConfig: nil,
		HttpRoutes: []*HttpRoute{
			{
				RequestMethod: http.MethodGet,
				UrlRoute:      "/get",
				HttpHandler:   testHandler,
			},
			{
				RequestMethod: http.MethodPost,
				UrlRoute:      "/post",
				HttpHandler:   testHandler,
			},
			{
				RequestMethod: http.MethodOptions,
				UrlRoute:      "/options",
				HttpHandler:   testHandler,
			},
			{
				RequestMethod: http.MethodPut,
				UrlRoute:      "/put",
				HttpHandler:   testHandler,
			},
			{
				RequestMethod: http.MethodDelete,
				UrlRoute:      "/delete",
				HttpHandler:   testHandler,
			},
			{
				RequestMethod: http.MethodHead,
				UrlRoute:      "/head",
				HttpHandler:   testHandler,
			},
			{
				RequestMethod: "INVALID",
				UrlRoute:      "/custom",
				HttpHandler:   testHandler,
			},
		},
	}

	r, sessionManager, ownsSessionManager, err := handleHttpRoutes(mockRouter, cfg)
	assert.NoError(t, err)
	assert.NotNil(t, r)
	assert.Nil(t, sessionManager)
	assert.False(t, ownsSessionManager)

	for _, rt := range []string{"/get", "/post", "/put", "/delete", "/options", "/head", "/custom"} {
		_, ok := mockRouter.routes[rt]
		assert.True(t, ok, "route %s should be registered", rt)
	}

	assert.Equal(t, http.MethodPut, mockRouter.routes["/put"])
	assert.Equal(t, http.MethodDelete, mockRouter.routes["/delete"])
}

func Test_handleHttpRoutes_withSessionMiddleware(t *testing.T) {
	mockRouter := &mockRouter{}
	mockRouter.On("Use", mock.Anything).Return().Once()

	cfg := &ServerConfig{
		SessionConfig: &session.Config{
			Store: session.StoreConfig{Type: session.StoreMemory},
		},
	}

	r, sessionManager, ownsSessionManager, err := handleHttpRoutes(mockRouter, cfg)
	assert.NoError(t, err)
	assert.NotNil(t, r)
	assert.NotNil(t, sessionManager)
	assert.True(t, ownsSessionManager)

	mockRouter.AssertCalled(t, "Use", mock.Anything)
}

func Test_handleHttpRoutes_withExplicitSessionManager(t *testing.T) {
	mockRouter := &mockRouter{}
	mockRouter.On("Use", mock.Anything).Return().Once()

	mgr, err := session.New(&session.Config{
		Store: session.StoreConfig{Type: session.StoreMemory},
	})
	require.NoError(t, err)

	cfg := &ServerConfig{
		SessionManager: mgr,
	}

	r, sessionManager, ownsSessionManager, err := handleHttpRoutes(mockRouter, cfg)
	assert.NoError(t, err)
	assert.NotNil(t, r)
	assert.Same(t, mgr, sessionManager)
	assert.False(t, ownsSessionManager)

	mockRouter.AssertCalled(t, "Use", mock.Anything)
}

func Test_handleHttpRoutes_withSessionError(t *testing.T) {
	mockRouter := &mockRouter{}

	cfg := &ServerConfig{
		SessionConfig: &session.Config{Store: session.StoreConfig{Type: session.StoreType("invalid")}},
	}

	r, sessionManager, ownsSessionManager, err := handleHttpRoutes(mockRouter, cfg)
	assert.Error(t, err)
	assert.Nil(t, r)
	assert.Nil(t, sessionManager)
	assert.False(t, ownsSessionManager)
}

func TestApplyMiddlewareAndRoute_NoMiddleware(t *testing.T) {
	called := false
	mockHandler := func(w http.ResponseWriter, r *http.Request) {
		called = true
	}

	mockMethod := func(path string, handler http.HandlerFunc) {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		w := httptest.NewRecorder()
		handler(w, req)
	}

	route := &HttpRoute{
		UrlRoute:    "/test",
		HttpHandler: mockHandler,
		Middlewares: nil,
	}

	applyMiddlewareAndRoute(mockMethod, route)

	assert.True(t, called, "Handler should be called directly without middleware")
}

func TestApplyMiddlewareAndRoute_WithMiddleware(t *testing.T) {
	called := false
	middlewareCalled := false

	mockHandler := func(w http.ResponseWriter, r *http.Request) {
		called = true
	}

	stdMiddleware := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			middlewareCalled = true
			next.ServeHTTP(w, r)
		})
	}

	mw := middleware.ToMiddlewareFn(stdMiddleware)

	mockMethod := func(path string, handler http.HandlerFunc) {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		w := httptest.NewRecorder()
		handler(w, req)
	}

	route := &HttpRoute{
		UrlRoute:    "/test-with-mw",
		HttpHandler: mockHandler,
		Middlewares: []middleware.MiddlewareFn{mw},
	}

	applyMiddlewareAndRoute(mockMethod, route)

	assert.True(t, middlewareCalled, "Middleware should be invoked")
	assert.True(t, called, "Final handler should also be called")
}

func TestSetupApiKeyMinifier_WithConfig(t *testing.T) {
	expectedKeyMap := sync.Map{}
	expectedKeyMap.Store("fullName", "fn")
	expectedKeyMap.Store("userId", "uid")

	serverConfig := &ServerConfig{
		ApiMinifyConfig: &ApiMinifyConfig{
			IsEnabled: true,
			KeyMap:    &expectedKeyMap,
		},
	}

	setupApiKeyMinifier(serverConfig)
	assert.True(t, minifier.IsApiKeyMinifierEnabled())

	serverConfig.ApiMinifyConfig.IsEnabled = false
	setupApiKeyMinifier(serverConfig)
	assert.False(t, minifier.IsApiKeyMinifierEnabled())
}

func TestSetupApiKeyMinifier_NilConfig(t *testing.T) {
	serverConfig := &ServerConfig{
		ApiMinifyConfig: nil,
	}

	setupApiKeyMinifier(serverConfig)
	assert.False(t, minifier.IsApiKeyMinifierEnabled())
}

func TestRunBackgroundWorkers_NoWorkers(t *testing.T) {
	var called bool

	origFunc := startOverseerFunc
	startOverseerFunc = func(_ context.Context, workers []*workers.WorkerConfig, l logger.Logger, _ *workers.OverseerOptions) {
		called = true
	}
	defer func() {
		startOverseerFunc = origFunc
	}()

	cfg := &ServerConfig{
		BGWorkers: []*workers.WorkerConfig{},
		ServerLogger: logger.Init(&logger.LoggerConfig{
			Name:     "test",
			Provider: logger.LoggerProviderZerolog,
			Type:     logger.LoggerTypeStdout,
			Level:    logger.LogLevelDebug,
			BaseDir:  "/tmp",
		}),
	}

	runBackgroundWorkers(cfg)
	assert.False(t, called, "StartOverseer should not be called when BGWorkers is empty")
}

func TestRunBackgroundWorkers_WithWorkers(t *testing.T) {
	var called bool
	var passedWorkers []*workers.WorkerConfig
	var passedLogger logger.Logger
	origSleep := workers.GetOverseerSleepTimeout()
	defer workers.SetOverseerSleepTimeout(origSleep)

	origFunc := startOverseerFunc
	startOverseerFunc = func(_ context.Context, workers []*workers.WorkerConfig, l logger.Logger, _ *workers.OverseerOptions) {
		called = true
		passedWorkers = workers
		passedLogger = l
	}
	defer func() {
		startOverseerFunc = origFunc
	}()

	mockWorker := &workers.WorkerConfig{
		Name:      "Dummy",
		MaxCount:  1,
		IsEnabled: true,
		New: func() (workers.WorkerInterface, error) {
			return nil, nil
		},
	}

	cfg := &ServerConfig{
		BGWorkers: []*workers.WorkerConfig{mockWorker},
		ServerLogger: logger.Init(&logger.LoggerConfig{
			Name:     "test",
			Provider: logger.LoggerProviderZerolog,
			Type:     logger.LoggerTypeStdout,
			Level:    logger.LogLevelDebug,
			BaseDir:  "/tmp",
		}),
		WorkerSleepTimeout: 2 * time.Second, // should be capped to 5s
	}

	runBackgroundWorkers(cfg)

	assert.True(t, called, "StartOverseer should be called when BGWorkers are present")
	assert.Equal(t, 5*time.Second, cfg.WorkerSleepTimeout, "WorkerSleepTimeout should be capped to 5s")
	assert.Equal(t, cfg.WorkerSleepTimeout, workers.GetOverseerSleepTimeout(), "Overseer sleep timeout should be configured from server config")
	assert.Equal(t, cfg.BGWorkers, passedWorkers)
	assert.Equal(t, cfg.ServerLogger, passedLogger)
}

func TestStartHttpServer_ListenError(t *testing.T) {
	origSignalNotify := signalNotifyFn
	origListen := listenAndServeFn
	origPanic := panicFn
	defer func() {
		signalNotifyFn = origSignalNotify
		listenAndServeFn = origListen
		panicFn = origPanic
	}()

	signalNotifyFn = func(ch chan<- os.Signal, _ ...os.Signal) {}
	listenAndServeFn = func(s *http.Server) error { return errors.New("listen failed") }

	panicCh := make(chan struct{}, 1)
	panicFn = func(v interface{}) { panicCh <- struct{}{} }

	log := &mockLogger{}
	cfg := &ServerConfig{
		ServerPort:   8081,
		ReadTimeout:  time.Second,
		WriteTimeout: time.Second,
		IdleTimeout:  time.Second,
		ServerLogger: log,
	}

	startHttpServer(newHTTPServer(router.NewRouter(router.RouterCHI), cfg, nil, false))

	select {
	case <-panicCh:
		// ok
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("expected panic on listen error")
	}
	assert.GreaterOrEqual(t, log.errorCalls.Load(), int64(1), "expected error logs on errors")
}

func TestDefaultListenAndServe_ReturnsErrorOnInvalidAddr(t *testing.T) {
	srv := &http.Server{
		Addr:    "invalid-address",
		Handler: http.NewServeMux(),
	}

	err := defaultListenAndServe(srv)
	assert.Error(t, err)
}

func TestDefaultPanic_Panics(t *testing.T) {
	assert.Panics(t, func() {
		defaultPanic("boom")
	})
}
