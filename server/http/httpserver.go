// Package http provides functionalities to configure, run, and manage an HTTP server,
// including middleware support, API route handling, background workers, and graceful shutdown.
package http

import (
	"compress/flate"
	"context"
	"errors"
	"fmt"
	goversion "go/version"
	"log"
	"net/http"
	"os"
	"os/signal"
	"reflect"
	"runtime"
	"sync"
	"syscall"
	"time"

	"github.com/cshekharsharma/photon/middleware"
	"github.com/cshekharsharma/photon/utils/rest/minifier"
	"github.com/cshekharsharma/photon/workers"

	"github.com/cshekharsharma/photon/core/router"
	"github.com/cshekharsharma/photon/core/session"

	"github.com/rs/cors"
)

// HTTPServer is a configured Photon HTTP server with explicit lifecycle control.
type HTTPServer struct {
	server             *http.Server
	router             router.Router
	config             *ServerConfig
	sessionManager     *session.Manager
	ownsSessionManager bool
	sessionCloseOnce   sync.Once
	sessionCloseErr    error
}

var (
	startOverseerFunc = workers.StartOverseer
	minimumGoVersion  = "go1.27.1"

	startHttpServerFn = startHttpServer

	newRouterFn         = router.NewRouter
	listenAndServeFn    = defaultListenAndServe
	listenAndServeTLSFn = defaultListenAndServeTLS
	shutdownFn          = func(s *http.Server, ctx context.Context) error {
		return s.Shutdown(ctx)
	}
	closeSessionManagerFn = func(m *session.Manager) error {
		return m.Close()
	}
	signalNotifyFn = signal.Notify
	panicFn        = defaultPanic
)

func defaultListenAndServe(s *http.Server) error {
	return s.ListenAndServe()
}

func defaultListenAndServeTLS(s *http.Server, certFile, keyFile string) error {
	return s.ListenAndServeTLS(certFile, keyFile)
}

func defaultPanic(v interface{}) {
	panic(v)
}

// NewHTTPServer normalizes configuration, builds the router, applies middleware,
// registers routes, and returns a server handle that the caller can start or stop.
func NewHTTPServer(serverconfig *ServerConfig) (*HTTPServer, error) {
	if err := validateEnvironmentError(); err != nil {
		return nil, err
	}

	if err := serverconfig.Normalize(); err != nil {
		return nil, fmt.Errorf("invalid HTTP server configuration: %w", err)
	}

	httpRouter := newRouterFn(serverconfig.RouteProvider)
	if httpRouter == nil {
		return nil, fmt.Errorf("cannot configure HTTP router: unsupported route provider %q", serverconfig.RouteProvider)
	}

	httpRouter = handleMiddlewares(httpRouter, serverconfig)
	httpRouter, sessionManager, ownsSessionManager, err := handleHttpRoutes(httpRouter, serverconfig)
	if err != nil {
		return nil, fmt.Errorf("cannot configure HTTP routes: %w", err)
	}

	setupApiKeyMinifier(serverconfig)

	return newHTTPServer(httpRouter, serverconfig, sessionManager, ownsSessionManager), nil
}

func newHTTPServer(
	httpRouter router.Router,
	serverconfig *ServerConfig,
	sessionManager *session.Manager,
	ownsSessionManager bool,
) *HTTPServer {
	return &HTTPServer{
		router:             httpRouter,
		config:             serverconfig,
		sessionManager:     sessionManager,
		ownsSessionManager: ownsSessionManager,
		server: &http.Server{
			Addr:              fmt.Sprintf(":%d", serverconfig.ServerPort),
			Handler:           httpRouter,
			ReadTimeout:       serverconfig.ReadTimeout,
			ReadHeaderTimeout: serverconfig.ReadHeaderTimeout,
			WriteTimeout:      serverconfig.WriteTimeout,
			IdleTimeout:       serverconfig.IdleTimeout,
			MaxHeaderBytes:    serverconfig.MaxHeaderBytes,
			TLSConfig:         serverconfig.TLSConfig,
			BaseContext:       serverconfig.BaseContext,
			ConnContext:       serverconfig.ConnContext,
			ErrorLog:          serverconfig.ErrorLog,
		},
	}
}

// Start launches background workers configured on the server and then starts
// serving. It blocks until the HTTP listener returns.
func (s *HTTPServer) Start() error {
	if s == nil || s.server == nil || s.config == nil {
		return fmt.Errorf("HTTP server is not initialized")
	}

	runBackgroundWorkers(s.config)
	return s.ListenAndServe()
}

// ListenAndServe starts serving without launching Photon background workers.
// This is useful for tests and applications that manage workers independently.
func (s *HTTPServer) ListenAndServe() error {
	if s == nil || s.server == nil {
		return fmt.Errorf("HTTP server is not initialized")
	}
	if s.config != nil && s.config.TLSCertFile != "" {
		return listenAndServeTLSFn(s.server, s.config.TLSCertFile, s.config.TLSKeyFile)
	}

	return listenAndServeFn(s.server)
}

// Shutdown gracefully stops the underlying net/http server.
func (s *HTTPServer) Shutdown(ctx context.Context) error {
	if s == nil || s.server == nil {
		return fmt.Errorf("HTTP server is not initialized")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	shutdownErr := shutdownFn(s.server, ctx)
	sessionErr := s.closeOwnedSession()

	return errors.Join(shutdownErr, sessionErr)
}

func (s *HTTPServer) closeOwnedSession() error {
	if s == nil || !s.ownsSessionManager || s.sessionManager == nil {
		return nil
	}

	s.sessionCloseOnce.Do(func() {
		s.sessionCloseErr = closeSessionManagerFn(s.sessionManager)
	})

	return s.sessionCloseErr
}

// Addr returns the configured listen address.
func (s *HTTPServer) Addr() string {
	if s == nil || s.server == nil {
		return ""
	}

	return s.server.Addr
}

// Router returns the configured Photon router.
func (s *HTTPServer) Router() router.Router {
	if s == nil {
		return nil
	}

	return s.router
}

// Server returns the underlying net/http server for advanced integrations.
func (s *HTTPServer) Server() *http.Server {
	if s == nil {
		return nil
	}

	return s.server
}

// SessionManager returns the session manager attached to the HTTP middleware.
func (s *HTTPServer) SessionManager() *session.Manager {
	if s == nil {
		return nil
	}

	return s.sessionManager
}

// StartHttpServer initializes and starts the HTTP server.
//
// It performs the following steps:
// - Validates the Go runtime environment.
// - Sets up the router with middlewares and HTTP routes.
// - Configures API key minification, if enabled.
// - Launches background workers, if any.
// - Starts the HTTP server and handles graceful shutdown.
//
// Parameters:
//   - serverconfig: A pointer to ServerConfig containing server settings and route configurations.
func StartHttpServer(serverconfig *ServerConfig) {
	server, err := NewHTTPServer(serverconfig)
	if err != nil {
		panic(err.Error())
	}

	runBackgroundWorkers(server.config)
	startHttpServerFn(server)
}

// validateEnvironment ensures the Go runtime version meets the minimum requirements.
// If the runtime version is outdated, the program exits with an error message.
func validateEnvironment() {
	if err := validateEnvironmentError(); err != nil {
		log.Printf("Initialization Error: Go runtime version [%s] is too old. "+
			"Minimum %s required.\n", runtime.Version(), minimumGoVersion)

		panic("Go runtime version is too old. Please update to at least " + minimumGoVersion)
	}
}

func validateEnvironmentError() error {
	current := runtime.Version()
	if goVersionLess(current, minimumGoVersion) {
		return fmt.Errorf("go runtime version is too old; please update to at least %s", minimumGoVersion)
	}

	return nil
}

func goVersionLess(current, minimum string) bool {
	if goversion.IsValid(current) && goversion.IsValid(minimum) {
		return goversion.Compare(current, minimum) < 0
	}

	return current < minimum
}

// setupApiKeyMinifier configures the API key minification settings based on the server configuration.
//
// Parameters:
//   - serverconfig: A pointer to ServerConfig containing API key minifier settings.
func setupApiKeyMinifier(serverconfig *ServerConfig) {
	if serverconfig.ApiMinifyConfig != nil {
		isEnabled := serverconfig.ApiMinifyConfig.IsEnabled
		keymap := serverconfig.ApiMinifyConfig.KeyMap

		minifier.SetupApiKeyMinifierConfig(isEnabled, keymap)
	}
}

// handleMiddlewares applies a set of middlewares to the router.
//
// The middlewares include default ones provided by `Photon` for security, logging,
// and request handling, as well as custom middlewares specified in the server configuration.
//
// Parameters:
//   - router: The router instance to apply the middlewares to.
//   - serverconfig: A pointer to ServerConfig containing middleware settings.
//
// Returns:
//   - A router instance with all middlewares applied.
func handleMiddlewares(router router.Router, serverconfig *ServerConfig) router.Router {
	// Configure CORS options for security
	if serverconfig.CorsOptions == nil {
		router.Use(cors.Default().Handler)
	} else {
		router.Use(cors.New(cors.Options{
			MaxAge:           serverconfig.CorsOptions.MaxAge,
			AllowedOrigins:   serverconfig.CorsOptions.AllowedOrigins,
			AllowedMethods:   serverconfig.CorsOptions.AllowedMethods,
			AllowedHeaders:   serverconfig.CorsOptions.AllowedHeaders,
			AllowCredentials: serverconfig.CorsOptions.AllowCredentials,
		}).Handler)
	}

	// Apply default middlewares provided by `Photon`
	router.Use(middleware.RealIP)
	router.Use(middleware.CleanPath)
	router.Use(middleware.NewCompressor(flate.DefaultCompression).Handler)
	router.Use(middleware.GetHead)
	router.Use(middleware.Heartbeat("/heartbeat"))
	router.Use(middleware.RequestId)
	router.Use(middleware.Recoverer(serverconfig.ErrorLogger))
	router.Use(middleware.SecurityHeaders(middleware.SecurityHeadersOptions{}))
	router.Use(middleware.RequestLogger(serverconfig.AccessLogger))
	router.Use(middleware.RequestInit)

	// Apply API minification middleware
	minifierMwInput := make(map[string]bool, len(serverconfig.HttpRoutes))
	for _, httprouteValue := range serverconfig.HttpRoutes {
		minifierMwInput[httprouteValue.UrlRoute] = httprouteValue.EnableMinify
	}
	if len(minifierMwInput) > 0 {
		router.Use(middleware.ApiMinifier(minifierMwInput))
	}

	// Apply custom middlewares provided by the application
	if len(serverconfig.Middlewares) > 0 {
		for _, customMiddleware := range serverconfig.Middlewares {
			router.Use(customMiddleware)
		}
	}

	return router
}

// handleHttpRoutes configures the HTTP routes on the router based on the server configuration.
//
// If session configuration is enabled, it initializes the session manager.
//
// Parameters:
//   - router: The router instance to configure routes on.
//   - serverconfig: A pointer to ServerConfig containing HTTP route settings.
//
// Returns:
//   - A router instance with all routes configured.
//   - An error if session initialization fails.
func handleHttpRoutes(router router.Router, serverconfig *ServerConfig) (router.Router, *session.Manager, bool, error) {
	// Initialize session if configured
	sessionManager := serverconfig.SessionManager
	ownsSessionManager := false
	if sessionManager == nil && serverconfig.SessionConfig != nil {
		var err error
		sessionManager, err = session.New(serverconfig.SessionConfig)
		if err != nil {
			return nil, nil, false, err
		}
		ownsSessionManager = true
	}
	if sessionManager != nil {
		router.Use(sessionManager.Middleware())
	}

	// Configure HTTP routes
	if len(serverconfig.HttpRoutes) > 0 {
		for _, httproute := range serverconfig.HttpRoutes {
			switch httproute.RequestMethod {
			case http.MethodGet:
				applyMiddlewareAndRoute(router.Get, httproute)

			case http.MethodPost:
				applyMiddlewareAndRoute(router.Post, httproute)

			case http.MethodPut:
				applyMiddlewareAndRoute(router.Put, httproute)

			case http.MethodDelete:
				applyMiddlewareAndRoute(router.Delete, httproute)

			case http.MethodOptions:
				applyMiddlewareAndRoute(router.Options, httproute)

			case http.MethodHead:
				applyMiddlewareAndRoute(router.Head, httproute)

			default:
				router.HandleFunc(httproute.UrlRoute, httproute.HttpHandler)
			}
		}
	}

	return router, sessionManager, ownsSessionManager, nil
}

// applyMiddlewareAndRoute applies middlewares to an HTTP route and registers it with the router.
//
// Parameters:
//   - method: A function to register the route (e.g., router.Get or router.Post).
//   - httproute: A pointer to HttpRoute containing the route configuration.
func applyMiddlewareAndRoute(method func(string, http.HandlerFunc), httproute *HttpRoute) {
	if len(httproute.Middlewares) == 0 {
		method(httproute.UrlRoute, httproute.HttpHandler)
	} else {
		finalHandler := httproute.HttpHandler
		for _, middleware := range httproute.Middlewares {
			finalHandler = middleware(finalHandler)
		}
		method(httproute.UrlRoute, finalHandler)
	}
}

// startHttpServer starts the HTTP server and handles graceful shutdown.
//
// Parameters:
//   - router: The configured router instance.
//   - serverconfig: A pointer to ServerConfig containing server settings.
func startHttpServer(server *HTTPServer) {
	if server == nil || server.config == nil {
		panicFn("HTTP server is not initialized")
		return
	}

	serverconfig := server.config

	// Set up shutdown handling
	shutdownChan := make(chan os.Signal, 1)
	signalNotifyFn(shutdownChan,
		os.Interrupt,
		syscall.SIGTERM,
		syscall.SIGHUP,
		syscall.SIGQUIT,
	)
	defer signal.Stop(shutdownChan)

	errChan := make(chan error, 1)
	go func() {
		serverconfig.ServerLogger.Info("Starting go server on port :%d",
			serverconfig.ServerPort)

		err := server.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		errChan <- err
	}()

	var receivedSignal os.Signal
	select {
	case err := <-errChan:
		if err == nil {
			return
		}
		if sessionErr := server.closeOwnedSession(); sessionErr != nil {
			serverconfig.ServerLogger.Error("Error closing session manager: %v", sessionErr)
		}
		serverconfig.ServerLogger.Error("%v", err)
		panicFn(fmt.Sprintf("%v", err))
		return
	case receivedSignal = <-shutdownChan:
	}

	serverconfig.ServerLogger.Info("Shutting down the server due to signal: %s",
		receivedSignal.String())

	if serverconfig.ShutdownHook != nil {
		if reflect.TypeOf(serverconfig.ShutdownHook).Kind() == reflect.Func {
			serverconfig.ShutdownHook()
		}
	}

	shutdownTimeout := serverconfig.ShutdownTimeout
	if shutdownTimeout == 0 {
		shutdownTimeout = DefaultShutdownTimeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		serverconfig.ServerLogger.Error("Error shutting down server: %v", err)
	}

	serverconfig.ServerLogger.Info("Server gracefully stopped.")
}

// runBackgroundWorkers initializes and runs background workers as specified in the server configuration.
//
// Parameters:
//   - serverconfig: A pointer to ServerConfig containing background worker settings.
func runBackgroundWorkers(serverconfig *ServerConfig) {
	if len(serverconfig.BGWorkers) == 0 {
		return
	}

	if serverconfig.WorkerSleepTimeout < 5*time.Second {
		// Enforce a minimum sleep timeout to avoid rapid worker execution
		serverconfig.WorkerSleepTimeout = 5 * time.Second
	}
	workers.SetOverseerSleepTimeout(serverconfig.WorkerSleepTimeout)

	startOverseerFunc(context.Background(), serverconfig.BGWorkers, serverconfig.ServerLogger, nil)
}
