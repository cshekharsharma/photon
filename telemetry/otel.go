package telemetry

import (
	"context"
	"fmt"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/log/global"

	lognoop "go.opentelemetry.io/otel/log/noop"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

var (
	telemetry       *Telemetry
	telemetryMu     sync.RWMutex
	resourceNewHook = resource.New
)

type Telemetry struct {
	Tracer trace.Tracer
	Meter  metric.Meter
	Logger log.Logger

	tracerProvider trace.TracerProvider
	meterProvider  metric.MeterProvider
	loggerProvider log.LoggerProvider
}

// InitTelemetry initializes OpenTelemetry with the provided context and options.
func InitTelemetry(ctx context.Context, options *Options) (*Telemetry, error) {
	err := validateOptions(options)
	if err != nil {
		return nil, err
	}

	resource, err := resourceNewHook(ctx,
		resource.WithAttributes(
			semconv.ServiceNameKey.String(options.ServiceName),
			semconv.DeploymentEnvironmentKey.String(string(options.Environment)),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	// setup TracerProvider.
	var tracerProvider trace.TracerProvider
	if options.NoOpTracer {
		tracerProvider = sdktrace.NewTracerProvider(
			sdktrace.WithSampler(sdktrace.NeverSample()),
			sdktrace.WithResource(resource),
		)
	} else {
		tracerProvider, err = initTracerProvider(ctx, options, resource)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize tracer provider: %w", err)
		}
	}

	otel.SetTracerProvider(tracerProvider)

	// Setup MeterProvider.
	var meterProvider metric.MeterProvider
	if options.NoOpMeter {
		meterProvider = metricnoop.NewMeterProvider()
	} else {
		meterProvider, err = initMeterProvider(ctx, options, resource)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize meter provider: %w", err)
		}
	}

	otel.SetMeterProvider(meterProvider)

	// Setup LoggerProvider.
	var loggerProvider log.LoggerProvider
	if options.NoOpLogger {
		loggerProvider = lognoop.NewLoggerProvider()
	} else {
		loggerProvider, err = initLoggerProvider(ctx, options, resource)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize logger provider: %w", err)
		}
	}

	global.SetLoggerProvider(loggerProvider)
	telemetryInstance := &Telemetry{
		Tracer:         otel.Tracer(options.ServiceName),
		Meter:          otel.Meter(options.ServiceName),
		Logger:         loggerProvider.Logger(options.ServiceName),
		tracerProvider: tracerProvider,
		meterProvider:  meterProvider,
		loggerProvider: loggerProvider,
	}

	telemetryMu.Lock()
	telemetry = telemetryInstance
	telemetryMu.Unlock()

	return telemetryInstance, nil
}

// validateOptions checks if the provided options are valid.
func validateOptions(options *Options) error {
	if options == nil {
		return fmt.Errorf("options are required")
	}
	if options.ServiceName == "" {
		return fmt.Errorf("service name is required")
	}

	if options.Environment == "" {
		return fmt.Errorf("environment is required")
	}

	if options.SampleRate < 0 || options.SampleRate > 1 {
		return fmt.Errorf("sample rate must be between 0 and 1")
	}

	return nil
}

// Shutdown flushes and shuts down telemetry providers that support those lifecycle hooks.
func (t *Telemetry) Shutdown(ctx context.Context) error {
	if t == nil {
		return nil
	}

	if err := forceFlush(ctx, t.loggerProvider); err != nil {
		return fmt.Errorf("failed to flush logger provider: %w", err)
	}
	if err := forceFlush(ctx, t.meterProvider); err != nil {
		return fmt.Errorf("failed to flush meter provider: %w", err)
	}
	if err := forceFlush(ctx, t.tracerProvider); err != nil {
		return fmt.Errorf("failed to flush tracer provider: %w", err)
	}

	if err := shutdownProvider(ctx, t.loggerProvider); err != nil {
		return fmt.Errorf("failed to shutdown logger provider: %w", err)
	}
	if err := shutdownProvider(ctx, t.meterProvider); err != nil {
		return fmt.Errorf("failed to shutdown meter provider: %w", err)
	}
	if err := shutdownProvider(ctx, t.tracerProvider); err != nil {
		return fmt.Errorf("failed to shutdown tracer provider: %w", err)
	}

	telemetryMu.Lock()
	if telemetry == t {
		telemetry = nil
	}
	telemetryMu.Unlock()

	return nil
}

// Get returns the initialized Telemetry instance or a no-op instance when telemetry has not been initialized.
func Get() *Telemetry {
	telemetryMu.RLock()
	current := telemetry
	telemetryMu.RUnlock()
	if current != nil {
		return current
	}

	return newNoOpTelemetry("photon-uninitialized")
}

func newNoOpTelemetry(serviceName string) *Telemetry {
	tracerProvider := tracenoop.NewTracerProvider()
	meterProvider := metricnoop.NewMeterProvider()
	loggerProvider := lognoop.NewLoggerProvider()

	return &Telemetry{
		Tracer:         tracerProvider.Tracer(serviceName),
		Meter:          meterProvider.Meter(serviceName),
		Logger:         loggerProvider.Logger(serviceName),
		tracerProvider: tracerProvider,
		meterProvider:  meterProvider,
		loggerProvider: loggerProvider,
	}
}

type forceFlusher interface {
	ForceFlush(context.Context) error
}

type shutdowner interface {
	Shutdown(context.Context) error
}

func forceFlush(ctx context.Context, provider any) error {
	if provider == nil {
		return nil
	}
	flusher, ok := provider.(forceFlusher)
	if !ok {
		return nil
	}
	return flusher.ForceFlush(ctx)
}

func shutdownProvider(ctx context.Context, provider any) error {
	if provider == nil {
		return nil
	}
	shutdown, ok := provider.(shutdowner)
	if !ok {
		return nil
	}
	return shutdown.Shutdown(ctx)
}
