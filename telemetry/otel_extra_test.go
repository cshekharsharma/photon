package telemetry

import (
	"context"
	"errors"
	"testing"

	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	otellog "go.opentelemetry.io/otel/log"
	lognoop "go.opentelemetry.io/otel/log/noop"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

type contextKey string

func TestInitTelemetry_ResourceCreationError(t *testing.T) {
	testMutex.Lock()
	defer testMutex.Unlock()

	orig := resourceNewHook
	defer func() { resourceNewHook = orig }()
	resourceNewHook = func(ctx context.Context, opts ...resource.Option) (*resource.Resource, error) {
		return nil, errors.New("resource creation failed")
	}

	_, err := InitTelemetry(context.Background(), &Options{
		ServiceName: "svc",
		Environment: EnvDevelopment,
		SampleRate:  DefaultSampleRate,
		NoOpTracer:  true,
		NoOpMeter:   true,
		NoOpLogger:  true,
	})
	if err == nil {
		t.Fatalf("expected resource creation error")
	}
}

func TestInitTelemetryUsesCallerContext(t *testing.T) {
	testMutex.Lock()
	defer testMutex.Unlock()

	expectedCtx := context.WithValue(context.Background(), contextKey("resource"), "ctx")
	orig := resourceNewHook
	defer func() { resourceNewHook = orig }()
	resourceNewHook = func(ctx context.Context, opts ...resource.Option) (*resource.Resource, error) {
		if ctx != expectedCtx {
			t.Fatalf("resource creation did not receive caller context")
		}
		return resource.Empty(), nil
	}

	telemetry = nil
	got, err := InitTelemetry(expectedCtx, &Options{
		ServiceName: "svc",
		Environment: EnvDevelopment,
		SampleRate:  DefaultSampleRate,
		NoOpTracer:  true,
		NoOpMeter:   true,
		NoOpLogger:  true,
	})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if got == nil {
		t.Fatalf("expected non-nil telemetry")
	}
	if telemetry != got {
		t.Fatalf("expected initialized telemetry to be stored globally")
	}
}

func TestInitLoggerProvider_ExporterError(t *testing.T) {
	orig := newOTLPLogExporterHook
	defer func() { newOTLPLogExporterHook = orig }()
	newOTLPLogExporterHook = func(ctx context.Context, options ...otlploggrpc.Option) (*otlploggrpc.Exporter, error) {
		return nil, errors.New("log exporter fail")
	}

	_, err := initLoggerProvider(context.Background(), &Options{
		LoggerExporter: LoggerExporterOTLP,
		LoggerEndpoint: &RemoteEndpoint{Host: "localhost", Port: "4319", Insecure: true},
	}, resource.Empty())
	if err == nil {
		t.Fatalf("expected logger exporter error")
	}
}

func TestInitMeterProvider_ExporterError(t *testing.T) {
	orig := newOTLPMeterExporterHook
	defer func() { newOTLPMeterExporterHook = orig }()
	newOTLPMeterExporterHook = func(ctx context.Context, options ...otlpmetricgrpc.Option) (*otlpmetricgrpc.Exporter, error) {
		return nil, errors.New("meter exporter fail")
	}

	_, err := initMeterProvider(context.Background(), &Options{
		MeterExporter: MeterExporterOTLP,
		MeterEndpoint: &RemoteEndpoint{Host: "localhost", Port: "4319", Insecure: true},
	}, resource.Empty())
	if err == nil {
		t.Fatalf("expected meter exporter error")
	}
}

func TestInitTracerProvider_ExporterError(t *testing.T) {
	orig := newOTLPTraceExporterHook
	defer func() { newOTLPTraceExporterHook = orig }()
	newOTLPTraceExporterHook = func(ctx context.Context, opts ...otlptracegrpc.Option) (*otlptrace.Exporter, error) {
		return nil, errors.New("trace exporter fail")
	}

	_, err := initTracerProvider(context.Background(), &Options{
		TracerExporter: TracerExporterOTLP,
		TracerEndpoint: &RemoteEndpoint{Host: "localhost", Port: "4317", Insecure: true},
		SampleRate:     DefaultSampleRate,
	}, resource.Empty())
	if err == nil {
		t.Fatalf("expected tracer exporter error")
	}
}

type fakeLifecycle struct {
	flushErr    error
	shutdownErr error
	flushed     bool
	shutdown    bool
}

func (f *fakeLifecycle) ForceFlush(context.Context) error {
	f.flushed = true
	return f.flushErr
}

func (f *fakeLifecycle) Shutdown(context.Context) error {
	f.shutdown = true
	return f.shutdownErr
}

type fakeTracerProvider struct {
	*fakeLifecycle
	trace.TracerProvider
}

type fakeMeterProvider struct {
	*fakeLifecycle
	metric.MeterProvider
}

type fakeLoggerProvider struct {
	*fakeLifecycle
	otellog.LoggerProvider
}

func TestTelemetryShutdownLifecycle(t *testing.T) {
	testMutex.Lock()
	defer testMutex.Unlock()

	tracerLifecycle := &fakeLifecycle{}
	meterLifecycle := &fakeLifecycle{}
	loggerLifecycle := &fakeLifecycle{}
	instance := &Telemetry{
		tracerProvider: fakeTracerProvider{fakeLifecycle: tracerLifecycle, TracerProvider: tracenoop.NewTracerProvider()},
		meterProvider:  fakeMeterProvider{fakeLifecycle: meterLifecycle, MeterProvider: metricnoop.NewMeterProvider()},
		loggerProvider: fakeLoggerProvider{fakeLifecycle: loggerLifecycle, LoggerProvider: lognoop.NewLoggerProvider()},
	}
	telemetry = instance

	if err := instance.Shutdown(context.Background()); err != nil {
		t.Fatalf("expected no shutdown error, got %v", err)
	}
	if !tracerLifecycle.flushed || !meterLifecycle.flushed || !loggerLifecycle.flushed {
		t.Fatalf("expected all providers to be flushed")
	}
	if !tracerLifecycle.shutdown || !meterLifecycle.shutdown || !loggerLifecycle.shutdown {
		t.Fatalf("expected all providers to be shutdown")
	}
	if telemetry != nil {
		t.Fatalf("expected global telemetry to be cleared")
	}

	if err := (*Telemetry)(nil).Shutdown(context.Background()); err != nil {
		t.Fatalf("nil shutdown should not fail: %v", err)
	}
}

func TestTelemetryShutdownReturnsLifecycleErrors(t *testing.T) {
	for _, tc := range []struct {
		name     string
		instance *Telemetry
		want     string
	}{
		{
			name: "logger flush",
			instance: &Telemetry{
				loggerProvider: fakeLoggerProvider{fakeLifecycle: &fakeLifecycle{flushErr: errors.New("logger flush")}, LoggerProvider: lognoop.NewLoggerProvider()},
			},
			want: "failed to flush logger provider: logger flush",
		},
		{
			name: "meter flush",
			instance: &Telemetry{
				meterProvider: fakeMeterProvider{fakeLifecycle: &fakeLifecycle{flushErr: errors.New("meter flush")}, MeterProvider: metricnoop.NewMeterProvider()},
			},
			want: "failed to flush meter provider: meter flush",
		},
		{
			name: "tracer flush",
			instance: &Telemetry{
				tracerProvider: fakeTracerProvider{fakeLifecycle: &fakeLifecycle{flushErr: errors.New("tracer flush")}, TracerProvider: tracenoop.NewTracerProvider()},
			},
			want: "failed to flush tracer provider: tracer flush",
		},
		{
			name: "logger shutdown",
			instance: &Telemetry{
				loggerProvider: fakeLoggerProvider{fakeLifecycle: &fakeLifecycle{shutdownErr: errors.New("logger shutdown")}, LoggerProvider: lognoop.NewLoggerProvider()},
			},
			want: "failed to shutdown logger provider: logger shutdown",
		},
		{
			name: "meter shutdown",
			instance: &Telemetry{
				meterProvider: fakeMeterProvider{fakeLifecycle: &fakeLifecycle{shutdownErr: errors.New("meter shutdown")}, MeterProvider: metricnoop.NewMeterProvider()},
			},
			want: "failed to shutdown meter provider: meter shutdown",
		},
		{
			name: "tracer shutdown",
			instance: &Telemetry{
				tracerProvider: fakeTracerProvider{fakeLifecycle: &fakeLifecycle{shutdownErr: errors.New("tracer shutdown")}, TracerProvider: tracenoop.NewTracerProvider()},
			},
			want: "failed to shutdown tracer provider: tracer shutdown",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.instance.Shutdown(context.Background())
			if err == nil || err.Error() != tc.want {
				t.Fatalf("expected %q, got %v", tc.want, err)
			}
		})
	}
}

func TestLifecycleHelpersIgnoreNilAndUnsupportedProviders(t *testing.T) {
	if err := forceFlush(context.Background(), nil); err != nil {
		t.Fatalf("nil force flush should not fail: %v", err)
	}
	if err := forceFlush(context.Background(), struct{}{}); err != nil {
		t.Fatalf("unsupported force flush should not fail: %v", err)
	}
	if err := shutdownProvider(context.Background(), nil); err != nil {
		t.Fatalf("nil shutdown should not fail: %v", err)
	}
	if err := shutdownProvider(context.Background(), struct{}{}); err != nil {
		t.Fatalf("unsupported shutdown should not fail: %v", err)
	}
}
