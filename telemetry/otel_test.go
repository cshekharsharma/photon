package telemetry

import (
	"context"
	"net/http"
	"sync"
	"testing"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

var testMutex sync.Mutex

func TestInitTelemetry_DefaultOptions(t *testing.T) {
	testMutex.Lock()
	defer testMutex.Unlock()

	options := &Options{
		ServiceName:    "test-service27",
		Environment:    EnvDevelopment,
		SampleRate:     DefaultSampleRate,
		NoOpTracer:     false,
		NoOpMeter:      false,
		NoOpLogger:     false,
		TracerExporter: TracerExporterOTLP,
		MeterExporter:  MeterExporterOTLP,
		LoggerExporter: LoggerExporterOTLP,
		TracerEndpoint: &RemoteEndpoint{Host: "localhost", Port: "4317", Insecure: true},
		MeterEndpoint:  &RemoteEndpoint{Host: "localhost", Port: "4319", Insecure: true},
		LoggerEndpoint: &RemoteEndpoint{Host: "localhost", Port: "4319", Insecure: true},
	}

	telemetryInstance, err := InitTelemetry(context.Background(), options)
	if err != nil {
		t.Fatalf("InitTelemetry returned an error: %v", err)
	}

	if telemetryInstance.Tracer == nil {
		t.Error("Expected Tracer to be initialized, but got nil")
	}

	if telemetryInstance.Meter == nil {
		t.Error("Expected Meter to be initialized, but got nil")
	}

	if telemetryInstance.Logger == nil {
		t.Error("Expected Logger to be initialized, but got nil")
	}
}

func TestInitTelemetry_NoOpWithOLTP(t *testing.T) {
	testMutex.Lock()
	defer testMutex.Unlock()

	options := &Options{
		ServiceName:    "test-service73",
		Environment:    EnvDevelopment,
		SampleRate:     DefaultSampleRate,
		NoOpTracer:     true,
		NoOpMeter:      true,
		NoOpLogger:     true,
		TracerExporter: TracerExporterOTLP,
		MeterExporter:  MeterExporterOTLP,
		LoggerExporter: LoggerExporterOTLP,
	}

	telemetryInstance, err := InitTelemetry(context.Background(), options)
	if err != nil {
		t.Fatalf("InitTelemetry returned an error: %v", err)
	}

	if telemetryInstance.Tracer == nil {
		t.Error("Expected Tracer to be initialized, but got nil")
	}

	if telemetryInstance.Meter == nil {
		t.Error("Expected Meter to be initialized, but got nil")
	}

	if telemetryInstance.Logger == nil {
		t.Error("Expected Logger to be initialized, but got nil")
	}
}

func TestGet(t *testing.T) {
	testMutex.Lock()
	defer testMutex.Unlock()

	telemetry = nil

	for i := 0; i < 3; i++ {
		telemetryInstance := Get()
		if telemetryInstance == nil {
			t.Fatalf("Expected Get to return a Telemetry instance, but got nil")
		}

		if telemetryInstance.Tracer == nil {
			t.Error("Expected Tracer to be initialized, but got nil")
		}

		if telemetryInstance.Meter == nil {
			t.Error("Expected Meter to be initialized, but got nil")
		}

		if telemetryInstance.Logger == nil {
			t.Error("Expected Logger to be initialized, but got nil")
		}
	}

	if telemetry != nil {
		t.Fatal("Get should not initialize global telemetry")
	}

	want := newNoOpTelemetry("already-initialized")
	telemetry = want
	if got := Get(); got != want {
		t.Fatal("expected Get to return initialized telemetry")
	}
}

func TestInitTelemetry_UnsupportedMeterExporter(t *testing.T) {
	testMutex.Lock()
	defer testMutex.Unlock()

	options := &Options{
		ServiceName:    "test-service37",
		Environment:    EnvDevelopment,
		NoOpMeter:      false,
		MeterExporter:  MeterExporterType("invalidMeter"),
		TracerEndpoint: &RemoteEndpoint{Host: "localhost", Port: "4317", Insecure: true},
		MeterEndpoint:  &RemoteEndpoint{Host: "localhost", Port: "4319", Insecure: true},
		LoggerEndpoint: &RemoteEndpoint{Host: "localhost", Port: "4319", Insecure: true},
	}

	_, err := InitTelemetry(context.Background(), options)
	if err == nil {
		t.Fatal("Expected error due to unsupported meter exporter, but got nil")
	}
}

func TestInitTelemetry_InvalidResource(t *testing.T) {
	testMutex.Lock()
	defer testMutex.Unlock()

	_, err := InitTelemetry(context.Background(), nil)
	if err == nil {
		t.Error("Expected error due to nil options, but got nil")
	}

	_, err = InitTelemetry(context.Background(), &Options{})
	if err == nil {
		t.Error("Expected error due to missing service name, but got nil")
	}

	_, err = InitTelemetry(context.Background(), &Options{ServiceName: "svc"})
	if err == nil {
		t.Error("Expected error due to missing environment, but got nil")
	}

	_, err = InitTelemetry(context.Background(), &Options{ServiceName: "svc", Environment: EnvDevelopment, SampleRate: 1.1})
	if err == nil {
		t.Error("Expected error due to invalid sample rate, but got nil")
	}
}

func TestInitTelemetry_TracerExporterError(t *testing.T) {
	testMutex.Lock()
	defer testMutex.Unlock()

	// Provide invalid endpoint to cause exporter creation error
	options := &Options{
		ServiceName:    "test-service46",
		Environment:    EnvDevelopment,
		NoOpTracer:     false,
		TracerExporter: TracerExporterOTLP,
		TracerEndpoint: &RemoteEndpoint{Host: "", Port: "", Insecure: true}, // invalid endpoint
		MeterEndpoint:  &RemoteEndpoint{Host: "", Port: "", Insecure: true}, // invalid endpoint
		LoggerEndpoint: &RemoteEndpoint{Host: "", Port: "", Insecure: true}, // invalid endpoint
	}

	_, err := InitTelemetry(context.Background(), options)
	if err != nil {
		t.Logf("InitTelemetry returned expected error: %v", err)
	} else {
		t.Error("Expected error due to invalid tracer exporter configuration, but got nil")
	}
}

func TestInitTelemetry_MeterExporterError(t *testing.T) {
	testMutex.Lock()
	defer testMutex.Unlock()

	options := &Options{
		ServiceName:    "test-service57",
		Environment:    EnvDevelopment,
		NoOpMeter:      false,
		MeterExporter:  MeterExporterOTLP,
		TracerEndpoint: &RemoteEndpoint{Host: "localhost", Port: "4317", Insecure: true},
		MeterEndpoint:  &RemoteEndpoint{Host: "localhost", Port: "4319", Insecure: true},
		LoggerEndpoint: &RemoteEndpoint{Host: "localhost", Port: "4319", Insecure: true},
	}

	_, err := InitTelemetry(context.Background(), options)
	if err != nil {
		t.Logf("InitTelemetry returned expected error: %v", err)
	} else {
		t.Error("Expected error due to invalid meter exporter configuration, but got nil")
	}
}

func TestInitTelemetry_LoggerExporterError(t *testing.T) {
	testMutex.Lock()
	defer testMutex.Unlock()

	options := &Options{
		ServiceName:    "test-service57",
		Environment:    EnvDevelopment,
		NoOpMeter:      true,
		NoOpTracer:     true,
		TracerEndpoint: &RemoteEndpoint{Host: "localhost", Port: "4317", Insecure: true},
		MeterEndpoint:  &RemoteEndpoint{Host: "localhost", Port: "4319", Insecure: true},
		LoggerEndpoint: nil,
	}

	_, err := InitTelemetry(context.Background(), options)
	if err != nil {
		t.Logf("InitTelemetry returned expected error: %v", err)
	} else {
		t.Error("Expected error due to invalid logger exporter configuration, but got nil")
	}
}

func TestStartSpanAndEndSpan(t *testing.T) {
	testMutex.Lock()
	defer testMutex.Unlock()

	exporter := tracetest.NewInMemoryExporter()
	bsp := sdktrace.NewBatchSpanProcessor(exporter)

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithSpanProcessor(bsp),
	)

	otel.SetTracerProvider(tp)

	telemetry = &Telemetry{}
	telemetry.Tracer = otel.Tracer("test-service91")

	ctx := context.Background()
	ctx, span := telemetry.Tracer.Start(ctx, "test-span")
	if span == nil {
		t.Error("Expected span to be started, but got nil")
	}

	span.End()

	if err := tp.ForceFlush(ctx); err != nil {
		t.Fatalf("failed to flush tracer provider: %v", err)
	}

	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Errorf("Expected 1 span to be exported, but got %d", len(spans))
	} else {
		if spans[0].Name != "test-span" {
			t.Errorf("Expected span name to be 'test-span', but got '%s'", spans[0].Name)
		}
	}
}

func TestContextPropagation(t *testing.T) {
	testMutex.Lock()
	defer testMutex.Unlock()

	exporter := tracetest.NewInMemoryExporter()
	bsp := sdktrace.NewBatchSpanProcessor(exporter)

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithSpanProcessor(bsp),
	)

	otel.SetTracerProvider(tp)
	telemetry = &Telemetry{}
	telemetry.Tracer = otel.Tracer("test-service19")

	ctx := context.Background()
	ctx, span := telemetry.Tracer.Start(ctx, "parent-span")
	defer span.End()

	req, err := http.NewRequestWithContext(ctx, "GET", "http://example1.com", nil)
	if err != nil {
		t.Fatalf("Failed to create HTTP request: %v", err)
	}

	propagator := otel.GetTextMapPropagator()
	propagator.Inject(ctx, propagation.HeaderCarrier(req.Header))

	extractedCtx := propagator.Extract(ctx, propagation.HeaderCarrier(req.Header))
	extractedSpan := trace.SpanFromContext(extractedCtx)

	spanCtx := extractedSpan.SpanContext()
	if extractedSpan == nil || !spanCtx.IsValid() {
		t.Error("Failed to extract valid span from context")
	}
}

func TestGetUnaryServerInterceptor(t *testing.T) {
	interceptor := otelgrpc.NewServerHandler()
	if interceptor == nil {
		t.Error("Expected UnaryServerInterceptor to be initialized, but got nil")
	}
}

func TestGetStreamServerInterceptor(t *testing.T) {
	interceptor := otelgrpc.NewServerHandler()
	if interceptor == nil {
		t.Error("Expected StreamServerInterceptor to be initialized, but got nil")
	}
}

func TestInitTelemetry_UnsupportedTracerExporter(t *testing.T) {
	testMutex.Lock()
	defer testMutex.Unlock()

	options := &Options{
		ServiceName:    "test-service31",
		Environment:    EnvDevelopment,
		SampleRate:     DefaultSampleRate,
		NoOpTracer:     false,
		TracerExporter: TracerExporterType("invalidTracer"), // Unsupported exporter
		TracerEndpoint: &RemoteEndpoint{Host: "localhost", Port: "4317", Insecure: true},
		MeterEndpoint:  &RemoteEndpoint{Host: "localhost", Port: "4319", Insecure: true},
	}

	_, err := InitTelemetry(context.Background(), options)
	if err == nil {
		t.Fatal("Expected error due to unsupported tracer exporter, but got nil")
	}
}

func TestValidateOptions(t *testing.T) {
	tests := []struct {
		name    string
		options Options
		wantErr string
	}{
		{
			name:    "missing service name",
			options: Options{Environment: "dev", SampleRate: 0.5},
			wantErr: "service name is required",
		},
		{
			name:    "missing environment",
			options: Options{ServiceName: "my-app", SampleRate: 0.5},
			wantErr: "environment is required",
		},
		{
			name:    "sample rate < 0",
			options: Options{ServiceName: "my-app", Environment: "dev", SampleRate: -0.1},
			wantErr: "sample rate must be between 0 and 1",
		},
		{
			name:    "sample rate > 1",
			options: Options{ServiceName: "my-app", Environment: "dev", SampleRate: 1.1},
			wantErr: "sample rate must be between 0 and 1",
		},
		{
			name:    "valid config",
			options: Options{ServiceName: "my-app", Environment: "dev", SampleRate: 0.8},
			wantErr: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateOptions(&tt.options)
			if tt.wantErr == "" && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if tt.wantErr != "" {
				if err == nil {
					t.Errorf("expected error %q but got nil", tt.wantErr)
				} else if err.Error() != tt.wantErr {
					t.Errorf("expected error %q, got %q", tt.wantErr, err.Error())
				}
			}
		})
	}
}
