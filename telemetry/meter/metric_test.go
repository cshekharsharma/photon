package meter

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cshekharsharma/photon/telemetry"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
)

// Initializes telemetry with a dummy OTLP endpoint (mockable)
func setupNoopTelemetry() {
	_, err := telemetry.InitTelemetry(context.Background(), &telemetry.Options{
		ServiceName:    "test-service",
		Environment:    telemetry.EnvDevelopment,
		SampleRate:     1.0,
		NoOpTracer:     true,
		NoOpMeter:      false,
		MeterExporter:  telemetry.MeterExporterOTLP,
		TracerExporter: telemetry.TracerExporterOTLP,
		MeterEndpoint: &telemetry.RemoteEndpoint{
			Host:     "localhost",
			Port:     "4317",
			Insecure: true,
		},
	})
	if err != nil {
		return
	}
}

func resetMaps() {
	counterMap = sync.Map{}
	histogramMap = sync.Map{}
	gaugeMap = sync.Map{}
	telemetryGet = telemetry.Get
}

type fakeFloat64Observer struct {
	metric.Float64Observer
	values []float64
}

func (f *fakeFloat64Observer) Observe(value float64, _ ...metric.ObserveOption) {
	f.values = append(f.values, value)
}

type fakeInt64Observer struct {
	metric.Int64Observer
	values []int64
}

func (f *fakeInt64Observer) Observe(value int64, _ ...metric.ObserveOption) {
	f.values = append(f.values, value)
}

type failingMeter struct {
	metric.Meter
	counterErr    error
	histogramErr  error
	floatGaugeErr error
	intGaugeErr   error
}

func (m failingMeter) Int64Counter(name string, options ...metric.Int64CounterOption) (metric.Int64Counter, error) {
	if m.counterErr != nil {
		return nil, m.counterErr
	}
	return m.Meter.Int64Counter(name, options...)
}

func (m failingMeter) Float64Histogram(name string, options ...metric.Float64HistogramOption) (metric.Float64Histogram, error) {
	if m.histogramErr != nil {
		return nil, m.histogramErr
	}
	return m.Meter.Float64Histogram(name, options...)
}

func (m failingMeter) Float64ObservableGauge(name string, options ...metric.Float64ObservableGaugeOption) (metric.Float64ObservableGauge, error) {
	if m.floatGaugeErr != nil {
		return nil, m.floatGaugeErr
	}
	return m.Meter.Float64ObservableGauge(name, options...)
}

func (m failingMeter) Int64ObservableGauge(name string, options ...metric.Int64ObservableGaugeOption) (metric.Int64ObservableGauge, error) {
	if m.intGaugeErr != nil {
		return nil, m.intGaugeErr
	}
	return m.Meter.Int64ObservableGauge(name, options...)
}

func useFailingMeter(t *testing.T, meter metric.Meter) {
	t.Helper()
	telemetryGet = func() *telemetry.Telemetry {
		return &telemetry.Telemetry{Meter: meter}
	}
	t.Cleanup(func() { telemetryGet = telemetry.Get })
}

func TestRecordCounter(t *testing.T) {
	setupNoopTelemetry()
	resetMaps()
	RecordCounter("test_counter", 1, map[string]interface{}{"label": "value"})
}

func TestAddBatchCounters(t *testing.T) {
	setupNoopTelemetry()
	resetMaps()
	counts := map[string]int64{
		"counter1": 1,
		"counter2": 2,
	}
	AddBatchCounters(counts, map[string]interface{}{"batch": "true"})
}

func TestIncrementErrorCounter(t *testing.T) {
	setupNoopTelemetry()
	resetMaps()
	err := errors.New("test error")
	IncrementErrorCounter("error_counter", err, map[string]interface{}{"operation": "create"})

	IncrementErrorCounter("error_counter", nil, map[string]interface{}{})
}

func TestRecordHistogram(t *testing.T) {
	setupNoopTelemetry()
	resetMaps()
	RecordHistogram("test_histogram", 12.5, map[string]interface{}{"unit": "ms"})
}

func TestRecordLatency(t *testing.T) {
	setupNoopTelemetry()
	resetMaps()
	start := time.Now().Add(-500 * time.Millisecond)
	RecordLatency("test_latency", start, map[string]interface{}{"api": "/test"})
}

func TestInitObservableGauge(t *testing.T) {
	setupNoopTelemetry()
	resetMaps()
	testFloat64CallbacksHook = func(cbs []metric.Float64Callback) {
		obs := &fakeFloat64Observer{}
		for _, cb := range cbs {
			_ = cb(context.Background(), obs)
		}
		if len(obs.values) == 0 {
			t.Fatalf("expected observe call")
		}
	}
	t.Cleanup(func() { testFloat64CallbacksHook = nil })

	err := InitObservableGauge("gauge_callback", func(ctx context.Context) float64 {
		return 42.0
	})
	assert.NoError(t, err)

	err = InitObservableGauge("", func(ctx context.Context) float64 { return 1 })
	assert.Error(t, err)
}

func TestSetGaugeValue_New(t *testing.T) {
	setupNoopTelemetry()
	resetMaps()
	testInt64CallbacksHook = func(cbs []metric.Int64Callback) {
		obs := &fakeInt64Observer{}
		for _, cb := range cbs {
			_ = cb(context.Background(), obs)
		}
		if len(obs.values) == 0 {
			t.Fatalf("expected observe call")
		}
	}
	t.Cleanup(func() { testInt64CallbacksHook = nil })

	err := SetGaugeValue("dynamic_gauge", 99, map[string]interface{}{"source": "manual"})
	assert.NoError(t, err)

	err = SetGaugeValue("", 1, map[string]interface{}{})
	assert.Error(t, err)
}

func TestSetGaugeValue_Update(t *testing.T) {
	setupNoopTelemetry()
	resetMaps()
	_ = SetGaugeValue("dynamic_gauge", 100, map[string]interface{}{"source": "manual"})
	err := SetGaugeValue("dynamic_gauge", 200, map[string]interface{}{"source": "manual"})
	assert.NoError(t, err)
}

func TestIncrementGauge(t *testing.T) {
	setupNoopTelemetry()
	resetMaps()
	name := "test_increment_gauge"
	err := IncrementGauge(name, map[string]interface{}{"label": "increment"})
	assert.NoError(t, err)

	val, ok := gaugeMap.Load(name)
	if !ok {
		t.Fatalf("Expected gauge to be initialized")
	}

	if val.(*atomic.Int64).Load() != 1 {
		t.Errorf("Expected gauge value to be 1, got %d", val.(*atomic.Int64).Load())
	}

	err = IncrementGauge(name, map[string]interface{}{"label": "increment"})
	assert.NoError(t, err)

	val, ok = gaugeMap.Load(name)
	if !ok {
		t.Fatalf("Expected gauge to be initialized")
	}

	if val.(*atomic.Int64).Load() != 2 {
		t.Errorf("Expected gauge value to be 2, got %d", val.(*atomic.Int64).Load())
	}
}

func TestDecrementGauge(t *testing.T) {
	setupNoopTelemetry()
	resetMaps()
	name := "test_decrement_gauge"
	_ = IncrementGauge(name, map[string]interface{}{"label": "decrement"})
	DecrementGauge(name)

	val, ok := gaugeMap.Load(name)
	if !ok {
		t.Fatalf("Expected gauge to exist")
	}

	if val.(*atomic.Int64).Load() != 0 {
		t.Errorf("Expected gauge value to be 0, got %d", val.(*atomic.Int64).Load())
	}
}

func TestRecordCounter_InvalidName(t *testing.T) {
	setupNoopTelemetry()
	resetMaps()
	RecordCounter("", 1, map[string]interface{}{})
}

func TestRecordCounter_InstrumentError(t *testing.T) {
	resetMaps()
	useFailingMeter(t, failingMeter{
		Meter:      metricnoop.NewMeterProvider().Meter("test"),
		counterErr: errors.New("counter failed"),
	})

	RecordCounter("counter", 1, map[string]interface{}{})
}

func TestRecordHistogram_InvalidName(t *testing.T) {
	setupNoopTelemetry()
	resetMaps()
	RecordHistogram("", 1.0, map[string]interface{}{})
}

func TestRecordHistogram_InstrumentError(t *testing.T) {
	resetMaps()
	useFailingMeter(t, failingMeter{
		Meter:        metricnoop.NewMeterProvider().Meter("test"),
		histogramErr: errors.New("histogram failed"),
	})

	RecordHistogram("histogram", 1.0, map[string]interface{}{})
}

func TestAddBatchCounters_InvalidName(t *testing.T) {
	setupNoopTelemetry()
	resetMaps()
	AddBatchCounters(map[string]int64{"": 1}, map[string]interface{}{})
}

func TestAddBatchCounters_InstrumentError(t *testing.T) {
	resetMaps()
	useFailingMeter(t, failingMeter{
		Meter:      metricnoop.NewMeterProvider().Meter("test"),
		counterErr: errors.New("counter failed"),
	})

	AddBatchCounters(map[string]int64{"counter": 1}, map[string]interface{}{})
}

func TestObservableGaugeInstrumentErrors(t *testing.T) {
	resetMaps()
	useFailingMeter(t, failingMeter{
		Meter:         metricnoop.NewMeterProvider().Meter("test"),
		floatGaugeErr: errors.New("float gauge failed"),
		intGaugeErr:   errors.New("int gauge failed"),
	})

	err := InitObservableGauge("float_gauge", func(context.Context) float64 { return 1 })
	assert.ErrorContains(t, err, "float gauge failed")

	err = SetGaugeValue("int_gauge", 1, map[string]interface{}{})
	assert.ErrorContains(t, err, "int gauge failed")
}

func TestDecrementGauge_Missing(t *testing.T) {
	setupNoopTelemetry()
	resetMaps()
	DecrementGauge("missing_gauge")
}
