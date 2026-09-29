package workers

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cshekharsharma/photon/core/logger"
	"github.com/stretchr/testify/assert"
)

type testWorker struct {
	name          string
	id            string
	execErr       error
	runCount      *atomic.Int32
	signalRun     chan struct{}
	runErr        error
	panicRun      bool
	blockRun      chan struct{}
	beatUntilDone bool
}

type panicIDWorker struct{}

type valueLogger struct{}

func (v valueLogger) With(fields map[string]interface{}) logger.Logger { return v }
func (v valueLogger) Trace(message string, args ...interface{})        {}
func (v valueLogger) Debug(message string, args ...interface{})        {}
func (v valueLogger) Info(message string, args ...interface{})         {}
func (v valueLogger) Warn(message string, args ...interface{})         {}
func (v valueLogger) Error(message string, args ...interface{})        {}
func (v valueLogger) Fatal(message string, args ...interface{})        {}
func (v valueLogger) Panic(message string, args ...interface{})        {}
func (v valueLogger) Log(level logger.LogLevel, message string, args ...interface{}) {
}
func (v valueLogger) TraceWithFields(fields map[string]interface{}, message string, args ...interface{}) {
}
func (v valueLogger) DebugWithFields(fields map[string]interface{}, message string, args ...interface{}) {
}
func (v valueLogger) InfoWithFields(fields map[string]interface{}, message string, args ...interface{}) {
}
func (v valueLogger) WarnWithFields(fields map[string]interface{}, message string, args ...interface{}) {
}
func (v valueLogger) ErrorWithFields(fields map[string]interface{}, message string, args ...interface{}) {
}
func (v valueLogger) FatalWithFields(fields map[string]interface{}, message string, args ...interface{}) {
}
func (v valueLogger) PanicWithFields(fields map[string]interface{}, message string, args ...interface{}) {
}
func (v valueLogger) LogWithFields(level logger.LogLevel, fields map[string]interface{}, message string, args ...interface{}) {
}

type controlledPanicLogger struct {
	panicOnInfo  atomic.Bool
	panicOnWarn  atomic.Bool
	panicOnError atomic.Bool
	onWarn       func()
}

func (l *controlledPanicLogger) With(fields map[string]interface{}) logger.Logger { return l }
func (l *controlledPanicLogger) Trace(message string, args ...interface{})        {}
func (l *controlledPanicLogger) Debug(message string, args ...interface{})        {}
func (l *controlledPanicLogger) Info(message string, args ...interface{}) {
	if l.panicOnInfo.Load() {
		panic("controlled info panic")
	}
}
func (l *controlledPanicLogger) Warn(message string, args ...interface{}) {
	if l.onWarn != nil {
		l.onWarn()
	}
	if l.panicOnWarn.Load() {
		panic("controlled warn panic")
	}
}
func (l *controlledPanicLogger) Error(message string, args ...interface{}) {
	if l.panicOnError.Load() {
		panic("controlled error panic")
	}
}
func (l *controlledPanicLogger) Fatal(message string, args ...interface{}) {}
func (l *controlledPanicLogger) Panic(message string, args ...interface{}) {}
func (l *controlledPanicLogger) Log(level logger.LogLevel, message string, args ...interface{}) {
}
func (l *controlledPanicLogger) TraceWithFields(fields map[string]interface{}, message string, args ...interface{}) {
}
func (l *controlledPanicLogger) DebugWithFields(fields map[string]interface{}, message string, args ...interface{}) {
}
func (l *controlledPanicLogger) InfoWithFields(fields map[string]interface{}, message string, args ...interface{}) {
	if l.panicOnInfo.Load() {
		panic("controlled info panic")
	}
}
func (l *controlledPanicLogger) WarnWithFields(fields map[string]interface{}, message string, args ...interface{}) {
	if l.onWarn != nil {
		l.onWarn()
	}
	if l.panicOnWarn.Load() {
		panic("controlled warn panic")
	}
}
func (l *controlledPanicLogger) ErrorWithFields(fields map[string]interface{}, message string, args ...interface{}) {
	if l.panicOnError.Load() {
		panic("controlled error panic")
	}
}
func (l *controlledPanicLogger) FatalWithFields(fields map[string]interface{}, message string, args ...interface{}) {
}
func (l *controlledPanicLogger) PanicWithFields(fields map[string]interface{}, message string, args ...interface{}) {
}
func (l *controlledPanicLogger) LogWithFields(level logger.LogLevel, fields map[string]interface{}, message string, args ...interface{}) {
}

func (w *testWorker) GetWorkerName() string { return w.name }
func (w *testWorker) GetWorkerId() string   { return w.id }
func (w *testWorker) SetWorkerId(id string) { w.id = id }
func (w *testWorker) GetWorkerExecutionErr() error {
	return w.execErr
}
func (w *testWorker) SetWorkerExecutionErr(err error) { w.execErr = err }
func (w *testWorker) Run(runtime WorkerRuntime) error {
	if w.panicRun {
		panic("boom")
	}
	if w.runCount != nil {
		w.runCount.Add(1)
	}
	if w.signalRun != nil {
		select {
		case w.signalRun <- struct{}{}:
		default:
		}
	}
	if w.beatUntilDone {
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-runtime.Done():
				return runtime.Err()
			case <-ticker.C:
				runtime.Beat()
			}
		}
	}
	if w.blockRun != nil {
		<-w.blockRun
	}
	return w.runErr
}

func (w *panicIDWorker) GetWorkerName() string           { return "panic-id-worker" }
func (w *panicIDWorker) GetWorkerId() string             { panic("panic getting worker id") }
func (w *panicIDWorker) SetWorkerId(id string)           {}
func (w *panicIDWorker) GetWorkerExecutionErr() error    { return nil }
func (w *panicIDWorker) SetWorkerExecutionErr(err error) {}
func (w *panicIDWorker) Run(WorkerRuntime) error         { return nil }

func getTestLogger() logger.Logger {
	return logger.Init(&logger.LoggerConfig{
		Name:     "worker_test",
		Provider: logger.LoggerProviderZerolog,
		Type:     logger.LoggerTypeStdout,
		BaseDir:  "/tmp",
		Level:    logger.LogLevelDebug,
	})
}

func resetWorkerTestState() {
	startOnce = sync.Once{}
	workerList = nil
	workerChan = nil
	workerConfigByID = nil
	workerByID = nil
	workerRestartHistory = nil
	workerFailureSignalSeen = nil
	workerStatuses = nil
	overseerOptions = OverseerOptions{}
	overseerTestHookMu.Lock()
	workerChanInitializedHook = nil
	executeOverseerDoneHook = nil
	overseerTestHookMu.Unlock()
	restartTimestampsMu.Lock()
	restartTimestamps = nil
	restartTimestampsMu.Unlock()
	workerRestartLimit = 10
	workerRestartWindow = 60 * time.Second
	workerRestartBackoff = 500 * time.Millisecond
	workerRestartBackoffM = 30 * time.Second
	workerFailureSignalTTL = 2 * time.Minute
	SetOverseerSleepTimeout(time.Second)
	SetWorkerWatchdogInterval(5 * time.Second)
	SetWorkerHeartbeatTimeout(30 * time.Second)
}

func TestWorkerConfigValidateAndNewWorkerBranches(t *testing.T) {
	var nilCfg *WorkerConfig
	assert.Error(t, nilCfg.Validate())
	_, err := nilCfg.NewWorker()
	assert.Error(t, err)

	cfg := &WorkerConfig{Name: "x", New: nil, MaxCount: 1}
	assert.Error(t, cfg.Validate())

	cfg.Name = ""
	cfg.New = func() (WorkerInterface, error) { return &testWorker{name: "x"}, nil }
	assert.Error(t, cfg.Validate())

	cfg.Name = "x"
	cfg.New = func() (WorkerInterface, error) { return &testWorker{name: "x"}, nil }
	cfg.MaxCount = 0
	assert.Error(t, cfg.Validate())

	cfg.MaxCount = 1
	assert.NoError(t, cfg.Validate())

	cfg.New = func() (WorkerInterface, error) { return nil, errors.New("boom") }
	_, err = cfg.NewWorker()
	assert.Error(t, err)

	cfg.New = func() (WorkerInterface, error) { return nil, nil }
	_, err = cfg.NewWorker()
	assert.Error(t, err)

	cfg.New = func() (WorkerInterface, error) { return &testWorker{name: "ok"}, nil }
	worker, err := cfg.NewWorker()
	assert.NoError(t, err)
	assert.NotNil(t, worker)
}

func TestLoggerAndSleepConfigBranches(t *testing.T) {
	resetWorkerTestState()

	SetLogger(getTestLogger())
	assert.NotNil(t, workerlogger)
	SetLogger(nil)
	assert.NotNil(t, workerlogger)

	assert.True(t, isNilLogger(nil))
	var ptr *valueLogger
	var ptrAsLogger logger.Logger = ptr
	assert.True(t, isNilLogger(ptrAsLogger))
	assert.False(t, isNilLogger(valueLogger{}))
	assert.NotNil(t, resolveWorkerLogger(nil))
	assert.NotNil(t, resolveWorkerLogger(valueLogger{}))

	SetOverseerSleepTimeout(-1 * time.Second)
	assert.Equal(t, time.Second, GetOverseerSleepTimeout())
	SetOverseerSleepTimeout(10 * time.Millisecond)
	assert.Equal(t, minOverseerSleep, GetOverseerSleepTimeout())
	SetOverseerSleepTimeout(6 * time.Second)
	assert.Equal(t, 6*time.Second, GetOverseerSleepTimeout())
}

func TestNormalizeOverseerContext(t *testing.T) {
	var nilCtx context.Context
	if normalizeOverseerContext(nilCtx) == nil {
		t.Fatal("expected background context")
	}

	type testContextKey string
	ctx := context.WithValue(context.Background(), testContextKey("key"), "value")
	if normalizeOverseerContext(ctx) != ctx {
		t.Fatal("expected existing context to be reused")
	}
}

func TestStartAllWorkersAndLaunchWorkerBranches(t *testing.T) {
	resetWorkerTestState()
	SetLogger(getTestLogger())
	workerChan = make(chan WorkerInterface, 8)

	var created atomic.Int32
	var ran atomic.Int32
	runSignal := make(chan struct{}, 4)

	workerList = []*WorkerConfig{
		nil,
		{
			Name:      "invalid",
			MaxCount:  0,
			IsEnabled: true,
			New: func() (WorkerInterface, error) {
				return &testWorker{name: "never"}, nil
			},
		},
		{
			Name:      "disabled",
			MaxCount:  1,
			IsEnabled: false,
			New: func() (WorkerInterface, error) {
				return &testWorker{name: "never"}, nil
			},
		},
		{
			Name:      "FactoryWorker",
			MaxCount:  3,
			IsEnabled: true,
			New: func() (WorkerInterface, error) {
				created.Add(1)
				return &testWorker{name: "FactoryWorker", runCount: &ran, signalRun: runSignal}, nil
			},
		},
	}

	startAllWorkers(context.Background())

	timeout := time.After(300 * time.Millisecond)
	runEvents := 0
	for runEvents < 3 {
		select {
		case <-runSignal:
			runEvents++
		case <-timeout:
			t.Fatalf("expected 3 worker runs, got %d", runEvents)
		}
	}
	assert.Equal(t, int32(3), created.Load())
	assert.Equal(t, int32(3), ran.Load())

	failureSignals := 0
	timeout = time.After(300 * time.Millisecond)
	for failureSignals < 3 {
		select {
		case <-workerChan:
			failureSignals++
		case <-timeout:
			t.Fatalf("expected 3 worker failure signals, got %d", failureSignals)
		}
	}

	launchWorkerFromConfig(context.Background(), nil)
	workerRestartLimit = 0
	badCfg := &WorkerConfig{
		Name:      "bad",
		MaxCount:  1,
		IsEnabled: true,
		New: func() (WorkerInterface, error) {
			return nil, errors.New("new failed")
		},
	}
	launchWorkerFromConfig(context.Background(), badCfg)

	okCfg := &WorkerConfig{
		Name:      "ok",
		MaxCount:  1,
		IsEnabled: true,
		New: func() (WorkerInterface, error) {
			return &testWorker{name: "ok"}, nil
		},
	}
	launchWorkerFromConfig(context.Background(), okCfg)

	select {
	case <-workerChan:
	case <-time.After(300 * time.Millisecond):
		t.Fatal("expected worker failure signal for launched worker")
	}
}

func TestStartAllWorkersWithContextCancellation(t *testing.T) {
	resetWorkerTestState()
	SetLogger(getTestLogger())
	workerChan = make(chan WorkerInterface, 4)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var created atomic.Int32
	workerList = []*WorkerConfig{
		{
			Name:      "never-starts",
			MaxCount:  1,
			IsEnabled: true,
			New: func() (WorkerInterface, error) {
				created.Add(1)
				return &testWorker{name: "never-starts"}, nil
			},
		},
	}

	startAllWorkers(ctx)
	assert.Equal(t, int32(0), created.Load())

	resetWorkerTestState()
	SetLogger(getTestLogger())
	workerChan = make(chan WorkerInterface, 4)
	ctx, cancel = context.WithCancel(context.Background())
	runSignal := make(chan struct{}, 1)
	workerList = []*WorkerConfig{
		{
			Name:      "starts-once",
			MaxCount:  3,
			IsEnabled: true,
			New: func() (WorkerInterface, error) {
				created.Add(1)
				cancel()
				return &testWorker{name: "starts-once", signalRun: runSignal}, nil
			},
		},
	}

	created.Store(0)
	startAllWorkers(ctx)
	assert.Equal(t, int32(1), created.Load())
	select {
	case <-runSignal:
	case <-time.After(300 * time.Millisecond):
		t.Fatal("expected first worker to start before test reset")
	}
}

func TestRunWorkerSafelyBranches(t *testing.T) {
	resetWorkerTestState()
	SetLogger(getTestLogger())
	workerChan = make(chan WorkerInterface, 6)

	errRuntime, _ := newWorkerRunContext(context.Background(), "err-worker-id", "ErrWorker")
	panicRuntime, _ := newWorkerRunContext(context.Background(), "panic-worker-id", "PanicWorker")
	nilRuntime, _ := newWorkerRunContext(context.Background(), "nil-worker-id", "NilExitWorker")

	runWorkerSafely(&testWorker{name: "ErrWorker", runErr: assert.AnError}, errRuntime)
	runWorkerSafely(&testWorker{name: "PanicWorker", panicRun: true}, panicRuntime)
	runWorkerSafely(&testWorker{name: "NilExitWorker"}, nilRuntime)

	seen := map[string]bool{}
	timeout := time.After(500 * time.Millisecond)
	for len(seen) < 3 {
		select {
		case failed := <-workerChan:
			seen[failed.GetWorkerName()] = true
			assert.Error(t, failed.GetWorkerExecutionErr())
		case <-timeout:
			t.Fatalf("expected 3 worker failure signals, got %d", len(seen))
		}
	}

	canceledPanicRuntime, cancelPanic := newWorkerRunContext(context.Background(), "canceled-panic-id", "CanceledPanic")
	cancelPanic()
	canceledPanicWorker := &testWorker{name: "CanceledPanic", panicRun: true}
	runWorkerSafely(canceledPanicWorker, canceledPanicRuntime)
	assert.ErrorIs(t, canceledPanicWorker.GetWorkerExecutionErr(), context.Canceled)

	canceledErrRuntime, cancelErr := newWorkerRunContext(context.Background(), "canceled-error-id", "CanceledError")
	cancelErr()
	canceledErrWorker := &testWorker{name: "CanceledError", runErr: assert.AnError}
	runWorkerSafely(canceledErrWorker, canceledErrRuntime)
	assert.ErrorIs(t, canceledErrWorker.GetWorkerExecutionErr(), assert.AnError)

	canceledNilRuntime, cancelNil := newWorkerRunContext(context.Background(), "canceled-nil-id", "CanceledNil")
	cancelNil()
	canceledNilWorker := &testWorker{name: "CanceledNil"}
	runWorkerSafely(canceledNilWorker, canceledNilRuntime)
	assert.ErrorIs(t, canceledNilWorker.GetWorkerExecutionErr(), context.Canceled)

	select {
	case failed := <-workerChan:
		t.Fatalf("canceled runtime should not emit failure signal, got %s", failed.GetWorkerName())
	default:
	}
}

func TestSignalAndRegistrationBranches(t *testing.T) {
	resetWorkerTestState()

	signalWorkerFailure(nil)
	signalWorkerFailure(&testWorker{name: "nochan", id: "id-no-chan"})

	workerChan = make(chan WorkerInterface, 2)
	assert.True(t, shouldEmitWorkerFailureSignal(""))
	assert.True(t, shouldEmitWorkerFailureSignal("A"))
	assert.False(t, shouldEmitWorkerFailureSignal("A"))
	workerFailureSignalSeen["stale"] = time.Now().Add(-5 * time.Minute)
	assert.True(t, shouldEmitWorkerFailureSignal("B"))
	_, staleStillThere := workerFailureSignalSeen["stale"]
	assert.False(t, staleStillThere)

	w := &testWorker{name: "dup", id: "worker-id-1"}
	signalWorkerFailure(w)
	signalWorkerFailure(w)
	select {
	case <-workerChan:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("expected first failure signal")
	}
	select {
	case <-workerChan:
		t.Fatal("expected duplicate signal to be suppressed")
	case <-time.After(100 * time.Millisecond):
	}

	workerChan = make(chan WorkerInterface) // unbuffered => default branch drops instead of leaking a goroutine
	signalWorkerFailure(&testWorker{name: "default-path", id: "id-default"})
	select {
	case <-workerChan:
		t.Fatal("expected full/unbuffered channel signal to be dropped")
	case <-time.After(200 * time.Millisecond):
	}

	assert.Nil(t, unregisterRunningWorker(""))
	assert.Nil(t, unregisterRunningWorker("missing"))
	registerRunningWorker("", nil)
	cfg := &WorkerConfig{Name: "x", MaxCount: 1, IsEnabled: true, New: func() (WorkerInterface, error) {
		return &testWorker{name: "x"}, nil
	}}
	registerRunningWorker("wid", cfg)
	assert.Equal(t, cfg, unregisterRunningWorker("wid"))
	assert.Nil(t, unregisterRunningWorker("wid"))

	workerConfigByID = map[string]*WorkerConfig{"legacy-id": cfg}
	workerByID = map[string]*runningWorkerState{}
	assert.Equal(t, cfg, unregisterRunningWorker("legacy-id"))
}

func TestRestartTrackingAndSchedulingBranches(t *testing.T) {
	resetWorkerTestState()
	SetLogger(getTestLogger())
	workerChan = make(chan WorkerInterface, 4)
	workerRestartBackoff = 5 * time.Millisecond
	workerRestartBackoffM = 5 * time.Millisecond

	assert.Equal(t, workerRestartBackoff, computeWorkerRestartBackoff(1))
	assert.LessOrEqual(t, computeWorkerRestartBackoff(100), workerRestartBackoffM)
	workerRestartBackoff = 2 * time.Second
	workerRestartBackoffM = 3 * time.Second
	assert.Equal(t, 3*time.Second, computeWorkerRestartBackoff(3))
	workerRestartBackoff = 100 * time.Millisecond
	workerRestartBackoffM = time.Second
	assert.Equal(t, 200*time.Millisecond, computeWorkerRestartBackoff(2))
	workerRestartBackoff = 5 * time.Millisecond
	workerRestartBackoffM = 5 * time.Millisecond

	workerRestartHistory = make(map[string][]time.Time)
	workerRestartLimit = 3
	workerRestartWindow = time.Minute
	_, _, allowed := trackWorkerRestart("X")
	assert.True(t, allowed)
	_, _, allowed = trackWorkerRestart("X")
	assert.True(t, allowed)
	_, _, allowed = trackWorkerRestart("X")
	assert.True(t, allowed)
	_, _, allowed = trackWorkerRestart("X")
	assert.False(t, allowed)

	cfg := &WorkerConfig{
		Name:      "schedule",
		MaxCount:  1,
		IsEnabled: true,
		New: func() (WorkerInterface, error) {
			return &testWorker{name: "schedule"}, nil
		},
	}

	workerRestartHistory = make(map[string][]time.Time)
	workerRestartLimit = 1
	scheduleWorkerRestart(context.Background(), cfg, nil)
	time.Sleep(20 * time.Millisecond)
	select {
	case <-workerChan:
	case <-time.After(300 * time.Millisecond):
		t.Fatal("expected scheduled worker relaunch signal")
	}

	workerRestartHistory = make(map[string][]time.Time)
	workerRestartLimit = 0
	scheduleWorkerRestart(context.Background(), cfg, &testWorker{name: "schedule", id: "id-over-limit"})
}

func TestWorkerRuntimeAndWatchdogConfigBranches(t *testing.T) {
	resetWorkerTestState()

	runCtx, cancel := newWorkerRunContext(context.Background(), "wid", "wname")
	defer cancel()
	firstHeartbeat := runCtx.LastHeartbeat()
	time.Sleep(time.Millisecond)
	runCtx.Beat()

	assert.Equal(t, "wid", runCtx.WorkerID())
	assert.Equal(t, "wname", runCtx.WorkerName())
	assert.True(t, runCtx.LastHeartbeat().After(firstHeartbeat))

	SetWorkerWatchdogInterval(-1 * time.Second)
	assert.Equal(t, 5*time.Second, GetWorkerWatchdogInterval())
	SetWorkerWatchdogInterval(time.Millisecond)
	assert.Equal(t, minOverseerSleep, GetWorkerWatchdogInterval())
	SetWorkerWatchdogInterval(2 * time.Second)
	assert.Equal(t, 2*time.Second, GetWorkerWatchdogInterval())

	SetWorkerHeartbeatTimeout(-1 * time.Second)
	assert.Equal(t, time.Duration(0), GetWorkerHeartbeatTimeout())
	SetWorkerHeartbeatTimeout(3 * time.Second)
	assert.Equal(t, 3*time.Second, GetWorkerHeartbeatTimeout())

	applyOverseerOptions(&OverseerOptions{
		Watchdog: WatchdogOptions{
			Interval:         250 * time.Millisecond,
			HeartbeatTimeout: 750 * time.Millisecond,
		},
	})
	assert.Equal(t, 250*time.Millisecond, GetWorkerWatchdogInterval())
	assert.Equal(t, 750*time.Millisecond, GetWorkerHeartbeatTimeout())
}

func TestCheckWorkerHeartbeats(t *testing.T) {
	resetWorkerTestState()
	SetLogger(getTestLogger())
	workerChan = make(chan WorkerInterface, 4)
	workerConfigByID = make(map[string]*WorkerConfig)
	workerByID = make(map[string]*runningWorkerState)
	workerRestartBackoff = 5 * time.Millisecond
	workerRestartBackoffM = 5 * time.Millisecond

	workerByID["nil-state"] = nil
	SetWorkerHeartbeatTimeout(0)
	checkWorkerHeartbeats(context.Background())
	assert.Contains(t, workerByID, "nil-state")

	SetWorkerHeartbeatTimeout(20 * time.Millisecond)
	healthyRuntime, healthyCancel := newWorkerRunContext(context.Background(), "healthy-id", "healthy")
	defer healthyCancel()
	healthyCfg := &WorkerConfig{Name: "healthy", MaxCount: 1, IsEnabled: true, New: func() (WorkerInterface, error) {
		return &testWorker{name: "healthy"}, nil
	}}
	registerRunningWorkerState("healthy-id", healthyCfg, &testWorker{name: "healthy", id: "healthy-id"}, healthyRuntime, healthyCancel)
	checkWorkerHeartbeats(context.Background())
	assert.NotNil(t, unregisterRunningWorker("healthy-id"))

	var errorsSeen atomic.Int32
	var stopsSeen atomic.Int32
	var restartsSeen atomic.Int32
	replacementStarted := make(chan struct{}, 1)
	releaseReplacement := make(chan struct{})
	staleCfg := &WorkerConfig{Name: "stale", MaxCount: 1, IsEnabled: true, New: func() (WorkerInterface, error) {
		return &testWorker{name: "stale", signalRun: replacementStarted, blockRun: releaseReplacement}, nil
	}}
	setOverseerOptions(OverseerOptions{
		Hooks: WorkerHooks{
			OnError:   func(WorkerEvent) { errorsSeen.Add(1) },
			OnStop:    func(WorkerEvent) { stopsSeen.Add(1) },
			OnRestart: func(WorkerEvent) { restartsSeen.Add(1) },
		},
	})
	staleRuntime, staleCancel := newWorkerRunContext(context.Background(), "stale-id", "stale")
	staleRuntime.lastHeartbeat.Store(time.Now().Add(-time.Second).UnixNano())
	staleWorker := &testWorker{name: "stale", id: "stale-id"}
	registerRunningWorkerState("stale-id", staleCfg, staleWorker, staleRuntime, staleCancel)

	checkWorkerHeartbeats(context.Background())

	assert.Error(t, staleRuntime.Err())
	assert.Error(t, staleWorker.GetWorkerExecutionErr())
	assert.Contains(t, staleWorker.GetWorkerExecutionErr().Error(), "heartbeat timed out")
	assert.Nil(t, unregisterRunningWorker("stale-id"))
	assert.Equal(t, int32(1), errorsSeen.Load())
	assert.Equal(t, int32(1), stopsSeen.Load())
	assert.Equal(t, int32(1), restartsSeen.Load())

	select {
	case <-replacementStarted:
	case <-time.After(300 * time.Millisecond):
		t.Fatal("expected watchdog to start replacement worker")
	}
	close(releaseReplacement)
	select {
	case <-workerChan:
	case <-time.After(300 * time.Millisecond):
		t.Fatal("expected replacement worker to finish before test reset")
	}
}

func TestScheduleWorkerRestartWithContextCancellation(t *testing.T) {
	resetWorkerTestState()
	SetLogger(getTestLogger())
	workerChan = make(chan WorkerInterface, 1)
	workerRestartHistory = make(map[string][]time.Time)
	workerRestartBackoff = 5 * time.Millisecond
	workerRestartBackoffM = 5 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var restartHooks atomic.Int32
	var created atomic.Int32
	setOverseerOptions(OverseerOptions{
		Hooks: WorkerHooks{
			OnRestart: func(WorkerEvent) {
				restartHooks.Add(1)
			},
		},
	})
	cfg := &WorkerConfig{
		Name:      "canceled-schedule",
		MaxCount:  1,
		IsEnabled: true,
		New: func() (WorkerInterface, error) {
			created.Add(1)
			return &testWorker{name: "canceled-schedule"}, nil
		},
	}

	scheduleWorkerRestart(ctx, cfg, &testWorker{name: "canceled-schedule", id: "wid"})
	assert.Empty(t, workerRestartHistory)
	assert.Equal(t, int32(0), restartHooks.Load())
	assert.Equal(t, int32(0), created.Load())
}

func TestExecuteOverseerPanicRecoveryPath(t *testing.T) {
	resetWorkerTestState()
	SetLogger(getTestLogger())
	restartLimit = 0
	restartTimestamps = []time.Time{time.Now()}

	workerList = []*WorkerConfig{
		{
			Name:      "panic-worker",
			MaxCount:  1,
			IsEnabled: true,
			New: func() (WorkerInterface, error) {
				panic("factory panic")
			},
		},
	}

	// Should recover and return without hanging.
	executeOverseer(context.Background(), true)
}

func TestExecuteOverseerNonPanicMonitorPathWithInvalidConfigs(t *testing.T) {
	resetWorkerTestState()
	SetOverseerSleepTimeout(20 * time.Millisecond)
	restartLimit = 0
	restartTimestamps = nil

	SetLogger(getTestLogger())
	chanReady := make(chan chan WorkerInterface, 1)
	overseerTestHookMu.Lock()
	workerChanInitializedHook = func(ch chan WorkerInterface) {
		select {
		case chanReady <- ch:
		default:
		}
	}
	overseerTestHookMu.Unlock()

	workerList = []*WorkerConfig{
		nil,
		{
			Name:      "disabled",
			MaxCount:  1,
			IsEnabled: false,
			New: func() (WorkerInterface, error) {
				return &testWorker{name: "never"}, nil
			},
		},
		{
			Name:      "invalid",
			MaxCount:  0,
			IsEnabled: true,
			New: func() (WorkerInterface, error) {
				return &testWorker{name: "never"}, nil
			},
		},
	}

	done := make(chan struct{})
	go func() {
		executeOverseer(context.Background(), false)
		close(done)
	}()

	var ch chan WorkerInterface
	select {
	case ch = <-chanReady:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("expected workerChan to be initialized")
	}
	ch <- &panicIDWorker{}

	select {
	case <-done:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("executeOverseer did not exit in controlled monitor path")
	}
}

func TestStartOverseerInitializesAndHonorsOnce(t *testing.T) {
	resetWorkerTestState()
	restartLimit = 0
	restartTimestamps = []time.Time{time.Now()}
	done := make(chan struct{}, 1)
	overseerTestHookMu.Lock()
	executeOverseerDoneHook = func() {
		select {
		case done <- struct{}{}:
		default:
		}
	}
	overseerTestHookMu.Unlock()

	workersA := []*WorkerConfig{
		{
			Name:      "once-worker-a",
			MaxCount:  1,
			IsEnabled: true,
			New: func() (WorkerInterface, error) {
				panic("panic to short-circuit execute loop")
			},
		},
	}
	workersB := []*WorkerConfig{
		{
			Name:      "once-worker-b",
			MaxCount:  1,
			IsEnabled: true,
			New: func() (WorkerInterface, error) {
				return &testWorker{name: "b"}, nil
			},
		},
	}

	StartOverseer(context.Background(), workersA, nil, nil)
	select {
	case <-done:
	case <-time.After(300 * time.Millisecond):
		t.Fatal("expected overseer to finish")
	}
	assert.Equal(t, workersA[0].Name, workerList[0].Name)
	assert.NotSame(t, workersA[0], workerList[0])
	assert.NotNil(t, workerlogger)

	// startOnce should ignore subsequent calls.
	StartOverseer(context.Background(), workersB, getTestLogger(), nil)
	assert.Equal(t, workersA[0].Name, workerList[0].Name)
}

func TestStartOverseerStopsMonitorAndRecordsStatus(t *testing.T) {
	resetWorkerTestState()
	SetOverseerSleepTimeout(20 * time.Millisecond)
	workerRestartBackoff = 5 * time.Millisecond
	workerRestartBackoffM = 5 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{}, 1)
	overseerTestHookMu.Lock()
	executeOverseerDoneHook = func() {
		select {
		case done <- struct{}{}:
		default:
		}
	}
	overseerTestHookMu.Unlock()

	var starts atomic.Int32
	var errorsSeen atomic.Int32
	var restarts atomic.Int32
	runSignal := make(chan struct{}, 1)
	restartSignal := make(chan struct{}, 1)
	workers := []*WorkerConfig{
		{
			Name:      "ctx-worker",
			MaxCount:  1,
			IsEnabled: true,
			New: func() (WorkerInterface, error) {
				return &testWorker{name: "ctx-worker", signalRun: runSignal}, nil
			},
		},
	}

	StartOverseer(ctx, workers, getTestLogger(), &OverseerOptions{
		RestartPolicy: RestartPolicy{Limit: 1, Window: time.Minute, MinBackoff: 5 * time.Millisecond, MaxBackoff: 5 * time.Millisecond},
		Hooks: WorkerHooks{
			OnStart: func(WorkerEvent) { starts.Add(1) },
			OnError: func(WorkerEvent) { errorsSeen.Add(1) },
			OnRestart: func(WorkerEvent) {
				restarts.Add(1)
				select {
				case restartSignal <- struct{}{}:
				default:
				}
			},
		},
	})

	select {
	case <-runSignal:
	case <-time.After(300 * time.Millisecond):
		t.Fatal("expected worker to start")
	}
	select {
	case <-restartSignal:
	case <-time.After(300 * time.Millisecond):
		t.Fatal("expected restart hook")
	}

	cancel()
	select {
	case <-done:
	case <-time.After(300 * time.Millisecond):
		t.Fatal("expected overseer to stop")
	}

	statuses := SnapshotWorkerStatuses()
	assert.NotEmpty(t, statuses)
	assert.GreaterOrEqual(t, starts.Load(), int32(1))
	assert.GreaterOrEqual(t, errorsSeen.Load(), int32(1))
	assert.GreaterOrEqual(t, restarts.Load(), int32(1))
}

func TestStartOverseerAcceptsTodoContext(t *testing.T) {
	resetWorkerTestState()
	restartLimit = 0
	restartTimestamps = []time.Time{time.Now()}

	done := make(chan struct{}, 1)
	overseerTestHookMu.Lock()
	executeOverseerDoneHook = func() {
		select {
		case done <- struct{}{}:
		default:
		}
	}
	overseerTestHookMu.Unlock()

	StartOverseer(context.TODO(), []*WorkerConfig{
		{
			Name:      "nil-context",
			MaxCount:  1,
			IsEnabled: true,
			New: func() (WorkerInterface, error) {
				panic("stop")
			},
		},
	}, getTestLogger(), nil)

	select {
	case <-done:
	case <-time.After(300 * time.Millisecond):
		t.Fatal("expected overseer to finish")
	}
}

func TestExecuteOverseerRecoveryHonorsCanceledContext(t *testing.T) {
	resetWorkerTestState()
	SetOverseerSleepTimeout(20 * time.Millisecond)
	restartLimit = 1

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	SetLogger(&controlledPanicLogger{
		onWarn: cancel,
	})
	workerList = []*WorkerConfig{
		{
			Name:      "canceled-recovery",
			MaxCount:  1,
			IsEnabled: true,
			New: func() (WorkerInterface, error) {
				panic("stop")
			},
		},
	}

	executeOverseer(ctx, true)
}

func TestWorkerHelperBranches(t *testing.T) {
	assert.Nil(t, cloneWorkerConfigs(nil))

	cloned := cloneWorkerConfigs([]*WorkerConfig{nil})
	assert.Len(t, cloned, 1)
	assert.Nil(t, cloned[0])

	updateWorkerStatusStopped("", nil)
	workerStatuses = nil
	updateWorkerStatusStopped("missing", nil)
}

func TestMonitorWorkersBranches(t *testing.T) {
	resetWorkerTestState()
	SetOverseerSleepTimeout(20 * time.Millisecond)
	workerRestartLimit = 0
	workerRestartHistory = make(map[string][]time.Time)
	workerChan = make(chan WorkerInterface, 8)

	panicLogger := &controlledPanicLogger{}
	SetLogger(panicLogger)

	done := make(chan struct{})
	go func() {
		defer func() {
			_ = recover()
			close(done)
		}()
		monitorWorkers(context.Background())
	}()

	workerChan <- &testWorker{name: "unknown", id: "unknown-id"}

	restartCfg := &WorkerConfig{
		Name:      "monitor-restart",
		MaxCount:  1,
		IsEnabled: true,
		New: func() (WorkerInterface, error) {
			return &testWorker{name: "monitor-restart"}, nil
		},
	}
	registerRunningWorker("known-id", restartCfg)
	workerChan <- &testWorker{name: "known", id: "known-id"}

	time.Sleep(40 * time.Millisecond) // allow ticker path
	panicLogger.panicOnError.Store(true)
	workerChan <- nil

	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("monitorWorkers did not exit after controlled panic")
	}
}

func TestMonitorWorkersTickerPath(t *testing.T) {
	resetWorkerTestState()
	SetLogger(getTestLogger())
	SetWorkerWatchdogInterval(20 * time.Millisecond)
	workerChan = make(chan WorkerInterface, 1)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		monitorWorkers(ctx)
		close(done)
	}()

	time.Sleep(150 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("monitorWorkers did not stop after ticker path")
	}
}

func TestExecuteOverseerRecoveryRestartBranch(t *testing.T) {
	resetWorkerTestState()
	SetOverseerSleepTimeout(100 * time.Millisecond)
	restartLimit = 1
	restartTimestamps = nil

	panicLogger := &controlledPanicLogger{}
	panicLogger.panicOnInfo.Store(true)
	SetLogger(panicLogger)

	workerList = []*WorkerConfig{
		{
			Name:      "panic-on-info",
			MaxCount:  1,
			IsEnabled: true,
			New: func() (WorkerInterface, error) {
				return &testWorker{name: "panic-on-info"}, nil
			},
		},
	}

	executeOverseer(context.Background(), false)
	time.Sleep(250 * time.Millisecond)
}
