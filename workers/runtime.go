package workers

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

type runningWorkerState struct {
	cfg     *WorkerConfig
	worker  WorkerInterface
	runtime *workerRunContext
	cancel  context.CancelFunc
}

type workerRunContext struct {
	context.Context
	workerID      string
	workerName    string
	lastHeartbeat atomic.Int64
}

func newWorkerRunContext(parent context.Context, workerID string, workerName string) (*workerRunContext, context.CancelFunc) {
	ctx, cancel := context.WithCancel(normalizeOverseerContext(parent))
	runCtx := &workerRunContext{
		Context:    ctx,
		workerID:   workerID,
		workerName: workerName,
	}
	runCtx.Beat()
	return runCtx, cancel
}

func (ctx *workerRunContext) Beat() {
	ctx.lastHeartbeat.Store(time.Now().UnixNano())
}

func (ctx *workerRunContext) WorkerID() string {
	return ctx.workerID
}

func (ctx *workerRunContext) WorkerName() string {
	return ctx.workerName
}

func (ctx *workerRunContext) LastHeartbeat() time.Time {
	return time.Unix(0, ctx.lastHeartbeat.Load())
}

// startAllWorkers initializes and launches all workers based on their configuration.
// Each worker is launched in its own goroutine.
func startAllWorkers(ctx context.Context) {
	ctx = normalizeOverseerContext(ctx)
	for _, cfg := range workerList {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if cfg == nil || !cfg.IsEnabled {
			continue
		}
		if err := cfg.Validate(); err != nil {
			workerlogger.Error("[WorkerOverseer] Skipping invalid worker config name=%s: %v", cfg.Name, err)
			continue
		}

		for i := 0; i < int(cfg.MaxCount); i++ {
			select {
			case <-ctx.Done():
				return
			default:
			}
			workerlogger.Info("[WorkerOverseer] Launching worker: %s (%d/%d)", cfg.Name, i+1, cfg.MaxCount)
			launchWorkerFromConfig(ctx, cfg)
		}
	}
}

func launchWorkerFromConfig(ctx context.Context, cfg *WorkerConfig) {
	ctx = normalizeOverseerContext(ctx)
	if cfg == nil {
		workerlogger.Error("[WorkerOverseer] launchWorkerFromConfig called with nil config")
		return
	}

	worker, err := cfg.NewWorker()
	if err != nil {
		workerlogger.Error("[WorkerOverseer] Failed creating worker name=%s: %v", cfg.Name, err)
		scheduleWorkerRestart(ctx, cfg, nil)
		return
	}

	launchWorker(ctx, worker, cfg)
}

// launchWorker assigns a new ID to the worker, clears any previous error, and starts it
// in a goroutine. The worker must push itself to the channel on exit or panic.
func launchWorker(ctx context.Context, worker WorkerInterface, cfg *WorkerConfig) {
	worker.SetWorkerExecutionErr(nil)
	workerID := generateNewWorkerId()
	worker.SetWorkerId(workerID)
	runCtx, cancel := newWorkerRunContext(ctx, workerID, cfg.Name)
	registerRunningWorkerState(workerID, cfg, worker, runCtx, cancel)
	updateWorkerStatusStarted(workerID, cfg.Name)
	callWorkerHook(func(h WorkerHooks) func(WorkerEvent) { return h.OnStart }, WorkerEvent{
		Name: cfg.Name,
		ID:   workerID,
		At:   time.Now(),
	})

	workerlogger.Info("[WorkerOverseer] Starting worker %s with ID %s",
		worker.GetWorkerName(), worker.GetWorkerId())

	go runWorkerSafely(worker, runCtx)
}

func runWorkerSafely(worker WorkerInterface, runtime WorkerRuntime) {
	defer func() {
		if r := recover(); r != nil {
			if runtime.Err() != nil {
				worker.SetWorkerExecutionErr(runtime.Err())
				return
			}
			worker.SetWorkerExecutionErr(fmt.Errorf("worker panic: %v", r))
			workerlogger.Error("[WorkerOverseer] Worker %s (id=%s) panicked: %v\n%s",
				worker.GetWorkerName(),
				worker.GetWorkerId(),
				r,
				string(debug.Stack()),
			)
			callWorkerHook(func(h WorkerHooks) func(WorkerEvent) { return h.OnError }, WorkerEvent{
				Name: worker.GetWorkerName(),
				ID:   worker.GetWorkerId(),
				Err:  worker.GetWorkerExecutionErr(),
				At:   time.Now(),
			})
			signalWorkerFailure(worker)
		}
	}()

	if err := worker.Run(runtime); err != nil {
		worker.SetWorkerExecutionErr(err)
		if runtime.Err() != nil {
			return
		}
		workerlogger.Error("[WorkerOverseer] Worker %s (id=%s) exited with error: %v",
			worker.GetWorkerName(),
			worker.GetWorkerId(),
			err,
		)
		callWorkerHook(func(h WorkerHooks) func(WorkerEvent) { return h.OnError }, WorkerEvent{
			Name: worker.GetWorkerName(),
			ID:   worker.GetWorkerId(),
			Err:  err,
			At:   time.Now(),
		})
		signalWorkerFailure(worker)
		return
	}

	if runtime.Err() != nil {
		worker.SetWorkerExecutionErr(runtime.Err())
		return
	}

	worker.SetWorkerExecutionErr(fmt.Errorf("worker exited unexpectedly without error"))
	workerlogger.Warn("[WorkerOverseer] Worker %s (id=%s) exited unexpectedly without error. Restarting.",
		worker.GetWorkerName(),
		worker.GetWorkerId(),
	)
	callWorkerHook(func(h WorkerHooks) func(WorkerEvent) { return h.OnError }, WorkerEvent{
		Name: worker.GetWorkerName(),
		ID:   worker.GetWorkerId(),
		Err:  worker.GetWorkerExecutionErr(),
		At:   time.Now(),
	})
	signalWorkerFailure(worker)
}

func signalWorkerFailure(worker WorkerInterface) {
	if worker == nil || workerChan == nil {
		return
	}
	if !shouldEmitWorkerFailureSignal(worker.GetWorkerId()) {
		return
	}

	select {
	case workerChan <- worker:
	default:
		workerlogger.Warn("[WorkerOverseer] Worker failure channel is full. Dropping duplicate/stale signal for worker id=%s", worker.GetWorkerId())
	}
}

func shouldEmitWorkerFailureSignal(workerID string) bool {
	if workerID == "" {
		return true
	}

	now := time.Now()
	workerFailureSignalMu.Lock()
	defer workerFailureSignalMu.Unlock()

	if workerFailureSignalSeen == nil {
		workerFailureSignalSeen = make(map[string]time.Time)
	}

	for id, ts := range workerFailureSignalSeen {
		if now.Sub(ts) > workerFailureSignalTTL {
			delete(workerFailureSignalSeen, id)
		}
	}

	if ts, ok := workerFailureSignalSeen[workerID]; ok && now.Sub(ts) <= workerFailureSignalTTL {
		return false
	}

	workerFailureSignalSeen[workerID] = now
	return true
}

func registerRunningWorker(workerID string, cfg *WorkerConfig) {
	registerRunningWorkerState(workerID, cfg, nil, nil, nil)
}

func registerRunningWorkerState(
	workerID string,
	cfg *WorkerConfig,
	worker WorkerInterface,
	runtime *workerRunContext,
	cancel context.CancelFunc,
) {
	if workerID == "" || cfg == nil {
		return
	}
	workerByIDMu.Lock()
	defer workerByIDMu.Unlock()
	if workerConfigByID == nil {
		workerConfigByID = make(map[string]*WorkerConfig)
	}
	if workerByID == nil {
		workerByID = make(map[string]*runningWorkerState)
	}
	workerConfigByID[workerID] = cfg
	workerByID[workerID] = &runningWorkerState{
		cfg:     cfg,
		worker:  worker,
		runtime: runtime,
		cancel:  cancel,
	}
}

func unregisterRunningWorker(workerID string) *WorkerConfig {
	state := unregisterRunningWorkerState(workerID)
	if state == nil {
		return nil
	}
	return state.cfg
}

func unregisterRunningWorkerState(workerID string) *runningWorkerState {
	if workerID == "" {
		return nil
	}
	workerByIDMu.Lock()
	defer workerByIDMu.Unlock()
	if workerConfigByID == nil {
		return nil
	}
	state := workerByID[workerID]
	if state == nil {
		if cfg := workerConfigByID[workerID]; cfg != nil {
			state = &runningWorkerState{cfg: cfg}
		}
	}
	delete(workerConfigByID, workerID)
	delete(workerByID, workerID)
	return state
}

// generateNewWorkerId returns a new UUID string to assign as worker ID.
func generateNewWorkerId() string {
	return uuid.New().String()
}
