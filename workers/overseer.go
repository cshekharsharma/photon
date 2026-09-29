package workers

import (
	"context"
	"sync"
	"time"

	"github.com/cshekharsharma/photon/core/logger"
)

var (
	startOnce sync.Once // Ensure that the overseer is started only once

	workerList   []*WorkerConfig
	workerChan   chan WorkerInterface
	workerlogger logger.Logger

	restartTimestampsMu sync.Mutex
	restartTimestamps   []time.Time
	restartLimit        = 5
	restartWindow       = 60 * time.Second

	workerRestartMu       sync.Mutex
	workerRestartHistory  map[string][]time.Time
	workerRestartLimit    = 10
	workerRestartWindow   = 60 * time.Second
	workerRestartBackoff  = 500 * time.Millisecond
	workerRestartBackoffM = 30 * time.Second

	workerByIDMu     sync.Mutex
	workerConfigByID map[string]*WorkerConfig
	workerByID       map[string]*runningWorkerState

	overseerConfigMu     sync.RWMutex
	overseerSleepTimeout = 1 * time.Second
	minOverseerSleep     = 100 * time.Millisecond
	workerWatchdogEvery  = 5 * time.Second
	workerHeartbeatLimit = 30 * time.Second

	workerFailureSignalMu   sync.Mutex
	workerFailureSignalSeen map[string]time.Time
	workerFailureSignalTTL  = 2 * time.Minute

	workerStatusMu sync.RWMutex
	workerStatuses map[string]WorkerStatus

	overseerOptionsMu sync.RWMutex
	overseerOptions   OverseerOptions

	overseerTestHookMu        sync.RWMutex
	workerChanInitializedHook func(chan WorkerInterface)
	executeOverseerDoneHook   func()
)

// StartOverseer initializes the overseer with a list of workers and a logger,
// ensuring it only starts once. It launches the overseer loop in a separate goroutine.
// The overseer manages lifecycle of workers and restarts them upon failure.
func StartOverseer(ctx context.Context, workers []*WorkerConfig, logger logger.Logger, options *OverseerOptions) {
	ctx = normalizeOverseerContext(ctx)
	startOnce.Do(func() {
		workerList = cloneWorkerConfigs(workers)
		workerlogger = resolveWorkerLogger(logger)
		applyOverseerOptions(options)
		workerRestartHistory = make(map[string][]time.Time, len(workerList))
		workerConfigByID = make(map[string]*WorkerConfig)
		workerByID = make(map[string]*runningWorkerState)
		workerFailureSignalSeen = make(map[string]time.Time)
		workerStatuses = make(map[string]WorkerStatus)
		startWorkers := true

		workerlogger.Info("[Overseer] Starting overseer job with %d workers: %v", len(workerList), workerList)
		go executeOverseer(ctx, startWorkers)
	})
}

func normalizeOverseerContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

// executeOverseer is the main loop that initializes the worker pool and monitors it.
// It includes recovery logic to restart itself in case of internal panic, and enforces
// a restart rate limit to avoid infinite crash loops.
func executeOverseer(ctx context.Context, startWorkers bool) {
	ctx = normalizeOverseerContext(ctx)
	defer notifyExecuteOverseerDone()
	defer func() {
		if r := recover(); r != nil {
			workerlogger.Warn("[WorkerOverseer] Recovered from panic: %+v. Restarting overseer...\n", r)

			now := time.Now()

			restartTimestampsMu.Lock()
			pruned := make([]time.Time, 0, len(restartTimestamps))
			for _, ts := range restartTimestamps {
				if now.Sub(ts) <= restartWindow {
					pruned = append(pruned, ts)
				}
			}
			restartTimestamps = append(pruned, now)
			restartCount := len(restartTimestamps)
			restartTimestampsMu.Unlock()

			if restartCount > restartLimit {
				workerlogger.Error("[WorkerOverseer] Exceeded %d restarts in %v. "+
					"Shutting down for safety.", restartLimit, restartWindow)

				return // in case of too many restarts, we don't want to restart again
			}

			select {
			case <-ctx.Done():
				return
			case <-time.After(GetOverseerSleepTimeout()):
			}
			go executeOverseer(ctx, false) // restart self
		}
	}()

	// Allocate exact buffer size to avoid blocking on signal.
	totalWorkerCount := 0
	for _, cfg := range workerList {
		if cfg == nil || !cfg.IsEnabled {
			continue
		}
		if err := cfg.Validate(); err != nil {
			workerlogger.Error("[WorkerOverseer] Invalid worker config name=%s: %v", cfg.Name, err)
			continue
		}

		totalWorkerCount += int(cfg.MaxCount)

		workerlogger.Info("[WorkerOverseer] Total %d threads to monitor for worker: %s",
			totalWorkerCount, cfg.Name)
	}
	if totalWorkerCount < 1 {
		totalWorkerCount = 1
	}
	workerChan = make(chan WorkerInterface, totalWorkerCount)
	notifyWorkerChanInitialized(workerChan)

	if startWorkers {
		startAllWorkers(ctx)
	}

	monitorWorkers(ctx)
}
