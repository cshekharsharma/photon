package workers

import (
	"context"
	"time"
)

func scheduleWorkerRestart(ctx context.Context, cfg *WorkerConfig, failedWorker WorkerInterface) {
	ctx = normalizeOverseerContext(ctx)
	workerName := cfg.Name
	if failedWorker != nil && failedWorker.GetWorkerName() != "" {
		workerName = failedWorker.GetWorkerName()
	}

	select {
	case <-ctx.Done():
		return
	default:
	}

	restartCount, restartDelay, allowed := trackWorkerRestart(workerName)
	if !allowed {
		workerlogger.Error(
			"[WorkerOverseer] Worker %s exceeded restart budget (%d in %v). Not restarting.",
			workerName,
			workerRestartLimit,
			workerRestartWindow,
		)
		return
	}
	updateWorkerStatusRestart(workerName, restartCount)
	callWorkerHook(func(h WorkerHooks) func(WorkerEvent) { return h.OnRestart }, WorkerEvent{
		Name:         workerName,
		ID:           workerIDFromFailure(failedWorker),
		Err:          errorFromFailure(failedWorker),
		RestartCount: restartCount,
		At:           time.Now(),
	})

	workerlogger.Warn(
		"[WorkerOverseer] Worker %s restart attempt=%d delayed by %s",
		workerName,
		restartCount,
		restartDelay,
	)

	timer := time.NewTimer(restartDelay)
	go func() {
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		launchWorkerFromConfig(ctx, cfg)
	}()
}

func trackWorkerRestart(workerName string) (int, time.Duration, bool) {
	workerRestartMu.Lock()
	defer workerRestartMu.Unlock()

	if workerRestartHistory == nil {
		workerRestartHistory = make(map[string][]time.Time)
	}

	now := time.Now()
	history := workerRestartHistory[workerName]

	pruned := make([]time.Time, 0, len(history)+1)
	for _, ts := range history {
		if now.Sub(ts) <= workerRestartWindow {
			pruned = append(pruned, ts)
		}
	}

	pruned = append(pruned, now)
	workerRestartHistory[workerName] = pruned

	restartCount := len(pruned)
	if restartCount > workerRestartLimit {
		return restartCount, 0, false
	}

	return restartCount, computeWorkerRestartBackoff(restartCount), true
}

func computeWorkerRestartBackoff(restartCount int) time.Duration {
	if restartCount <= 1 {
		return workerRestartBackoff
	}

	backoff := workerRestartBackoff
	for i := 1; i < restartCount; i++ {
		if backoff >= workerRestartBackoffM/2 {
			return workerRestartBackoffM
		}
		backoff *= 2
	}

	return backoff
}

func workerIDFromFailure(worker WorkerInterface) string {
	if worker == nil {
		return ""
	}
	return worker.GetWorkerId()
}

func errorFromFailure(worker WorkerInterface) error {
	if worker == nil {
		return nil
	}
	return worker.GetWorkerExecutionErr()
}
