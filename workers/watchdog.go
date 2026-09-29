package workers

import (
	"context"
	"fmt"
	"time"
)

// monitorWorkers listens for crashed or failed workers on a channel,
// and restarts them automatically.
func monitorWorkers(ctx context.Context) {
	ticker := time.NewTicker(GetWorkerWatchdogInterval())
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case failedWorker := <-workerChan:
			if failedWorker == nil {
				workerlogger.Error("[WorkerOverseer] Nil worker failure signal received. Skipping restart.")
				continue
			}

			state := unregisterRunningWorkerState(failedWorker.GetWorkerId())
			var cfg *WorkerConfig
			if state != nil {
				cfg = state.cfg
				if state.cancel != nil {
					state.cancel()
				}
			}
			updateWorkerStatusStopped(failedWorker.GetWorkerId(), failedWorker.GetWorkerExecutionErr())
			callWorkerHook(func(h WorkerHooks) func(WorkerEvent) { return h.OnStop }, WorkerEvent{
				Name: failedWorker.GetWorkerName(),
				ID:   failedWorker.GetWorkerId(),
				Err:  failedWorker.GetWorkerExecutionErr(),
				At:   time.Now(),
			})
			if cfg == nil {
				workerlogger.Warn(
					"[WorkerOverseer] Missing worker config for worker=%s id=%s. "+
						"Skipping restart (likely duplicate or stale failure signal).",
					failedWorker.GetWorkerName(),
					failedWorker.GetWorkerId(),
				)
				continue
			}

			workerlogger.Warn("[WorkerOverseer] Worker %s (id=%s) reported failure. Restarting...",
				failedWorker.GetWorkerName(), failedWorker.GetWorkerId())

			scheduleWorkerRestart(ctx, cfg, failedWorker)

		case <-ticker.C:
			checkWorkerHeartbeats(ctx)
		}
	}
}

func checkWorkerHeartbeats(ctx context.Context) {
	timeout := GetWorkerHeartbeatTimeout()
	if timeout <= 0 {
		return
	}

	now := time.Now()
	staleStates := staleRunningWorkers(now, timeout)
	for _, state := range staleStates {
		err := fmt.Errorf("worker heartbeat timed out after %s", timeout)
		state.worker.SetWorkerExecutionErr(err)
		if state.cancel != nil {
			state.cancel()
		}
		updateWorkerStatusStopped(state.runtime.WorkerID(), err)
		callWorkerHook(func(h WorkerHooks) func(WorkerEvent) { return h.OnError }, WorkerEvent{
			Name: state.worker.GetWorkerName(),
			ID:   state.runtime.WorkerID(),
			Err:  err,
			At:   now,
		})
		callWorkerHook(func(h WorkerHooks) func(WorkerEvent) { return h.OnStop }, WorkerEvent{
			Name: state.worker.GetWorkerName(),
			ID:   state.runtime.WorkerID(),
			Err:  err,
			At:   now,
		})
		workerlogger.Error("[WorkerOverseer] Worker %s (id=%s) missed heartbeat. Restarting.",
			state.worker.GetWorkerName(), state.runtime.WorkerID())
		scheduleWorkerRestart(ctx, state.cfg, state.worker)
	}
}

func staleRunningWorkers(now time.Time, timeout time.Duration) []*runningWorkerState {
	workerByIDMu.Lock()
	defer workerByIDMu.Unlock()

	stale := make([]*runningWorkerState, 0)
	for workerID, state := range workerByID {
		if state == nil || state.runtime == nil {
			continue
		}
		if now.Sub(state.runtime.LastHeartbeat()) <= timeout {
			continue
		}
		delete(workerByID, workerID)
		delete(workerConfigByID, workerID)
		stale = append(stale, state)
	}
	return stale
}
