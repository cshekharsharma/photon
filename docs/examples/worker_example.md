# Workers

Use the overseer when a service needs supervised background loops with restart limits, lifecycle hooks, and operational snapshots.

```go
package examples

import (
	"context"
	"errors"
	"time"

	"github.com/cshekharsharma/photon/core/logger"
	"github.com/cshekharsharma/photon/workers"
)

type EmailWorker struct {
	ctx context.Context
	id  string
	err error
}

func (w *EmailWorker) GetWorkerName() string       { return "email-dispatcher" }
func (w *EmailWorker) GetWorkerId() string         { return w.id }
func (w *EmailWorker) SetWorkerId(id string)       { w.id = id }
func (w *EmailWorker) GetWorkerExecutionErr() error { return w.err }
func (w *EmailWorker) SetWorkerExecutionErr(err error) {
	w.err = err
}

func (w *EmailWorker) Run(_ chan<- workers.WorkerInterface) error {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-w.ctx.Done():
			return nil
		case <-ticker.C:
			if err := pollAndSendEmail(w.ctx); err != nil {
				w.err = err
				return err
			}
		}
	}
}

func StartWorkers(ctx context.Context) {
	log := logger.Init(&logger.LoggerConfig{
		Name:     "workers",
		Provider: logger.LoggerProviderZerolog,
		Type:     logger.LoggerTypeStdout,
		Level:    logger.LogLevelInfo,
	})

	workers.StartOverseer(ctx, []*workers.WorkerConfig{
		{
			Name:      "email-dispatcher",
			MaxCount:  2,
			IsEnabled: true,
			New: func() (workers.WorkerInterface, error) {
				return &EmailWorker{ctx: ctx}, nil
			},
		},
	}, log, &workers.OverseerOptions{
		RestartPolicy: workers.RestartPolicy{
			Limit:      5,
			Window:     time.Minute,
			MinBackoff: 500 * time.Millisecond,
			MaxBackoff: 10 * time.Second,
		},
		Hooks: workers.WorkerHooks{
			OnError: func(event workers.WorkerEvent) {
				log.Error("worker=%s id=%s error=%v", event.Name, event.ID, event.Err)
			},
			OnRestart: func(event workers.WorkerEvent) {
				log.Warn("worker=%s restart=%d", event.Name, event.RestartCount)
			},
		},
	})

	_ = workers.SnapshotWorkerStatuses()
}

func pollAndSendEmail(ctx context.Context) error {
	if ctx.Err() != nil {
		return nil
	}
	return errors.New("transient queue read failure")
}
```
