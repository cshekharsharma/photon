package watcher

import (
	"context"
	"errors"

	"github.com/cshekharsharma/photon/cloud"
	"github.com/cshekharsharma/photon/cloud/entity/appconfig"
	"github.com/cshekharsharma/photon/core/logger"
)

// AwsAppConfigWatcher is responsible for watching configuration changes.
type AwsAppConfigWatcher struct {
	watcherOptions *WatcherOptions
}

// NewAwsAppConfigWatcher creates a new watcher with the config and update handler.
func NewAwsAppConfigWatcher(cfg *WatcherOptions, configSource string) *AwsAppConfigWatcher {
	if cfg == nil {
		cfg = &WatcherOptions{}
	}
	if cfg.Logger == nil {
		cfg.Logger = defaultWatcherLogger()
	}
	return &AwsAppConfigWatcher{
		watcherOptions: cfg,
	}
}

// Watch begins watching AppConfig. It triggers the updater on change.
func (a *AwsAppConfigWatcher) Watch(ctx context.Context) {
	if a == nil || a.watcherOptions == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		a.watcherOptions.Logger.Error("[AppConfigWatcher] context cancelled: %v", err)
		return
	}
	if err := a.validate(); err != nil {
		a.watcherOptions.Logger.Error("[AppConfigWatcher] invalid watcher options: %v", err)
		return
	}

	watchConfigInput := &appconfig.WatchConfigInput{
		UniqueConfigID: a.generateUniqueID(),
		Application:    a.watcherOptions.Application,
		Environment:    a.watcherOptions.Environment,
		ConfigProfile:  a.watcherOptions.ContentScope,
		PollInterval:   a.watcherOptions.PollInterval,
		ClientID:       a.watcherOptions.ClientID,
		Logger:         a.watcherOptions.Logger,
	}

	appConfigSvc, err := cloud.GetAppConfigService()
	if err != nil {
		a.watcherOptions.Logger.Error("[AppConfigWatcher] failed to initialize service: %v\n", err)
		return
	}

	err = appConfigSvc.WatchConfig(ctx, watchConfigInput, a.OnContentUpdate)

	if err != nil {
		a.watcherOptions.Logger.Error("[AppConfigWatcher] WatchConfig failed: %v\n", err)
		return
	}
}

func (a *AwsAppConfigWatcher) OnContentUpdate(result *appconfig.FetchConfigResult) {
	if a == nil || a.watcherOptions == nil {
		return
	}
	if result == nil {
		a.watcherOptions.Logger.Error("[AppConfigWatcher] received nil config result to update")
		return
	}

	if result.Content == "" {
		a.watcherOptions.Logger.Error("[AppConfigWatcher] received nil config file content from AWS appconfig")
		return
	}

	a.watcherOptions.Logger.Debug("[AppConfigWatcher] Pushing configuration update to updater channel")

	update := &UpdaterSchema{
		ContentSource: a.watcherOptions.ContentSource,
		ContentFormat: a.watcherOptions.ContentFormat,
		Content:       result.Content,
	}
	if a.watcherOptions.UpdateChannel != nil {
		if !sendUpdate(a.watcherOptions.UpdateChannel, update) {
			a.watcherOptions.Logger.Warn("[AppConfigWatcher] update channel full; dropping stale config update")
		}
		return
	}

	PushToContentUpdateChannelFn(update, a.watcherOptions.ContentSource)
}

func (a *AwsAppConfigWatcher) validate() error {
	if a.watcherOptions.Application == "" {
		return errors.New("application is required")
	}
	if a.watcherOptions.Environment == "" {
		return errors.New("environment is required")
	}
	if a.watcherOptions.ContentScope == "" {
		return errors.New("content scope is required")
	}
	if a.watcherOptions.ClientID == "" {
		return errors.New("client ID is required")
	}
	return nil
}

func defaultWatcherLogger() logger.Logger {
	return logger.Init(&logger.LoggerConfig{
		Name:     "photon-watcher",
		Provider: logger.LoggerProviderZerolog,
		Type:     logger.LoggerTypeStdout,
	})
}
