package features

import (
	"context"
	"fmt"
	"time"

	"github.com/cshekharsharma/photon/coordination/network/watcher"
	"github.com/cshekharsharma/photon/utils/filesys"
)

var watcherLoopSleepTime = 1 * time.Second

// watchUpdaterChannel listens to the updater channel for feature updates and processes them.
// It handles updates from different content sources, writes the updated content to the file system,
// reinitializes features, and invokes an optional update callback if provided.
// In case of a panic, the function recovers and restarts itself to ensure continuous operation.
//
// Parameters:
//   - options: Pointer to InitOptions containing configuration and dependencies for the watcher.
func watchUpdaterChannel(ctx context.Context, options *InitOptions) {
	if ctx == nil {
		ctx = context.Background()
	}
	defer func() {
		if r := recover(); r != nil {
			options.WatcherOptions.Logger.Error("[FeatureWatch] Error in watchUpdaterChannel: %v", r)
			if ctx.Err() == nil {
				watchUpdaterChannel(ctx, options) // Restart the watcher in case of panic
			}
		}
	}()

	ch := featureUpdateChannel(options)
	for {
		select {
		case <-ctx.Done():
			return
		case entry, ok := <-ch:
			if !ok {
				return
			}

			if err := applyFeatureUpdateToOptions(options, entry); err != nil {
				options.WatcherOptions.Logger.Error("[FeatureWatch] Error writing feature update: %v", err)
				continue
			}

			cfg, populatedOpts, err := loadFeatureConfig(options)
			if err != nil {
				options.WatcherOptions.Logger.Error("[FeatureWatch] Error reloading features: %v", err)
				continue
			}
			options = populatedOpts
			updateDefaultFeatureStore(cfg)

			callFeatureUpdateCallback(options)

			options.WatcherOptions.Logger.Info("[FeatureWatch] Features updated successfully through updater channel: "+
				"BytesWritten=%d", len(entry.Content))

			time.Sleep(watcherLoopSleepTime) // Sleep to avoid tight loop
		}
	}
}

func applyFeatureUpdateToOptions(options *InitOptions, entry *watcher.UpdaterSchema) error {
	if options == nil {
		return fmt.Errorf("feature watcher options are required")
	}
	if entry == nil {
		return nil
	}

	switch entry.ContentSource {
	case uint8(FeatureSourceFile):
		if err := filesys.WriteFile(options.Input, []byte(entry.Content), 0644); err != nil {
			return err
		}
	case uint8(FeatureSourceRawBytes):
		options.Input = entry.Content
	}

	return nil
}

func updateDefaultFeatureStore(cfg *FeatureConfig) {
	mutex.Lock()
	defer mutex.Unlock()

	configCache = cfg
	areFeaturesInitialized = true
}

func callFeatureUpdateCallback(options *InitOptions) {
	if options == nil || options.WatcherOptions == nil || options.WatcherOptions.OnUpdateCallback == nil {
		return
	}
	options.WatcherOptions.OnUpdateCallback()
}

func featureUpdateChannel(options *InitOptions) <-chan *watcher.UpdaterSchema {
	if options != nil && options.WatcherOptions != nil && options.WatcherOptions.UpdateChannel != nil {
		return options.WatcherOptions.UpdateChannel
	}
	return watcher.ContentUpdateChannel
}
