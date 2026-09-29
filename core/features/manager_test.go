package features

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/cshekharsharma/photon/coordination/network/watcher"
	"github.com/cshekharsharma/photon/core/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func rawFeatureOptions(content string) *InitOptions {
	return &InitOptions{
		SourceType: FeatureSourceRawBytes,
		Input:      content,
	}
}

func testFeatureLogger(name string) logger.Logger {
	return logger.Init(&logger.LoggerConfig{
		Name:     name,
		Provider: logger.LoggerProviderZerolog,
		Type:     logger.LoggerTypeStdout,
	})
}

func TestFeatureManagerLifecycle(t *testing.T) {
	var nilManager *Manager
	cfg, err := nilManager.GetFeatureConfigStore()
	require.Error(t, err)
	assert.Empty(t, cfg.Features)
	nilManager.Close()

	emptyManager := &Manager{}
	cfg, err = emptyManager.GetFeatureConfigStore()
	require.Error(t, err)
	assert.Empty(t, cfg.Features)
	emptyManager.Close()

	manager, err := NewManager(rawFeatureOptions(validConfig))
	require.NoError(t, err)

	cfg, err = manager.GetFeatureConfigStore()
	require.NoError(t, err)
	assert.Contains(t, cfg.Features, "featureA")

	manager.Close()
}

func TestFeatureManagerReload(t *testing.T) {
	manager, err := NewManager(rawFeatureOptions(validConfig))
	require.NoError(t, err)

	err = manager.reload(&watcher.UpdaterSchema{
		ContentSource: uint8(FeatureSourceRawBytes),
		Content:       `{"version":"1.0","attributes":{"canary":true,"platforms":["web"],"environments":["prod"],"regions":["us"]},"features":{}}`,
	})
	require.NoError(t, err)
	assert.Empty(t, manager.config.Features)

	err = manager.reload(&watcher.UpdaterSchema{
		ContentSource: uint8(FeatureSourceRawBytes),
		Content:       `{"invalid-json"`,
	})
	require.Error(t, err)

	err = (&Manager{}).reload(&watcher.UpdaterSchema{})
	require.Error(t, err)
}

func TestFeatureManagerWatcherSuccessAndClose(t *testing.T) {
	resetInitState()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	callback := make(chan struct{}, 1)
	opts := rawFeatureOptions(validConfig)
	opts.EnableWatch = true
	opts.WatcherOptions = &watcher.WatcherOptions{
		WatchContext: ctx,
		WatcherType:  watcher.WatcherTypeAwsAppConfig,
		Logger:       testFeatureLogger("manager-watch-success"),
		OnUpdateCallback: func() {
			callback <- struct{}{}
		},
	}

	manager, err := NewManager(opts)
	require.NoError(t, err)
	defer manager.Close()

	manager.updates <- &watcher.UpdaterSchema{
		ContentSource: uint8(FeatureSourceRawBytes),
		Content:       validConfig,
	}

	select {
	case <-callback:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for feature manager callback")
	}

	manager.Close()
	assert.Nil(t, manager.cancel)
}

func TestFeatureManagerStartWatcherDefaultsContext(t *testing.T) {
	opts := rawFeatureOptions(validConfig)
	opts.EnableWatch = true
	opts.WatcherOptions = &watcher.WatcherOptions{
		Logger: testFeatureLogger("manager-watch-default-context"),
	}

	manager, err := NewManager(opts)
	require.NoError(t, err)
	defer manager.Close()

	assert.NotNil(t, manager.cancel)
}

func TestFeatureManagerWatcherErrorClosedAndNilContext(t *testing.T) {
	options := rawFeatureOptions(validConfig)
	options.WatcherOptions = &watcher.WatcherOptions{Logger: testFeatureLogger("manager-watch-error")}

	manager := &Manager{
		options: options,
		updates: make(chan *watcher.UpdaterSchema, 2),
	}
	manager.updates <- &watcher.UpdaterSchema{
		ContentSource: uint8(FeatureSourceRawBytes),
		Content:       `{"invalid-json"`,
	}
	close(manager.updates)

	var nilCtx context.Context
	manager.watchUpdaterChannel(nilCtx)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	manager = &Manager{updates: make(chan *watcher.UpdaterSchema), options: options}
	manager.watchUpdaterChannel(ctx)
}

func TestLegacyFeatureWatcherNilContext(t *testing.T) {
	ch := make(chan *watcher.UpdaterSchema)
	close(ch)

	var nilCtx context.Context
	watchUpdaterChannel(nilCtx, &InitOptions{
		WatcherOptions: &watcher.WatcherOptions{
			UpdateChannel: ch,
			Logger:        testFeatureLogger("legacy-watch-nil-context"),
		},
	})
}

func TestFeatureManagerHelpers(t *testing.T) {
	assert.Nil(t, cloneInitOptions(nil))
	assert.EqualError(t, errFeatureManagerNotInitialized(), "feature config is not initialized")

	withWatcher := &InitOptions{
		SourceType: FeatureSourceRawBytes,
		Input:      validConfig,
		WatcherOptions: &watcher.WatcherOptions{
			UpdateChannel: make(chan *watcher.UpdaterSchema, 1),
		},
	}
	cloned := cloneInitOptions(withWatcher)
	require.NotNil(t, cloned)
	assert.NotSame(t, withWatcher.WatcherOptions, cloned.WatcherOptions)
	assert.Equal(t, withWatcher.WatcherOptions.UpdateChannel, cloned.WatcherOptions.UpdateChannel)

	assert.Error(t, (*InitOptions)(nil).IsValid())
	assert.Nil(t, populateRequiredOptionsProperties(nil))
	assert.EqualError(t, applyFeatureUpdateToOptions(nil, nil), "feature watcher options are required")

	options := rawFeatureOptions(validConfig)
	assert.NoError(t, applyFeatureUpdateToOptions(options, nil))

	callFeatureUpdateCallback(nil)
	callFeatureUpdateCallback(&InitOptions{})
	callFeatureUpdateCallback(&InitOptions{WatcherOptions: &watcher.WatcherOptions{}})

	globalCh := make(chan *watcher.UpdaterSchema, 1)
	previous := watcher.ContentUpdateChannel
	watcher.ContentUpdateChannel = globalCh
	defer func() {
		watcher.ContentUpdateChannel = previous
	}()
	var expectedGlobal <-chan *watcher.UpdaterSchema = globalCh
	assert.Equal(t, expectedGlobal, featureUpdateChannel(&InitOptions{}))
}

func TestApplyFeatureUpdateToOptionsFile(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "feature-manager-file-*.json")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, os.Remove(tmpFile.Name()))
	}()

	options := &InitOptions{
		SourceType: FeatureSourceFile,
		Input:      tmpFile.Name(),
	}

	err = applyFeatureUpdateToOptions(options, &watcher.UpdaterSchema{
		ContentSource: uint8(FeatureSourceFile),
		Content:       validConfig,
	})
	require.NoError(t, err)

	data, err := os.ReadFile(tmpFile.Name())
	require.NoError(t, err)
	assert.Equal(t, validConfig, string(data))
}
