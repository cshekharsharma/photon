package features

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/cshekharsharma/photon/coordination/network/watcher"
	"github.com/cshekharsharma/photon/core/logger"
	"github.com/stretchr/testify/assert"
)

func Test_watchUpdaterChannel_FileWriteAndReload(t *testing.T) {
	watcherLoopSleepTime = 100 * time.Millisecond

	tmpFile, err := os.CreateTemp("", "feature-config-*.json")
	assert.NoError(t, err)
	defer func() {
		assert.NoError(t, os.Remove(tmpFile.Name()))
	}()

	ch := make(chan *watcher.UpdaterSchema, 1)
	ctx, cancel := context.WithCancel(context.Background())

	expectedContent := `{"version":"1.0","attributes":{"canary":true,"platforms":["web"],"environments":["prod"],"regions":["us"]},"features":{}}`

	callbackCh := make(chan string, 1)
	opts := &InitOptions{
		EnableWatch: true,
		Input:       tmpFile.Name(),
		SourceType:  FeatureSourceFile,
		WatcherOptions: &watcher.WatcherOptions{
			WatchContext:  ctx,
			Logger:        logger.Init(&logger.LoggerConfig{Name: "test", Type: logger.LoggerTypeStdout}),
			UpdateChannel: ch,
			OnUpdateCallback: func() {
				dir := t.TempDir()
				tempFile, err := os.CreateTemp(dir, "callback-invoked-*.tmp")
				if err != nil {
					t.Fatalf("failed to create temp file: %v", err)
				}
				callbackCh <- tempFile.Name()
				defer func() {
					assert.NoError(t, tempFile.Close())
				}()
			},
		},
	}

	ch <- &watcher.UpdaterSchema{
		ContentSource: uint8(FeatureSourceFile),
		Content:       expectedContent,
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		watchUpdaterChannel(ctx, opts)
	}()
	defer func() {
		cancel()
		wg.Wait()
	}()

	var tempfilepath string
	select {
	case tempfilepath = <-callbackCh:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for update callback")
	}
	cancel()

	actualContentBytes, err := os.ReadFile(tmpFile.Name())
	assert.NoError(t, err)
	assert.Equal(t, expectedContent, string(actualContentBytes))
	assert.FileExists(t, tempfilepath, "Expected temp file to be created by callback")
}

func Test_watchUpdaterChannel_RawBytesWorkflow(t *testing.T) {
	watcherLoopSleepTime = 100 * time.Millisecond

	ch := make(chan *watcher.UpdaterSchema, 1)
	ctx, cancel := context.WithCancel(context.Background())

	callbackCh := make(chan string, 1)
	opts := &InitOptions{
		EnableWatch: true,
		Input:       validConfig,
		SourceType:  FeatureSourceRawBytes,
		WatcherOptions: &watcher.WatcherOptions{
			WatchContext:  ctx,
			Logger:        logger.Init(&logger.LoggerConfig{Name: "test", Type: logger.LoggerTypeStdout}),
			UpdateChannel: ch,
			OnUpdateCallback: func() {
				dir := t.TempDir()
				tempFile, err := os.CreateTemp(dir, "callback-invoked-rawbytes-*.tmp")
				if err != nil {
					t.Fatalf("failed to create temp file: %v", err)
				}
				callbackCh <- tempFile.Name()
				defer func() {
					assert.NoError(t, tempFile.Close())
				}()
			},
		},
	}

	ch <- &watcher.UpdaterSchema{
		ContentSource: uint8(FeatureSourceRawBytes),
		Content:       validConfig,
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		watchUpdaterChannel(ctx, opts)
	}()
	defer func() {
		cancel()
		wg.Wait()
	}()

	var tempfilepath string
	select {
	case tempfilepath = <-callbackCh:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for update callback")
	}
	cancel()

	assert.FileExists(t, tempfilepath, "Expected temp file to be created by callback")
	cfg, err := GetFeatureConfigStore()
	assert.NoError(t, err)
	assert.Contains(t, cfg.Features, "featureA")
}

func Test_watchUpdaterChannel_RecoveryBlock(t *testing.T) {
	watcherLoopSleepTime = 100 * time.Millisecond

	ch := make(chan *watcher.UpdaterSchema, 1)
	ctx, cancel := context.WithCancel(context.Background())

	opts := &InitOptions{
		EnableWatch: true,
		Input:       "irrelevant",
		SourceType:  FeatureSourceRawBytes,
		WatcherOptions: &watcher.WatcherOptions{
			WatchContext:  ctx,
			Logger:        logger.Init(&logger.LoggerConfig{Name: "recovery-test", Type: logger.LoggerTypeStdout}),
			UpdateChannel: ch,
			OnUpdateCallback: func() {
				panic("simulated panic from callback")
			},
		},
	}

	ch <- &watcher.UpdaterSchema{
		ContentSource: uint8(FeatureSourceRawBytes),
		Content: `{
			"version": "1.0",
			"attributes": {
				"canary": true,
				"platforms": ["web"],
				"environments": ["prod"],
				"regions": ["us"]
			},
			"features": {}
		}`,
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		watchUpdaterChannel(ctx, opts)
	}()
	defer func() {
		cancel()
		wg.Wait()
	}()

	time.Sleep(300 * time.Millisecond) // Let the panic happen and recovery restart
	cancel()
}

func Test_watchUpdaterChannel_FileWriteError(t *testing.T) {
	watcherLoopSleepTime = time.Millisecond

	blocker, err := os.CreateTemp("", "feature-blocker-*")
	assert.NoError(t, err)
	assert.NoError(t, blocker.Close())
	defer func() {
		assert.NoError(t, os.Remove(blocker.Name()))
	}()

	ch := make(chan *watcher.UpdaterSchema, 1)

	callbackCalled := false
	opts := &InitOptions{
		Input:      blocker.Name() + "/feature.json",
		SourceType: FeatureSourceFile,
		WatcherOptions: &watcher.WatcherOptions{
			WatchContext: context.Background(),
			Logger: logger.Init(&logger.LoggerConfig{
				Name:     "write-error",
				Provider: logger.LoggerProviderZerolog,
				Type:     logger.LoggerTypeStdout,
			}),
			UpdateChannel: ch,
			OnUpdateCallback: func() {
				callbackCalled = true
			},
		},
	}

	ch <- &watcher.UpdaterSchema{
		ContentSource: uint8(FeatureSourceFile),
		Content:       validConfig,
	}
	close(ch)

	watchUpdaterChannel(context.Background(), opts)
	assert.False(t, callbackCalled)
}

func Test_watchUpdaterChannel_InitError(t *testing.T) {
	watcherLoopSleepTime = time.Millisecond

	ch := make(chan *watcher.UpdaterSchema, 1)

	callbackCalled := false
	opts := &InitOptions{
		Input:      `{"invalid-json"`,
		SourceType: FeatureSourceRawBytes,
		WatcherOptions: &watcher.WatcherOptions{
			WatchContext: context.Background(),
			Logger: logger.Init(&logger.LoggerConfig{
				Name:     "init-error",
				Provider: logger.LoggerProviderZerolog,
				Type:     logger.LoggerTypeStdout,
			}),
			UpdateChannel: ch,
			OnUpdateCallback: func() {
				callbackCalled = true
			},
		},
	}

	ch <- &watcher.UpdaterSchema{
		ContentSource: uint8(FeatureSourceRawBytes),
		Content:       `{"invalid-json"`,
	}
	close(ch)

	watchUpdaterChannel(context.Background(), opts)
	assert.False(t, callbackCalled)
}
