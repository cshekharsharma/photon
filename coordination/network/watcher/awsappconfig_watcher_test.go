package watcher

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cshekharsharma/photon/cloud"
	"github.com/cshekharsharma/photon/cloud/contract"
	"github.com/cshekharsharma/photon/cloud/entity/appconfig"
	"github.com/cshekharsharma/photon/core/logger"
)

type mockAppConfigService struct {
	watchFn func(context.Context, *appconfig.WatchConfigInput, func(*appconfig.FetchConfigResult)) error
}

func (m *mockAppConfigService) WatchConfig(ctx context.Context, input *appconfig.WatchConfigInput, onUpdate func(*appconfig.FetchConfigResult)) error {
	return m.watchFn(ctx, input, onUpdate)
}

func (m *mockAppConfigService) FetchConfig(ctx context.Context, input *appconfig.FetchConfigInput) (*appconfig.FetchConfigResult, error) {
	return nil, nil
}

func (m *mockAppConfigService) GetCurrentConfiguration(uniqueConfigId string) (*appconfig.FetchConfigResult, error) {
	return nil, nil
}

func (m *mockAppConfigService) StopWatching(id string) error { return nil }
func (m *mockAppConfigService) StopAllWatching()             {}

func getConsoleLogger(name string, buff io.Writer) logger.Logger {
	return logger.Init(&logger.LoggerConfig{
		Provider: logger.LoggerProviderZerolog,
		Name:     name,
		Level:    logger.LogLevelDebug,
		Type:     logger.LoggerTypeStdout,
		Writer:   buff,
	})
}

// testcases
var (
	originalGetter                     = cloud.GetAppConfigService
	mockAppConfigServiceInstance       *mockAppConfigService
	pushCalled                         bool
	pushMutex                          sync.Mutex
	pushedConfig                       *UpdaterSchema
	pushedSource                       uint8
	originalPushToContentUpdateChannel = PushToContentUpdateChannelFn
)

func setup(t *testing.T) {
	t.Helper()
	mockAppConfigServiceInstance = &mockAppConfigService{}
	cloud.AppConfigServiceProvider = func() (contract.AppConfigInterface, error) {
		return mockAppConfigServiceInstance, nil
	}
	PushToContentUpdateChannelFn = func(schema *UpdaterSchema, source uint8) {
		pushMutex.Lock()
		defer pushMutex.Unlock()
		pushCalled = true
		pushedConfig = schema
		pushedSource = source
	}
	pushCalled = false
	pushedConfig = nil
	pushedSource = 0
}

func teardown() {
	cloud.AppConfigServiceProvider = originalGetter
	PushToContentUpdateChannelFn = originalPushToContentUpdateChannel
}

// --- Tests ---

func TestWatch_SuccessfulUpdate(t *testing.T) {
	setup(t)
	defer teardown()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockAppConfigServiceInstance.watchFn = func(ctx context.Context, input *appconfig.WatchConfigInput, cb func(*appconfig.FetchConfigResult)) error {
		cb(&appconfig.FetchConfigResult{
			Content:       "mock-config",
			ConfigProfile: "test-profile",
			ContentType:   "application/json",
			ConfigID:      "test-id",
		})
		return nil
	}

	w := NewAwsAppConfigWatcher(&WatcherOptions{
		Application:   "myApp",
		Environment:   "dev",
		ContentScope:  "profile",
		ClientID:      "client123",
		PollInterval:  time.Second,
		ContentSource: 1, // file
		ContentFormat: 1, // json
		Logger:        getConsoleLogger("SuccessfulUpdate", &bytes.Buffer{}),
	}, "file:///tmp/config.json")

	w.Watch(ctx)

	pushMutex.Lock()
	defer pushMutex.Unlock()
	if !pushCalled {
		t.Fatal("Expected PushToConfigUpdateChannelFn to be called")
	}
	if pushedConfig.Content != "mock-config" {
		t.Errorf("Expected config content 'mock-config', got '%s'", pushedConfig.Content)
	}
}

func TestNewAwsAppConfigWatcher_Defaults(t *testing.T) {
	w := NewAwsAppConfigWatcher(nil, "file:///tmp/config.json")
	if w == nil || w.watcherOptions == nil {
		t.Fatal("expected watcher with default options")
	}
	if w.watcherOptions.Logger == nil {
		t.Fatal("expected default logger")
	}

	w = NewAwsAppConfigWatcher(&WatcherOptions{}, "file:///tmp/config.json")
	if w.watcherOptions.Logger == nil {
		t.Fatal("expected nil logger to be defaulted")
	}
}

func TestWatch_GuardRails(t *testing.T) {
	setup(t)
	defer teardown()

	var nilWatcher *AwsAppConfigWatcher
	nilWatcher.Watch(context.Background())

	buff := &bytes.Buffer{}
	w := NewAwsAppConfigWatcher(&WatcherOptions{
		Logger: getConsoleLogger("InvalidOptions", buff),
	}, "file:///tmp/config.json")
	w.Watch(context.Background())
	if !strings.Contains(buff.String(), "invalid watcher options") {
		t.Fatalf("expected invalid options log, got: %s", buff.String())
	}

	buff.Reset()
	w = NewAwsAppConfigWatcher(&WatcherOptions{
		Application:  "app",
		Environment:  "dev",
		ContentScope: "profile",
		ClientID:     "client",
		Logger:       getConsoleLogger("CanceledContext", buff),
	}, "file:///tmp/config.json")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	w.Watch(ctx)
	if !strings.Contains(buff.String(), "context cancelled") {
		t.Fatalf("expected canceled context log, got: %s", buff.String())
	}

	var gotCtx context.Context
	mockAppConfigServiceInstance.watchFn = func(ctx context.Context, input *appconfig.WatchConfigInput, cb func(*appconfig.FetchConfigResult)) error {
		gotCtx = ctx
		return nil
	}
	var nilCtx context.Context
	w.Watch(nilCtx)
	if gotCtx == nil {
		t.Fatal("expected nil context to be normalized")
	}
}

func TestWatch_ServiceError(t *testing.T) {
	defer teardown()
	called := false

	cloud.AppConfigServiceProvider = func() (contract.AppConfigInterface, error) {
		called = true
		return nil, errors.New("failed to init service")
	}

	w := NewAwsAppConfigWatcher(&WatcherOptions{
		Application:   "myApp",
		Environment:   "dev",
		ContentScope:  "profile",
		ClientID:      "client123",
		PollInterval:  time.Second,
		ContentSource: 1, // file
		ContentFormat: 1, // json
		Logger:        getConsoleLogger("ServiceError", &bytes.Buffer{}),
	}, "file:///tmp/config.json")

	w.Watch(context.Background())

	if !called {
		t.Error("Expected GetAppConfigService to be called")
	}
}

func TestWatch_WatchConfigError(t *testing.T) {
	setup(t)
	defer teardown()

	buff := &bytes.Buffer{}
	mockAppConfigServiceInstance.watchFn = func(ctx context.Context, input *appconfig.WatchConfigInput, cb func(*appconfig.FetchConfigResult)) error {
		return errors.New("watch failed")
	}

	w := NewAwsAppConfigWatcher(&WatcherOptions{
		Application:   "myApp",
		Environment:   "dev",
		ContentScope:  "profile",
		ClientID:      "client123",
		PollInterval:  time.Second,
		ContentSource: 1,
		ContentFormat: 1,
		Logger:        getConsoleLogger("WatchConfigError", buff),
	}, "file:///tmp/config.json")

	w.Watch(context.Background())

	if !strings.Contains(buff.String(), "WatchConfig failed") {
		t.Errorf("expected WatchConfig error to be logged, got: %s", buff.String())
	}
}

func TestOnContentUpdate_NilResult(t *testing.T) {
	setup(t)
	defer teardown()

	buff := &bytes.Buffer{}
	w := NewAwsAppConfigWatcher(&WatcherOptions{
		ContentSource: 1,
		ContentFormat: 1,
		Logger:        getConsoleLogger("NilResult", buff),
	}, "file:///tmp/config.json")

	w.OnContentUpdate(nil)

	if !strings.Contains(buff.String(), "received nil config result to update") {
		t.Errorf("Expected log to contain warning about nil config, got: %s", buff.String())
	}
}

func TestOnContentUpdate_NilWatcher(t *testing.T) {
	var nilWatcher *AwsAppConfigWatcher
	nilWatcher.OnContentUpdate(&appconfig.FetchConfigResult{Content: "ignored"})

	w := &AwsAppConfigWatcher{}
	w.OnContentUpdate(&appconfig.FetchConfigResult{Content: "ignored"})
}

func TestOnContentUpdate_EmptyContent(t *testing.T) {
	setup(t)
	defer teardown()

	buff := &bytes.Buffer{}
	w := NewAwsAppConfigWatcher(&WatcherOptions{
		ContentSource: 1, // file
		ContentFormat: 1, // json
		Logger:        getConsoleLogger("EmptyContent", buff),
	}, "file:///tmp/config.json")

	w.OnContentUpdate(&appconfig.FetchConfigResult{
		Content: "",
	})

	if !strings.Contains(buff.String(), "received nil config file content from AWS appconfig") {
		t.Errorf("Expected log for nil config data, got: %s", buff.String())
	}
}

func TestOnContentUpdate_UsesOwnedUpdateChannel(t *testing.T) {
	setup(t)
	defer teardown()

	updateCh := make(chan *UpdaterSchema, 1)
	w := NewAwsAppConfigWatcher(&WatcherOptions{
		ContentSource: 7,
		ContentFormat: 8,
		UpdateChannel: updateCh,
		Logger:        getConsoleLogger("OwnedUpdateChannel", &bytes.Buffer{}),
	}, "file:///tmp/config.json")

	w.OnContentUpdate(&appconfig.FetchConfigResult{Content: "owned-config"})

	select {
	case update := <-updateCh:
		if update.Content != "owned-config" || update.ContentSource != 7 || update.ContentFormat != 8 {
			t.Fatalf("unexpected update: %#v", update)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for owned update")
	}

	pushMutex.Lock()
	defer pushMutex.Unlock()
	if pushCalled {
		t.Fatal("expected global update channel not to be used")
	}
}

func TestOnContentUpdate_FullOwnedUpdateChannelDoesNotBlock(t *testing.T) {
	setup(t)
	defer teardown()

	buff := &bytes.Buffer{}
	updateCh := make(chan *UpdaterSchema, 1)
	updateCh <- &UpdaterSchema{Content: "old"}
	w := NewAwsAppConfigWatcher(&WatcherOptions{
		ContentSource: 7,
		ContentFormat: 8,
		UpdateChannel: updateCh,
		Logger:        getConsoleLogger("FullUpdateChannel", buff),
	}, "file:///tmp/config.json")

	done := make(chan struct{})
	go func() {
		defer close(done)
		w.OnContentUpdate(&appconfig.FetchConfigResult{Content: "new"})
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("OnContentUpdate blocked on full channel")
	}
	if !strings.Contains(buff.String(), "update channel full") {
		t.Fatalf("expected full channel warning, got: %s", buff.String())
	}
}

func TestAwsAppConfigWatcherValidate(t *testing.T) {
	tests := []struct {
		name string
		opts WatcherOptions
		want string
	}{
		{name: "missing application", opts: WatcherOptions{}, want: "application is required"},
		{name: "missing environment", opts: WatcherOptions{Application: "app"}, want: "environment is required"},
		{name: "missing content scope", opts: WatcherOptions{Application: "app", Environment: "dev"}, want: "content scope is required"},
		{name: "missing client id", opts: WatcherOptions{Application: "app", Environment: "dev", ContentScope: "profile"}, want: "client ID is required"},
		{name: "valid", opts: WatcherOptions{Application: "app", Environment: "dev", ContentScope: "profile", ClientID: "client"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := NewAwsAppConfigWatcher(&tt.opts, "file:///tmp/config.json")
			err := w.validate()
			if tt.want == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || err.Error() != tt.want {
				t.Fatalf("expected %q, got %v", tt.want, err)
			}
		})
	}
}
