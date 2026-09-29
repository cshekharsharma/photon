package config

import (
	"bytes"
	"context"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cshekharsharma/photon/coordination/network/watcher"
	"github.com/cshekharsharma/photon/core/logger"
	"github.com/knadh/koanf/parsers/json"
	"github.com/knadh/koanf/providers/rawbytes"
	"github.com/knadh/koanf/v2"
	"github.com/stretchr/testify/assert"
)

func getConsoleLogger(name string, buff io.Writer) logger.Logger {
	return logger.Init(&logger.LoggerConfig{
		Provider: logger.LoggerProviderZerolog,
		Name:     name,
		Level:    logger.LogLevelDebug,
		Type:     logger.LoggerTypeStdout,
		Writer:   buff,
	})
}

func TestAllConfig(t *testing.T) {
	jsonConfig := `{
        "bool": true,
        "int": 123,
        "float": 123.456,
        "string": "text",
        "time": "2020-01-01T12:00:00Z",
        "duration": "1h",
        "bools": [true, false],
        "ints": [1, 2, 3],
        "floats": [1.1, 2.2],
        "strings": ["a", "b"],
        "boolMap": {"key1": true, "key2": false},
        "intMap": {"key1": 1, "key2": 2},
        "floatMap": {"key1": 1.1, "key2": 2.2},
        "stringMap": {"key1": "a", "key2": "b"},
        "stringSliceMap": {"key1": ["a", "b"], "key2": ["c", "d"]}
    }`

	k := koanf.New(".")
	err := k.Load(rawbytes.Provider([]byte(jsonConfig)), json.Parser())
	assert.NoError(t, err)
	koanfWrapper := &Koanf{koanf: k}

	assert.NotNil(t, koanfWrapper.RawStore())
	assert.IsType(t, &koanf.Koanf{}, koanfWrapper.RawStore())

	assert.Equal(t, true, koanfWrapper.Get("bool").(bool))
	assert.Equal(t, "text", koanfWrapper.Get("string").(string))

	assert.Equal(t, true, koanfWrapper.GetBool("bool"))
	assert.Equal(t, 123, int(koanfWrapper.GetInt64("int")))
	assert.Equal(t, 123.456, koanfWrapper.GetFloat64("float"))
	assert.Equal(t, "text", koanfWrapper.GetString("string"))

	expectedTime, _ := time.Parse(time.RFC3339, "2020-01-01T12:00:00Z")
	assert.Equal(t, expectedTime, koanfWrapper.GetTime("time", time.RFC3339))
	assert.Equal(t, time.Hour, koanfWrapper.GetDuration("duration"))

	assert.Equal(t, []bool{true, false}, koanfWrapper.GetBoolSlice("bools"))
	assert.Equal(t, []int64{1, 2, 3}, koanfWrapper.GetInt64Slice("ints"))
	assert.Equal(t, []float64{1.1, 2.2}, koanfWrapper.GetFloat64Slice("floats"))
	assert.Equal(t, []string{"a", "b"}, koanfWrapper.GetStringSlice("strings"))

	assert.Equal(t, map[string]bool{"key1": true, "key2": false}, koanfWrapper.GetBoolMap("boolMap"))
	assert.Equal(t, map[string]int64{"key1": 1, "key2": 2}, koanfWrapper.GetInt64Map("intMap"))
	assert.Equal(t, map[string]float64{"key1": 1.1, "key2": 2.2}, koanfWrapper.GetFloat64Map("floatMap"))
	assert.Equal(t, map[string]string{"key1": "a", "key2": "b"}, koanfWrapper.GetStringMap("stringMap"))

	assert.Equal(t, map[string][]string{"key1": {"a", "b"}, "key2": {"c", "d"}}, koanfWrapper.GetStringSliceMap("stringSliceMap"))

	err = koanfWrapper.Set("newKey", "newValue")
	assert.NoError(t, err)
	assert.Equal(t, "newValue", koanfWrapper.GetString("newKey"))
}

func TestKoanfInit(t *testing.T) {
	content := `{"Name":"John"}`
	err := Init(ConfigProviderKoanf, &Options{
		Source:    SourceRawBytes,
		Format:    FormatJson,
		FilePath:  "",
		Content:   []byte(content),
		Delimiter: ".",
	})
	assert.NoError(t, err)
	assert.True(t, true, true)
}

func TestNewKoanf_ValidateError(t *testing.T) {
	cfg, err := newKoanf(nil)
	assert.Nil(t, cfg)
	assert.Error(t, err)
}

func TestLoadWithKoanf(t *testing.T) {
	content := `{"Name":"John"}`
	options := &Options{
		Source:    SourceRawBytes,
		Format:    FormatJson,
		FilePath:  "",
		Content:   []byte(content),
		Delimiter: ".",
	}

	cOptions = options
	cProvider = ConfigProviderKoanf

	res := Load()

	assert.True(t, true, true)
	assert.IsType(t, &Koanf{}, res)
}

func TestUnmarshal(t *testing.T) {
	jsonConfig := `{
		"Name": "john",
		"Age": 32,
		"IsMale": true
	}`

	k := koanf.New(".")
	err := k.Load(rawbytes.Provider([]byte(jsonConfig)), json.Parser())
	assert.NoError(t, err)
	koanfWrapper := &Koanf{koanf: k}

	unmarshalledStruct := &UnmarshalStruct{}
	newerr := koanfWrapper.Unmarshal("", &unmarshalledStruct)

	assert.Nil(t, newerr)
	assert.Equal(t, "john", unmarshalledStruct.Name)
	assert.Equal(t, int64(32), unmarshalledStruct.Age)
	assert.Equal(t, true, unmarshalledStruct.IsMale)
}

func TestKoanf_watchUpdaterChannel_FileSource(t *testing.T) {
	tempFile, err := os.CreateTemp("", "testconfig*.json")
	if err != nil {
		t.Fatalf("Failed to create temp config file: %v", err)
	}
	defer func() {
		assert.NoError(t, os.Remove(tempFile.Name()))
	}()

	content := `{"key": "value"}`
	var callbackCalled atomic.Bool

	k := &Koanf{
		opts: &Options{
			Source:    SourceFile,
			Format:    FormatJson,
			FilePath:  tempFile.Name(),
			Delimiter: ".",
			WatcherOptions: &watcher.WatcherOptions{
				OnUpdateCallback: func() {
					callbackCalled.Store(true)
				},
				Logger: getConsoleLogger("test", &bytes.Buffer{}),
			},
		},
	}

	ch := make(chan *watcher.UpdaterSchema, 1)
	watcher.ContentUpdateChannel = ch

	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		k.watchUpdaterChannel(context.Background())
	}()

	ch <- &watcher.UpdaterSchema{
		ContentSource: uint8(SourceFile),
		Content:       content,
	}

	time.Sleep(1 * time.Second)

	data, err := os.ReadFile(tempFile.Name())
	if err != nil {
		t.Fatalf("Failed to read updated config file: %v", err)
	}

	if !strings.Contains(string(data), `"key": "value"`) {
		t.Errorf("Config file not updated. Got: %s", data)
	}

	if !callbackCalled.Load() {
		t.Error("Expected OnUpdateCallback to be called")
	}

	close(ch)
	wg.Wait()
}

func TestKoanf_watchUpdaterChannel_RawBytesSource(t *testing.T) {
	var callbackCalled atomic.Bool

	k := &Koanf{
		opts: &Options{
			Source:    SourceRawBytes,
			Format:    FormatJson,
			Content:   []byte(`{"from":"bytes"}`),
			Delimiter: ".",
			WatcherOptions: &watcher.WatcherOptions{
				OnUpdateCallback: func() {
					callbackCalled.Store(true)
				},
				Logger: getConsoleLogger("test", &bytes.Buffer{}),
			},
		},
	}

	ch := make(chan *watcher.UpdaterSchema, 1)
	watcher.ContentUpdateChannel = ch

	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		k.watchUpdaterChannel(context.Background())
	}()

	ch <- &watcher.UpdaterSchema{
		ContentSource: uint8(SourceRawBytes),
		Content:       `{"from": "bytes"}`,
	}

	time.Sleep(500 * time.Millisecond)

	if !callbackCalled.Load() {
		t.Error("Expected OnUpdateCallback to be called for raw bytes")
	}

	close(ch)
	wg.Wait()
}

func TestKoanf_watchUpdaterChannel_PanicRecovery(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatal("watchUpdaterChannel should have recovered internally, but it propagated panic")
		}
	}()

	k := &Koanf{
		opts: &Options{
			WatcherOptions: &watcher.WatcherOptions{},
		},
	}

	ch := make(chan *watcher.UpdaterSchema, 1)
	watcher.ContentUpdateChannel = ch

	close(ch)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		k.watchUpdaterChannel(context.Background())
	}()
	wg.Wait()
}

func TestNewKoanf_FileLoadError_ReturnsError(t *testing.T) {
	tempFile, err := os.CreateTemp("", "invalid-config*.json")
	assert.NoError(t, err)
	defer func() {
		assert.NoError(t, os.Remove(tempFile.Name()))
	}()
	_, err = tempFile.WriteString(`{"invalid-json"`)
	assert.NoError(t, err)
	assert.NoError(t, tempFile.Close())

	_, err = newKoanf(&Options{
		Source:    SourceFile,
		Format:    FormatJson,
		FilePath:  tempFile.Name(),
		Delimiter: ".",
	})

	assert.Error(t, err)
}

func TestNewKoanf_RawBytesLoadError_ReturnsError(t *testing.T) {
	_, err := newKoanf(&Options{
		Source:    SourceRawBytes,
		Format:    FormatJson,
		Content:   []byte(`{"invalid-json"`),
		Delimiter: ".",
	})

	assert.Error(t, err)
}

func TestUnmarshal_Error(t *testing.T) {
	jsonConfig := `{"Name":"john"}`
	k := koanf.New(".")
	err := k.Load(rawbytes.Provider([]byte(jsonConfig)), json.Parser())
	assert.NoError(t, err)
	koanfWrapper := &Koanf{koanf: k}

	// Non-pointer destination should trigger unmarshal error path.
	dest := UnmarshalStruct{}
	err = koanfWrapper.Unmarshal("", dest)
	assert.Error(t, err)
}

func TestKoanf_watchUpdaterChannel_RecoversAndRestarts(t *testing.T) {
	watchUpdaterRestartMu.Lock()
	orig := watchUpdaterRestartHook
	watchUpdaterRestartMu.Unlock()
	defer func() {
		watchUpdaterRestartMu.Lock()
		watchUpdaterRestartHook = orig
		watchUpdaterRestartMu.Unlock()
	}()

	var restarted atomic.Bool
	watchUpdaterRestartMu.Lock()
	watchUpdaterRestartHook = func(k *Koanf) {
		restarted.Store(true)
	}
	watchUpdaterRestartMu.Unlock()

	k := &Koanf{
		opts: &Options{
			Source:    SourceRawBytes,
			Format:    FormatJson,
			Content:   []byte(`{"from":"panic-case"}`),
			Delimiter: ".",
			WatcherOptions: &watcher.WatcherOptions{
				OnUpdateCallback: func() {
					panic("callback panic")
				},
				Logger: getConsoleLogger("panic-recovery", &bytes.Buffer{}),
			},
		},
	}

	ch := make(chan *watcher.UpdaterSchema, 1)
	watcher.ContentUpdateChannel = ch
	ch <- &watcher.UpdaterSchema{
		ContentSource: uint8(SourceRawBytes),
		Content:       `{"from":"panic-case"}`,
	}
	close(ch)

	k.watchUpdaterChannel(context.Background())
	assert.True(t, restarted.Load())
}

func TestKoanf_watchUpdaterChannel_ReloadError(t *testing.T) {
	var callbackCalled atomic.Bool
	k := &Koanf{
		opts: &Options{
			Source:    SourceRawBytes,
			Format:    FormatJson,
			Content:   []byte(`{"invalid-json"`),
			Delimiter: ".",
			WatcherOptions: &watcher.WatcherOptions{
				OnUpdateCallback: func() {
					callbackCalled.Store(true)
				},
				Logger: getConsoleLogger("reload-error", &bytes.Buffer{}),
			},
		},
	}

	ch := make(chan *watcher.UpdaterSchema, 1)
	watcher.ContentUpdateChannel = ch
	ch <- &watcher.UpdaterSchema{
		ContentSource: uint8(SourceRawBytes),
		Content:       `{"invalid-json"`,
	}
	close(ch)

	k.watchUpdaterChannel(context.Background())
	assert.False(t, callbackCalled.Load())
}

func TestKoanf_watchUpdaterChannel_NilContext(t *testing.T) {
	k := &Koanf{
		opts: &Options{
			WatcherOptions: &watcher.WatcherOptions{
				Logger: getConsoleLogger("nil-context", &bytes.Buffer{}),
			},
		},
	}

	ch := make(chan *watcher.UpdaterSchema)
	watcher.ContentUpdateChannel = ch
	close(ch)

	var nilCtx context.Context
	assert.NotPanics(t, func() {
		k.watchUpdaterChannel(nilCtx)
	})
}

func TestKoanf_watchUpdaterChannel_FileWriteError(t *testing.T) {
	blocker, err := os.CreateTemp("", "koanf-blocker-*")
	assert.NoError(t, err)
	assert.NoError(t, blocker.Close())
	defer func() {
		assert.NoError(t, os.Remove(blocker.Name()))
	}()

	var callbackCalled atomic.Bool
	k := &Koanf{
		opts: &Options{
			Source:    SourceFile,
			Format:    FormatJson,
			FilePath:  blocker.Name() + "/config.json",
			Delimiter: ".",
			WatcherOptions: &watcher.WatcherOptions{
				OnUpdateCallback: func() {
					callbackCalled.Store(true)
				},
				Logger: getConsoleLogger("write-error", &bytes.Buffer{}),
			},
		},
	}

	ch := make(chan *watcher.UpdaterSchema, 1)
	watcher.ContentUpdateChannel = ch
	ch <- &watcher.UpdaterSchema{
		ContentSource: uint8(SourceFile),
		Content:       `{"key":"value"}`,
	}
	close(ch)

	k.watchUpdaterChannel(context.Background())
	assert.False(t, callbackCalled.Load())
}

func TestKoanf_watchUpdaterChannel_Recovers_DefaultRestartPath(t *testing.T) {
	watchUpdaterRestartMu.Lock()
	origHook := watchUpdaterRestartHook
	watchUpdaterRestartMu.Unlock()
	defer func() {
		watchUpdaterRestartMu.Lock()
		watchUpdaterRestartHook = origHook
		watchUpdaterRestartMu.Unlock()
	}()
	watchUpdaterRestartMu.Lock()
	watchUpdaterRestartHook = nil
	watchUpdaterRestartMu.Unlock()

	k := &Koanf{
		opts: &Options{
			Source:    SourceRawBytes,
			Format:    FormatJson,
			Content:   []byte(`{"from":"panic-case"}`),
			Delimiter: ".",
			WatcherOptions: &watcher.WatcherOptions{
				OnUpdateCallback: func() {
					panic("callback panic")
				},
				Logger: getConsoleLogger("panic-recovery-default", &bytes.Buffer{}),
			},
		},
	}

	ch := make(chan *watcher.UpdaterSchema, 1)
	watcher.ContentUpdateChannel = ch
	ch <- &watcher.UpdaterSchema{
		ContentSource: uint8(SourceRawBytes),
		Content:       `{"from":"panic-default-path"}`,
	}
	close(ch)

	assert.NotPanics(t, func() {
		k.watchUpdaterChannel(context.Background())
	})
}

type UnmarshalStruct struct {
	Name   string
	Age    int64
	IsMale bool
}
