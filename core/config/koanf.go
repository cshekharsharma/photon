package config

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/cshekharsharma/photon/coordination/network/watcher"
	"github.com/cshekharsharma/photon/utils/filesys"
	"github.com/knadh/koanf/parsers/json"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/providers/rawbytes"
	"github.com/knadh/koanf/v2"
)

var (
	koanfFatalfHook         = log.Fatalf
	watchUpdaterRestartHook func(*Koanf)
	watchUpdaterRestartMu   sync.RWMutex
)

// Koanf wraps the koanf.Koanf struct to provide additional methods for config management.
type Koanf struct {
	opts     *Options
	koanf    *koanf.Koanf
	updates  <-chan *watcher.UpdaterSchema
	onReload func(Config)
}

// newKoanf initializes a new Koanf instance based on the provided Options.
// It supports loading configurations from files and raw bytes in JSON format.
//
// Parameters:
// - options: A pointer to Options struct that defines the source,
// format, and other options for the configuration.
//
// Returns:
// - *Koanf: A pointer to the initialized Koanf instance.
//
// The function will log a fatal error if it fails to load the configuration.
func newKoanf(options *Options) (*Koanf, error) {
	if err := options.Validate(); err != nil {
		return nil, err
	}

	var k = koanf.New(options.Delimiter)
	var parser koanf.Parser

	if options.Format == FormatJson {
		parser = json.Parser()
	}

	if options.Source == SourceFile {
		if err := k.Load(file.Provider(options.FilePath), parser); err != nil {
			return nil, fmt.Errorf("error loading config: %w", err)
		}
	}

	if options.Source == SourceRawBytes {
		if err := k.Load(rawbytes.Provider([]byte(options.Content)), parser); err != nil {
			return nil, fmt.Errorf("error loading config: %w", err)
		}
	}

	return &Koanf{
		koanf: k,
		opts:  options,
	}, nil
}

// RawStore returns the underlying koanf.Koanf instance.
func (k *Koanf) RawStore() interface{} {
	return k.koanf
}

// Unmarshal unmarshals the configuration data into the given struct
// based on the specified path.
//
// Parameters:
// - path: A string specifying the path in the configuration data.
// - cfg: A pointer to the struct where the configuration data should be unmarshalled.
//
// Returns:
// - error: An error if the unmarshalling fails, otherwise nil.
func (k *Koanf) Unmarshal(path string, cfg interface{}) error {
	err := k.koanf.Unmarshal(path, cfg)
	if err != nil {
		return err
	}
	return nil
}

// Get retrieves the value associated with the given key in the configuration data.
//
// Parameters:
// - key: A string specifying the key in the configuration data.
//
// Returns:
// - interface{}: The value associated with the key.
func (k *Koanf) Get(key string) interface{} {
	return k.koanf.Get(key)
}

// Set sets the value for the given key in the configuration data.
//
// Parameters:
// - key: A string specifying the key in the configuration data.
// - value: The value to be set.
//
// Returns:
// - error: An error if setting the value fails, otherwise nil.
func (k *Koanf) Set(key string, value interface{}) error {
	return k.koanf.Set(key, value)
}

// GetBool retrieves the boolean value associated with the
// given key in the configuration data.
func (k *Koanf) GetBool(key string) bool {
	return k.koanf.Bool(key)
}

// GetInt64 retrieves the int64 value associated with the
// given key in the configuration data.
func (k *Koanf) GetInt64(key string) int64 {
	return k.koanf.Int64(key)
}

// GetFloat64 retrieves the float64 value associated with the
// given key in the configuration data.
func (k *Koanf) GetFloat64(key string) float64 {
	return k.koanf.Float64(key)
}

// GetString retrieves the string value associated with the
// given key in the configuration data.
func (k *Koanf) GetString(key string) string {
	return k.koanf.String(key)
}

// GetTime retrieves the time.Time value associated with the given key
// in the configuration data.
//
// Parameters:
// - key: A string specifying the key in the configuration data.
// - layout: The layout to parse the time value.
//
// Returns:
// - time.Time: The parsed time value.
func (k *Koanf) GetTime(key string, layout string) time.Time {
	return k.koanf.Time(key, layout)
}

// GetDuration retrieves the time.Duration value associated with
// the given key in the configuration data.
func (k *Koanf) GetDuration(key string) time.Duration {
	return k.koanf.Duration(key)
}

// GetBoolSlice retrieves the slice of boolean values associated with the given key in the configuration data.
func (k *Koanf) GetBoolSlice(key string) []bool {
	return k.koanf.Bools(key)
}

// GetInt64Slice retrieves the slice of int64 values associated with the given key in the configuration data.
func (k *Koanf) GetInt64Slice(key string) []int64 {
	return k.koanf.Int64s(key)
}

// GetFloat64Slice retrieves the slice of float64 values associated with the given key in the configuration data.
func (k *Koanf) GetFloat64Slice(key string) []float64 {
	return k.koanf.Float64s(key)
}

// GetStringSlice retrieves the slice of string values associated with the given key in the configuration data.
func (k *Koanf) GetStringSlice(key string) []string {
	return k.koanf.Strings(key)
}

// GetBoolMap retrieves the map of boolean values associated with the given key in the configuration data.
func (k *Koanf) GetBoolMap(key string) map[string]bool {
	return k.koanf.BoolMap(key)
}

// GetInt64Map retrieves the map of int64 values associated with the given key in the configuration data.
func (k *Koanf) GetInt64Map(key string) map[string]int64 {
	return k.koanf.Int64Map(key)
}

// GetFloat64Map retrieves the map of float64 values associated with the given key in the configuration data.
func (k *Koanf) GetFloat64Map(key string) map[string]float64 {
	return k.koanf.Float64Map(key)
}

// GetStringMap retrieves the map of string values associated with the given key in the configuration data.
func (k *Koanf) GetStringMap(key string) map[string]string {
	return k.koanf.StringMap(key)
}

// GetStringSliceMap retrieves the map of string slices associated with the given key in the configuration data.
func (k *Koanf) GetStringSliceMap(key string) map[string][]string {
	return k.koanf.StringsMap(key)
}

// watchUpdaterChannel listens for configuration updates from the updater channel.
// It handles the updates based on the source type (file or raw bytes).
func (k *Koanf) watchUpdaterChannel(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}
	defer func() {
		if r := recover(); r != nil {
			k.logWatcherError("[Config::Koanf] Error in watchUpdaterChannel: %v", r)
			watchUpdaterRestartMu.RLock()
			restartHook := watchUpdaterRestartHook
			watchUpdaterRestartMu.RUnlock()
			if restartHook != nil {
				restartHook(k) // Restart hook for tests/extensibility.
			} else if ctx.Err() == nil {
				k.watchUpdaterChannel(ctx) // Default behavior.
			}
		}
	}()

	ch := k.updateChannel()
	for {
		select {
		case <-ctx.Done():
			return
		case entry, ok := <-ch:
			if !ok {
				return
			}
			k.applyUpdate(entry)
		}
	}
}

func (k *Koanf) applyUpdate(entry *watcher.UpdaterSchema) {
	if entry == nil {
		return
	}

	switch entry.ContentSource {
	case uint8(SourceFile):
		if err := filesys.WriteFile(k.opts.FilePath, []byte(entry.Content), 0644); err != nil {
			k.logWatcherError("[Config::Koanf] Error writing config update: %v", err)
			return
		}

	case uint8(SourceRawBytes):
		k.opts.Content = []byte(entry.Content)
	}

	newInstance, err := newKoanf(k.opts)
	if err != nil {
		k.logWatcherError("[Config::Koanf] Error reloading config: %v", err)
		return
	}

	if k.onReload != nil {
		k.onReload(newInstance)
	} else {
		cMutex.Lock()
		instance = newInstance
		cMutex.Unlock()
	}

	if k.opts.WatcherOptions.OnUpdateCallback != nil {
		k.opts.WatcherOptions.OnUpdateCallback()
	}

	if k.opts != nil && k.opts.WatcherOptions != nil && k.opts.WatcherOptions.Logger != nil {
		k.opts.WatcherOptions.Logger.Info("[Config::Koanf] Configuration updated successfully through updater channel: "+
			"BytesWritten=%d", len(entry.Content))
	}

	time.Sleep(500 * time.Millisecond) // Sleep to avoid tight loop
}

func (k *Koanf) updateChannel() <-chan *watcher.UpdaterSchema {
	if k.updates != nil {
		return k.updates
	}
	if k.opts != nil && k.opts.WatcherOptions != nil && k.opts.WatcherOptions.UpdateChannel != nil {
		return k.opts.WatcherOptions.UpdateChannel
	}
	return watcher.ContentUpdateChannel
}

func (k *Koanf) logWatcherError(format string, args ...interface{}) {
	if k != nil && k.opts != nil && k.opts.WatcherOptions != nil && k.opts.WatcherOptions.Logger != nil {
		k.opts.WatcherOptions.Logger.Error(format, args...)
	}
}
