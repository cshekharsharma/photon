package config

import (
	"context"
	"fmt"
	"sync"

	"github.com/cshekharsharma/photon/coordination/network/watcher"
)

// Manager owns a config instance and, when enabled, its watcher lifecycle.
type Manager struct {
	mu     sync.RWMutex
	config Config

	provider string
	options  *Options

	updates chan *watcher.UpdaterSchema
	cancel  context.CancelFunc
}

func NewManager(provider string, options *Options) (*Manager, error) {
	if provider == "" {
		provider = ConfigProviderKoanf
	}
	if exists, _ := typesExistsInAllowedProviders(provider); !exists {
		return nil, fmt.Errorf("invalid config provider %s provided", provider)
	}
	if err := options.Validate(); err != nil {
		return nil, fmt.Errorf("error in config options: %v", err.Error())
	}

	opts := populateRequiredOptionsProperties(cloneOptions(options))
	return &Manager{
		provider: provider,
		options:  opts,
		updates:  make(chan *watcher.UpdaterSchema, 1),
	}, nil
}

func (m *Manager) Load() (Config, error) {
	if m == nil {
		return nil, fmt.Errorf("config manager is nil")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.config != nil {
		return m.config, nil
	}

	cfg, err := m.newConfigLocked()
	if err != nil {
		return nil, err
	}

	m.config = cfg
	m.startWatcherLocked(cfg)
	return cfg, nil
}

func (m *Manager) Close() {
	if m == nil {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.cancel != nil {
		m.cancel()
		m.cancel = nil
	}
	m.config = nil
}

func (m *Manager) setConfig(cfg Config) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.config = cfg
}

func (m *Manager) newConfigLocked() (Config, error) {
	switch m.provider {
	case ConfigProviderKoanf:
		cfg, err := newKoanf(m.options)
		if err != nil {
			return nil, err
		}
		cfg.updates = m.updates
		cfg.onReload = m.setConfig
		return cfg, nil
	default:
		return nil, fmt.Errorf("invalid config provider %s provided", m.provider)
	}
}

func (m *Manager) startWatcherLocked(cfg Config) {
	if m.cancel != nil || cfg == nil || m.options == nil || !m.options.EnableWatch {
		return
	}

	opts := m.options.WatcherOptions
	if opts == nil {
		return
	}
	opts.ContentSource = uint8(m.options.Source)
	opts.ContentFormat = uint8(m.options.Format)
	opts.UpdateChannel = m.updates

	watchCtx := opts.WatchContext
	if watchCtx == nil {
		watchCtx = context.Background()
	}
	ctx, cancel := context.WithCancel(watchCtx)
	m.cancel = cancel
	opts.WatchContext = ctx

	if koanfCfg, ok := cfg.(*Koanf); ok {
		go koanfCfg.watchUpdaterChannel(ctx)
	}

	w := watcher.NewAwsAppConfigWatcher(opts, string(m.options.Source))
	go w.Watch(ctx)
}

func cloneOptions(options *Options) *Options {
	if options == nil {
		return nil
	}

	copied := *options
	if options.Content != nil {
		copied.Content = append([]byte(nil), options.Content...)
	}
	if options.WatcherOptions != nil {
		watcherOptions := *options.WatcherOptions
		copied.WatcherOptions = &watcherOptions
	}
	return &copied
}

func typesExistsInAllowedProviders(provider string) (bool, error) {
	for _, allowed := range allowedProviders {
		if provider == allowed {
			return true, nil
		}
	}
	return false, nil
}
