// Package discovery provides a pluggable interface and Consul-based implementation
// for service discovery and registration in distributed systems.
package discovery

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/cshekharsharma/photon/core/logger"
	"github.com/hashicorp/consul/api"
)

var (
	watchIterationSleepTime = 1 * time.Second // Time to sleep between watch iterations
	watchBlockingWaitTime   = 5 * time.Minute // Time to wait for changes in the watch

	DefaultHealthCheckTTL          = 5 * time.Second // DefaultTTL is the default TTL for health checks.
	DefaultDeregisterCriticalAfter = 5 * time.Minute // Deregister a service if critical for this long.
	DefaultTTLUpdateInterval       = 5 * time.Second // Default interval for updating the TTL.
	defaultDeregisterTimeout       = 2 * time.Second
)

// consulDiscovery is a Consul-based implementation of the ServiceDiscovery interface.
// It supports service registration, discovery, deregistration, and watching for changes.
type consulDiscovery struct {
	client                  consulClient
	logger                  logger.Logger
	watchIterationSleepTime time.Duration
	watchBlockingWaitTime   time.Duration
}

type consulClient interface {
	Agent() agentInterface
	Health() healthInterface
}

type agentInterface interface {
	ServiceRegister(reg *api.AgentServiceRegistration) error
	ServiceDeregister(serviceID string) error
	UpdateTTL(checkID, output, status string) error
}

type healthInterface interface {
	Service(service, tag string, passingOnly bool, q *api.QueryOptions) ([]*api.ServiceEntry, *api.QueryMeta, error)
}

type realConsulClient struct {
	c *api.Client
}

func (r *realConsulClient) Agent() agentInterface {
	return r.c.Agent()
}

func (r *realConsulClient) Health() healthInterface {
	return r.c.Health()
}

// NewConsulDiscovery creates and returns a new Consul-based ServiceDiscovery instance.
//
// Parameters:
//   - addr: Address of the Consul agent (e.g., "localhost:8500").
//
// Returns:
//   - ServiceDiscovery: A fully initialized instance of the Consul-based discovery system.
//   - error: Any error encountered while creating the Consul client.
func NewConsulDiscovery(opts *Options) (ServiceDiscovery, error) {
	if opts == nil {
		return nil, errors.New("options are required")
	}
	consulCfg := populateConsulConfig(opts)

	client, err := api.NewClient(consulCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create consul client: %w", err)
	}

	consul := &consulDiscovery{
		client: &realConsulClient{
			c: client,
		},
		logger:                  discoveryLogger(opts.Logger),
		watchIterationSleepTime: watchIterationSleepTime,
		watchBlockingWaitTime:   watchBlockingWaitTime,
	}

	return consul, nil
}

// Register adds a new service instance to the Consul service catalog.
//
// It sets up a TTL-based health check and periodically marks the service as healthy.
// This ensures Consul knows the service is alive and can remove it if it stops sending heartbeats.
//
// Parameters:
//   - ctx: A context for cancellation.
//   - instance: The service instance to register.
//
// Returns:
//   - error: If registration or TTL updates fail.
func (c *consulDiscovery) Register(ctx context.Context, instance *ServiceInstance) error {
	ctx = normalizeDiscoveryContext(ctx)
	if err := ctx.Err(); err != nil {
		return err
	}
	if instance == nil {
		return errors.New("service instance cannot be nil")
	}
	if instance.ID == "" || instance.Name == "" {
		return errors.New("service ID and Name are required")
	}

	reg := &api.AgentServiceRegistration{
		ID:      instance.ID,
		Name:    instance.Name,
		Address: instance.Address,
		Port:    instance.Port,
		Tags:    instance.Tags,
		Meta:    instance.Metadata,
		Check: &api.AgentServiceCheck{
			TTL:                            DefaultHealthCheckTTL.String(),
			DeregisterCriticalServiceAfter: DefaultDeregisterCriticalAfter.String(),
		},
	}

	if err := c.client.Agent().ServiceRegister(reg); err != nil {
		return fmt.Errorf("failed to register service: %w", err)
	}

	// Start TTL heartbeat and cleanup handler
	go func() {
		ticker := time.NewTicker(DefaultTTLUpdateInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				c.log().Warn("context done, deregistering service %s", instance.ID)
				cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), defaultDeregisterTimeout)
				if err := c.Deregister(cleanupCtx, instance.ID); err != nil {
					c.log().Error("failed to deregister service %s: %v", instance.ID, err)
				}
				cancel()
				return
			case <-ticker.C:
				ttlErr := c.client.Agent().UpdateTTL("service:"+instance.ID, api.HealthPassing, api.HealthPassing)
				if ttlErr != nil {
					c.log().Error("failed to update TTL for service %s: %v", instance.ID, ttlErr)
				}
			}
		}
	}()

	return nil
}

// Deregister removes a registered service instance from Consul.
//
// Parameters:
//   - ctx: A context for cancellation.
//   - instanceID: The ID of the service instance to deregister.
//
// Returns:
//   - error: If deregistration fails.
func (c *consulDiscovery) Deregister(ctx context.Context, instanceID string) error {
	ctx = normalizeDiscoveryContext(ctx)
	if err := ctx.Err(); err != nil {
		return err
	}
	if instanceID == "" {
		return errors.New("service instance ID cannot be empty")
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- c.client.Agent().ServiceDeregister(instanceID)
	}()

	select {
	case <-ctx.Done():
		c.log().Error("failed to deregister service %s: %v", instanceID, ctx.Err())
		return fmt.Errorf("failed to deregister service %s: %w", instanceID, ctx.Err())
	case err := <-errCh:
		if err == nil {
			c.log().Info("successfully deregistered service %s", instanceID)
			return nil
		}
		c.log().Error("failed to deregister service %s: %v", instanceID, err)
		return fmt.Errorf("failed to deregister service %s: %w", instanceID, err)
	}
}

// Discover returns all healthy instances of a given service from Consul.
//
// Parameters:
//   - ctx: A context for cancellation.
//   - serviceName: The name of the service to discover.
//
// Returns:
//   - []*ServiceInstance: A list of discovered healthy instances.
//   - error: If discovery fails.
func (c *consulDiscovery) Discover(ctx context.Context, serviceName string) ([]*ServiceInstance, error) {
	ctx = normalizeDiscoveryContext(ctx)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if serviceName == "" {
		return nil, errors.New("service name cannot be empty")
	}

	services, _, err := c.client.Health().Service(serviceName, "", true, (&api.QueryOptions{}).WithContext(ctx))
	if err != nil {
		return nil, fmt.Errorf("failed to discover service %s: %w", serviceName, err)
	}

	var instances []*ServiceInstance
	for _, s := range services {
		if s.Service == nil {
			continue
		}
		instances = append(instances, &ServiceInstance{
			ID:       s.Service.ID,
			Name:     s.Service.Service,
			Address:  s.Service.Address,
			Port:     s.Service.Port,
			Tags:     s.Service.Tags,
			Metadata: s.Service.Meta,
		})
	}
	return instances, nil
}

// Watch monitors a specific service for changes in its healthy instances list,
// and invokes the provided callback function on every update.
//
// This method runs asynchronously in a goroutine and uses Consul's blocking query mechanism.
//
// Parameters:
//   - ctx: A context for cancellation.
//   - serviceName: The name of the service to watch.
//   - onChange: A callback function that is called with the updated list of instances.
//
// Returns:
//   - error: If setup fails (watch runs independently).
func (c *consulDiscovery) Watch(ctx context.Context, serviceName string, onChange func([]*ServiceInstance)) error {
	ctx = normalizeDiscoveryContext(ctx)
	if err := ctx.Err(); err != nil {
		return err
	}
	if serviceName == "" {
		return errors.New("service name cannot be empty")
	}
	if onChange == nil {
		return errors.New("onChange callback must be provided")
	}

	go func() {
		var lastIndex uint64
		for {
			select {
			case <-ctx.Done():
				c.log().Info("watch stopped for service %s", serviceName)
				return
			default:
				query := (&api.QueryOptions{
					WaitIndex: lastIndex,
					WaitTime:  c.watchWaitTime(),
				}).WithContext(ctx)
				entries, meta, err := c.client.Health().Service(serviceName, "", true, query)
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					c.log().Warn("watch error for %s: %v", serviceName, err)
					if !sleepWithContext(ctx, c.watchSleepTime()) {
						return
					}
					continue
				}

				if meta == nil || meta.LastIndex == lastIndex {
					continue
				}
				lastIndex = meta.LastIndex

				var instances []*ServiceInstance
				for _, e := range entries {
					if e.Service == nil {
						continue
					}
					instances = append(instances, &ServiceInstance{
						ID:       e.Service.ID,
						Name:     e.Service.Service,
						Address:  e.Service.Address,
						Port:     e.Service.Port,
						Tags:     e.Service.Tags,
						Metadata: e.Service.Meta,
					})
				}

				func() {
					defer func() {
						if r := recover(); r != nil {
							c.log().Error("panic in onChange handler for %s: %v", serviceName, r)
						}
					}()
					onChange(instances)
				}()
			}
		}
	}()
	return nil
}

// Close performs any necessary cleanup. For Consul, no explicit close is required.
//
// Returns:
//   - error: Always nil.
func (c *consulDiscovery) Close() error {
	return nil
}

func (c *consulDiscovery) watchWaitTime() time.Duration {
	if c != nil && c.watchBlockingWaitTime > 0 {
		return c.watchBlockingWaitTime
	}
	return watchBlockingWaitTime
}

func (c *consulDiscovery) watchSleepTime() time.Duration {
	if c != nil && c.watchIterationSleepTime > 0 {
		return c.watchIterationSleepTime
	}
	return watchIterationSleepTime
}

func (c *consulDiscovery) log() logger.Logger {
	if c != nil && c.logger != nil {
		return c.logger
	}
	return discoveryLogger(nil)
}

func discoveryLogger(l logger.Logger) logger.Logger {
	if l != nil {
		return l
	}
	return logger.Init(&logger.LoggerConfig{
		Name:     "photon-discovery",
		Provider: logger.LoggerProviderZerolog,
		Type:     logger.LoggerTypeStdout,
	})
}

func normalizeDiscoveryContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

func sleepWithContext(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return true
	}
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// populateConsulConfig populates the Consul configuration based on the provided options.
// It sets the address, scheme, token, datacenter, HTTP client, TLS configuration,
// and wait time for the Consul client.
func populateConsulConfig(opts *Options) *api.Config {
	cfg := api.DefaultConfig()
	if opts.Address != "" {
		cfg.Address = opts.Address
	}
	if opts.Scheme != "" {
		cfg.Scheme = opts.Scheme
	}
	if opts.Token != "" {
		cfg.Token = opts.Token
	}
	if opts.Datacenter != "" {
		cfg.Datacenter = opts.Datacenter
	}
	if opts.HTTPClient != nil {
		cfg.HttpClient = opts.HTTPClient
	}
	if opts.TLSConfig != nil {
		cfg.TLSConfig = api.TLSConfig{
			CAFile:             opts.TLSConfig.CAFile,
			CertFile:           opts.TLSConfig.CertFile,
			KeyFile:            opts.TLSConfig.KeyFile,
			InsecureSkipVerify: opts.TLSConfig.InsecureSkipVerify,
		}
	}
	if opts.WaitTime != 0 {
		cfg.WaitTime = opts.WaitTime
	}
	return cfg
}
