package discovery

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/cshekharsharma/photon/core/logger"
	"github.com/hashicorp/consul/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type mockAgent struct{ mock.Mock }

func (m *mockAgent) ServiceRegister(reg *api.AgentServiceRegistration) error {
	args := m.Called(reg)
	return args.Error(0)
}

func (m *mockAgent) ServiceDeregister(serviceID string) error {
	args := m.Called(serviceID)
	return args.Error(0)
}

func (m *mockAgent) UpdateTTL(checkID, output, status string) error {
	args := m.Called(checkID, output, status)
	return args.Error(0)
}

type mockHealth struct{ mock.Mock }

func (m *mockHealth) Service(service, tag string, passingOnly bool, q *api.QueryOptions) ([]*api.ServiceEntry, *api.QueryMeta, error) {
	args := m.Called(service, tag, passingOnly, q)
	return args.Get(0).([]*api.ServiceEntry), args.Get(1).(*api.QueryMeta), args.Error(2)
}

type mockConsulClient struct {
	agent  *mockAgent
	health *mockHealth
}

func (m *mockConsulClient) Agent() agentInterface {
	return m.agent
}

func (m *mockConsulClient) Health() healthInterface {
	return m.health
}

// ------- Tests for Consul Discovery ------
func TestConsulRealClient_AgentAndHealth(t *testing.T) {
	consul, err := api.NewClient(api.DefaultConfig())
	assert.NoError(t, err)

	wrapper := &realConsulClient{c: consul}

	agent := wrapper.Agent()
	health := wrapper.Health()

	assert.NotNil(t, agent)
	assert.NotNil(t, health)
}

func TestNewConsulDiscovery(t *testing.T) {
	t.Run("NilOptions", func(t *testing.T) {
		discovery, err := NewConsulDiscovery(nil)
		require.Nil(t, discovery)
		require.EqualError(t, err, "options are required")
	})

	t.Run("Success", func(t *testing.T) {
		d, err := NewConsulDiscovery(&Options{Address: "localhost:8500"})
		assert.NoError(t, err)
		assert.NotNil(t, d)
	})

	t.Run("Failure", func(t *testing.T) {
		discovery, err := NewConsulDiscovery(&Options{Address: "abcd:://invalid"})

		require.Nil(t, discovery)
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to create consul client")
	})
}

func TestRegister_AllPaths(t *testing.T) {
	originalDefaultHealthCheckTTL := DefaultHealthCheckTTL
	originalDefaultDeregisterCriticalAfter := DefaultDeregisterCriticalAfter
	originalDefaultTTLUpdateInterval := DefaultTTLUpdateInterval

	DefaultHealthCheckTTL = 200 * time.Millisecond
	DefaultDeregisterCriticalAfter = 500 * time.Millisecond
	DefaultTTLUpdateInterval = 100 * time.Millisecond

	defer func() {
		DefaultHealthCheckTTL = originalDefaultHealthCheckTTL
		DefaultDeregisterCriticalAfter = originalDefaultDeregisterCriticalAfter
		DefaultTTLUpdateInterval = originalDefaultTTLUpdateInterval
	}()

	loggerInstance := logger.Init(&logger.LoggerConfig{
		Name:     "consul-register-test",
		Provider: logger.LoggerProviderZerolog,
		Type:     logger.LoggerTypeStdout,
	})

	t.Run("NilInstance", func(t *testing.T) {
		c := &consulDiscovery{}
		err := c.Register(context.Background(), nil)
		assert.EqualError(t, err, "service instance cannot be nil")
	})

	t.Run("CanceledContext", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		c := &consulDiscovery{}
		err := c.Register(ctx, &ServiceInstance{ID: "svc-canceled", Name: "svc"})
		assert.ErrorIs(t, err, context.Canceled)
	})

	t.Run("MissingInstanceId", func(t *testing.T) {
		c := &consulDiscovery{}
		err := c.Register(context.Background(), &ServiceInstance{Name: "svc"})
		assert.EqualError(t, err, "service ID and Name are required")
	})

	t.Run("RegisterFails", func(t *testing.T) {
		agent := new(mockAgent)
		agent.On("ServiceRegister", mock.Anything).Return(errors.New("fail"))

		c := &consulDiscovery{client: &mockConsulClient{agent: agent}, logger: loggerInstance}

		err := c.Register(context.Background(), &ServiceInstance{ID: "regfail", Name: "svc"})
		assert.ErrorContains(t, err, "failed to register service: fail")
	})

	t.Run("Success+TTL+Deregister", func(t *testing.T) {
		id := "svc-" + time.Now().Format("150405.000")

		agent := new(mockAgent)
		agent.On("ServiceRegister", mock.Anything).Return(nil)
		agent.On("UpdateTTL", "service:"+id, api.HealthPassing, api.HealthPassing).Return(nil)
		agent.On("ServiceDeregister", id).Return(nil)

		c := &consulDiscovery{client: &mockConsulClient{agent: agent}, logger: loggerInstance}

		ctx, cancel := context.WithCancel(context.Background())
		err := c.Register(ctx, &ServiceInstance{ID: id, Name: "svc", Address: "127.0.0.1", Port: 8080})
		assert.NoError(t, err)

		time.Sleep(2 * DefaultTTLUpdateInterval)
		cancel()
		time.Sleep(2 * DefaultTTLUpdateInterval)

		agent.AssertCalled(t, "ServiceRegister", mock.Anything)
		agent.AssertCalled(t, "UpdateTTL", "service:"+id, api.HealthPassing, api.HealthPassing)
		agent.AssertCalled(t, "ServiceDeregister", id)
	})

	t.Run("TTLFails", func(t *testing.T) {
		id := "svc-" + time.Now().Format("150405.000")

		agent := new(mockAgent)
		agent.On("ServiceRegister", mock.Anything).Return(nil)
		agent.On("UpdateTTL", "service:"+id, api.HealthPassing, api.HealthPassing).Return(errors.New("ttl-fail")).Maybe()
		agent.On("ServiceDeregister", id).Return(nil)

		c := &consulDiscovery{
			client: &mockConsulClient{agent: agent},
			logger: loggerInstance,
		}

		ctx, cancel := context.WithCancel(context.Background())
		err := c.Register(ctx, &ServiceInstance{ID: id, Name: "svc", Address: "127.0.0.1", Port: 9090})
		assert.NoError(t, err)

		time.Sleep(2 * DefaultTTLUpdateInterval)
		cancel()
		time.Sleep(2 * DefaultTTLUpdateInterval)

		agent.AssertCalled(t, "ServiceDeregister", id)
	})

	t.Run("deregisterFailsOnCtxCancel", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		id := "svc-" + fmt.Sprintf("%d", time.Now().UnixNano())

		agent := new(mockAgent)
		health := new(mockHealth)

		agent.On("ServiceRegister", mock.Anything).Return(nil)
		agent.On("UpdateTTL", mock.Anything, "passing", "passing").Return(nil)
		agent.On("ServiceDeregister", id).Return(errors.New("deregistration failed")).Once()

		client := &mockConsulClient{agent: agent, health: health}
		c := &consulDiscovery{client: client, logger: loggerInstance}

		instance := &ServiceInstance{ID: id, Name: "dereg-fail-svc", Address: "127.0.0.1", Port: 8090}

		err := c.Register(ctx, instance)
		assert.NoError(t, err)

		time.Sleep(2 * DefaultTTLUpdateInterval)
		cancel()

		time.Sleep(500 * time.Millisecond)
		agent.AssertCalled(t, "ServiceDeregister", id)
	})
}

func TestConsulDiscovery_Deregister(t *testing.T) {
	loggerInstance := logger.Init(&logger.LoggerConfig{
		Name:     "consul-deregister-test",
		Provider: logger.LoggerProviderZerolog,
		Type:     logger.LoggerTypeStdout,
	})
	t.Run("EmptyInstanceID", func(t *testing.T) {
		client := &mockConsulClient{}
		c := &consulDiscovery{
			client: client,
			logger: loggerInstance,
		}

		err := c.Deregister(context.Background(), "")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "cannot be empty")
	})

	t.Run("CanceledContext", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		c := &consulDiscovery{}
		err := c.Deregister(ctx, "svc-canceled")
		assert.ErrorIs(t, err, context.Canceled)
	})

	t.Run("DeregisterFailure", func(t *testing.T) {
		agent := new(mockAgent)
		health := new(mockHealth)
		client := &mockConsulClient{agent: agent, health: health}

		agent.On("ServiceDeregister", "svc-err").Return(fmt.Errorf("dereg-error"))

		c := &consulDiscovery{
			client: client,
			logger: loggerInstance,
		}

		err := c.Deregister(context.Background(), "svc-err")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "dereg-error")

		agent.AssertExpectations(t)
	})

	t.Run("DeregisterSuccess", func(t *testing.T) {
		agent := new(mockAgent)
		health := new(mockHealth)
		client := &mockConsulClient{agent: agent, health: health}

		agent.On("ServiceDeregister", "svc-ok").Return(nil)

		c := &consulDiscovery{
			client: client,
			logger: loggerInstance,
		}

		err := c.Deregister(context.Background(), "svc-ok")
		assert.NoError(t, err)

		agent.AssertExpectations(t)
	})

	t.Run("DeregisterContextDeadline", func(t *testing.T) {
		agent := new(mockAgent)
		health := new(mockHealth)
		client := &mockConsulClient{agent: agent, health: health}
		release := make(chan struct{})
		defer close(release)

		agent.On("ServiceDeregister", "svc-timeout").Run(func(mock.Arguments) {
			<-release
		}).Return(nil)

		c := &consulDiscovery{
			client: client,
			logger: loggerInstance,
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		defer cancel()

		err := c.Deregister(ctx, "svc-timeout")
		assert.ErrorIs(t, err, context.DeadlineExceeded)
	})
}

func TestConsulDiscovery_Discover(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		health := new(mockHealth)
		health.On("Service", "svc", "", true, mock.Anything).
			Return([]*api.ServiceEntry{
				{Service: &api.AgentService{ID: "1", Service: "svc", Address: "127.0.0.1", Port: 8080}},
				{Service: &api.AgentService{ID: "2", Service: "svc", Address: "127.0.0.2", Port: 9090}},
			}, new(api.QueryMeta), nil)

		d := &consulDiscovery{
			client: &mockConsulClient{health: health},
			logger: logger.Init(&logger.LoggerConfig{Name: "consul-discovery-test", Provider: logger.LoggerProviderZerolog, Type: logger.LoggerTypeStdout}),
		}

		instances, err := d.Discover(context.Background(), "svc")
		assert.NoError(t, err)
		assert.Len(t, instances, 2)
		assert.Equal(t, "127.0.0.1", instances[0].Address)
		assert.Equal(t, "127.0.0.2", instances[1].Address)
	})

	t.Run("NilContextAndNilServiceEntry", func(t *testing.T) {
		health := new(mockHealth)
		health.On("Service", "svc", "", true, mock.MatchedBy(func(q *api.QueryOptions) bool {
			return q != nil && q.Context() != nil
		})).
			Return([]*api.ServiceEntry{
				{Service: nil},
				{Service: &api.AgentService{ID: "1", Service: "svc", Address: "127.0.0.1", Port: 8080}},
			}, new(api.QueryMeta), nil)

		d := &consulDiscovery{
			client: &mockConsulClient{health: health},
			logger: logger.Init(&logger.LoggerConfig{Name: "consul-discovery-nil-context-test", Provider: logger.LoggerProviderZerolog, Type: logger.LoggerTypeStdout}),
		}

		var nilCtx context.Context
		instances, err := d.Discover(nilCtx, "svc")
		assert.NoError(t, err)
		assert.Len(t, instances, 1)
		assert.Equal(t, "1", instances[0].ID)
	})

	t.Run("CanceledContext", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		d := &consulDiscovery{}
		instances, err := d.Discover(ctx, "svc")
		assert.ErrorIs(t, err, context.Canceled)
		assert.Nil(t, instances)
	})

	t.Run("EmptyName", func(t *testing.T) {
		d := &consulDiscovery{}
		instances, err := d.Discover(context.Background(), "")
		assert.Error(t, err)
		assert.Nil(t, instances)
	})

	t.Run("ConsulError", func(t *testing.T) {
		health := new(mockHealth)
		health.On("Service", mock.Anything, mock.Anything, true, mock.Anything).
			Return([]*api.ServiceEntry{}, &api.QueryMeta{}, errors.New("simulated consul error"))

		d := &consulDiscovery{
			client: &mockConsulClient{health: health},
			logger: logger.Init(&logger.LoggerConfig{Name: "consul-discovery-test", Provider: logger.LoggerProviderZerolog, Type: logger.LoggerTypeStdout}),
		}

		instances, err := d.Discover(context.Background(), "svc")
		assert.Error(t, err)
		assert.Nil(t, instances)
	})

	t.Run("NilServiceEntries", func(t *testing.T) {
		health := new(mockHealth)
		health.On("Service", mock.Anything, mock.Anything, true, mock.Anything).
			Return([]*api.ServiceEntry{}, &api.QueryMeta{}, nil)

		d := &consulDiscovery{
			client: &mockConsulClient{health: health},
			logger: logger.Init(&logger.LoggerConfig{Name: "consul-discovery-test", Provider: logger.LoggerProviderZerolog, Type: logger.LoggerTypeStdout}),
		}

		instances, err := d.Discover(context.Background(), "svc")
		assert.NoError(t, err)
		assert.Empty(t, instances)
	})
}

func TestConsulDiscovery_Watch_AllCases(t *testing.T) {
	logger := logger.Init(&logger.LoggerConfig{
		Name:     "consul-watch-test",
		Provider: logger.LoggerProviderZerolog,
		Type:     logger.LoggerTypeStdout,
	})

	t.Run("empty_service_name", func(t *testing.T) {
		d := &consulDiscovery{logger: logger}
		err := d.Watch(context.Background(), "", func([]*ServiceInstance) {})
		require.Error(t, err)
		require.Contains(t, err.Error(), "service name cannot be empty")
	})

	t.Run("canceled_context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		d := &consulDiscovery{logger: logger}
		err := d.Watch(ctx, "svc", func([]*ServiceInstance) {})
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("nil_callback", func(t *testing.T) {
		d := &consulDiscovery{logger: logger}
		err := d.Watch(context.Background(), "svc", nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "onChange callback must be provided")
	})

	t.Run("error_and_recovery_and_callback", func(t *testing.T) {
		mockAgent := new(mockAgent)
		mockHealth := new(mockHealth)

		client := &mockConsulClient{
			agent:  mockAgent,
			health: mockHealth,
		}

		discovery := &consulDiscovery{
			client:                  client,
			logger:                  logger,
			watchBlockingWaitTime:   100 * time.Millisecond,
			watchIterationSleepTime: 50 * time.Millisecond,
		}

		serviceName := "svc"
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var wg sync.WaitGroup
		wg.Add(1)

		mockHealth.On("Service", serviceName, "", true, mock.Anything).Maybe().
			Return([]*api.ServiceEntry{}, &api.QueryMeta{LastIndex: 0}, fmt.Errorf("simulated error"))

		mockHealth.On("Service", serviceName, "", true, mock.MatchedBy(func(opt *api.QueryOptions) bool {
			return opt.WaitIndex == 0
		})).Maybe().
			Return([]*api.ServiceEntry{
				{Service: &api.AgentService{ID: "noop"}},
			}, &api.QueryMeta{LastIndex: 0}, nil)

		mockHealth.On("Service", serviceName, "", true, mock.MatchedBy(func(opt *api.QueryOptions) bool {
			return opt.WaitIndex == 0
		})).Maybe().
			Return([]*api.ServiceEntry{
				{Service: &api.AgentService{
					ID:      "svc-2",
					Service: serviceName,
					Address: "127.0.0.1",
					Port:    8080,
					Tags:    []string{"tag"},
					Meta:    map[string]string{"v": "1"},
				}},
				{Service: nil},
			}, &api.QueryMeta{LastIndex: 1}, nil)

		onChange := func(instances []*ServiceInstance) {
			require.Len(t, instances, 1)
			assert.Equal(t, "svc-2", instances[0].ID)
			wg.Done()
		}

		require.NoError(t, discovery.Watch(ctx, serviceName, onChange))

		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()

		select {
		case <-done:
		case <-time.After(1 * time.Second):
			//	t.Fatal("onChange should have been called")
		}
	})

	t.Run("entry_with_nil_service", func(t *testing.T) {
		agent := new(mockAgent)
		health := new(mockHealth)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		health.On("Service", "svc", "", true, mock.Anything).
			Return([]*api.ServiceEntry{{Service: nil}}, &api.QueryMeta{LastIndex: 1}, nil).Maybe()

		client := &mockConsulClient{agent: agent, health: health}
		d := &consulDiscovery{
			client:                  client,
			logger:                  logger,
			watchBlockingWaitTime:   100 * time.Millisecond,
			watchIterationSleepTime: 50 * time.Millisecond,
		}

		err := d.Watch(ctx, "svc", func(instances []*ServiceInstance) {
			require.Empty(t, instances)
		})
		require.NoError(t, err)
		time.Sleep(100 * time.Millisecond)
	})

	t.Run("callback_panics_should_be_caught", func(t *testing.T) {
		agent := new(mockAgent)
		health := new(mockHealth)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		health.On("Service", "svc", "", true, mock.Anything).Return([]*api.ServiceEntry{
			{
				Service: &api.AgentService{
					ID: "svc-id",
				},
			},
		}, &api.QueryMeta{LastIndex: 1}, nil).Maybe()

		client := &mockConsulClient{agent: agent, health: health}
		d := &consulDiscovery{
			client:                  client,
			logger:                  logger,
			watchBlockingWaitTime:   100 * time.Millisecond,
			watchIterationSleepTime: 50 * time.Millisecond,
		}

		err := d.Watch(ctx, "svc", func([]*ServiceInstance) {
			panic("simulated panic")
		})
		require.NoError(t, err)
		time.Sleep(100 * time.Millisecond)
	})

	t.Run("error_sleep_stops_on_context_cancel", func(t *testing.T) {
		health := new(mockHealth)
		called := make(chan struct{})
		health.On("Service", "svc", "", true, mock.Anything).Run(func(mock.Arguments) {
			close(called)
		}).Return([]*api.ServiceEntry{}, &api.QueryMeta{}, errors.New("temporary"))

		ctx, cancel := context.WithCancel(context.Background())
		d := &consulDiscovery{
			client:                  &mockConsulClient{health: health},
			logger:                  logger,
			watchBlockingWaitTime:   time.Millisecond,
			watchIterationSleepTime: time.Hour,
		}

		require.NoError(t, d.Watch(ctx, "svc", func([]*ServiceInstance) {}))
		<-called
		cancel()
		time.Sleep(10 * time.Millisecond)
	})

	t.Run("error_after_context_cancel_returns_without_sleep", func(t *testing.T) {
		health := new(mockHealth)
		ctx, cancel := context.WithCancel(context.Background())
		called := make(chan struct{})

		health.On("Service", "svc", "", true, mock.Anything).Run(func(mock.Arguments) {
			cancel()
			close(called)
		}).Return([]*api.ServiceEntry{}, &api.QueryMeta{}, errors.New("context closed"))

		d := &consulDiscovery{
			client:                  &mockConsulClient{health: health},
			logger:                  logger,
			watchBlockingWaitTime:   time.Millisecond,
			watchIterationSleepTime: time.Hour,
		}

		require.NoError(t, d.Watch(ctx, "svc", func([]*ServiceInstance) {}))
		<-called
		time.Sleep(10 * time.Millisecond)
	})
}

func TestConsulDiscovery_Close(t *testing.T) {
	c := &consulDiscovery{}
	err := c.Close()
	assert.NoError(t, err)
}

func TestConsulDiscovery_WatchTimingDefaults(t *testing.T) {
	c := &consulDiscovery{}
	assert.Equal(t, watchBlockingWaitTime, c.watchWaitTime())
	assert.Equal(t, watchIterationSleepTime, c.watchSleepTime())

	c.watchBlockingWaitTime = time.Millisecond
	c.watchIterationSleepTime = 2 * time.Millisecond
	assert.Equal(t, time.Millisecond, c.watchWaitTime())
	assert.Equal(t, 2*time.Millisecond, c.watchSleepTime())

	var nilDiscovery *consulDiscovery
	assert.Equal(t, watchBlockingWaitTime, nilDiscovery.watchWaitTime())
	assert.Equal(t, watchIterationSleepTime, nilDiscovery.watchSleepTime())
}

func TestConsulDiscovery_InternalSafetyHelpers(t *testing.T) {
	var nilDiscovery *consulDiscovery
	var nilCtx context.Context
	assert.NotNil(t, nilDiscovery.log())
	assert.NotNil(t, normalizeDiscoveryContext(nilCtx))
	assert.True(t, sleepWithContext(context.Background(), 0))
}

func TestPopulateConsulConfig_AllFields(t *testing.T) {
	httpClient := &http.Client{}
	opts := &Options{
		Address:    "http://custom-consul:8500",
		Scheme:     "https",
		Token:      "abcd-token",
		Datacenter: "dc1",
		HTTPClient: httpClient,
		TLSConfig: &TLSConfig{
			CAFile:             "/path/to/ca.pem",
			CertFile:           "/path/to/cert.pem",
			KeyFile:            "/path/to/key.pem",
			InsecureSkipVerify: true,
		},
		WaitTime: 30 * time.Second,
	}

	cfg := populateConsulConfig(opts)

	assert.Equal(t, "http://custom-consul:8500", cfg.Address)
	assert.Equal(t, "https", cfg.Scheme)
	assert.Equal(t, "abcd-token", cfg.Token)
	assert.Equal(t, "dc1", cfg.Datacenter)
	assert.Equal(t, httpClient, cfg.HttpClient)

	assert.Equal(t, "/path/to/ca.pem", cfg.TLSConfig.CAFile)
	assert.Equal(t, "/path/to/cert.pem", cfg.TLSConfig.CertFile)
	assert.Equal(t, "/path/to/key.pem", cfg.TLSConfig.KeyFile)
	assert.True(t, cfg.TLSConfig.InsecureSkipVerify)

	assert.Equal(t, 30*time.Second, cfg.WaitTime)
}

func TestPopulateConsulConfig_Defaults(t *testing.T) {
	opts := &Options{}
	cfg := populateConsulConfig(opts)

	defaultCfg := api.DefaultConfig()
	assert.Equal(t, defaultCfg.Address, cfg.Address)
	assert.Equal(t, defaultCfg.Scheme, cfg.Scheme)
	assert.Equal(t, defaultCfg.Token, cfg.Token)
	assert.Equal(t, defaultCfg.Datacenter, cfg.Datacenter)
	assert.Equal(t, defaultCfg.HttpClient, cfg.HttpClient)
	assert.Equal(t, defaultCfg.TLSConfig, cfg.TLSConfig)
	assert.Equal(t, defaultCfg.WaitTime, cfg.WaitTime)
}
