package redis

import (
	"context"
	"errors"
	"testing"

	"github.com/cshekharsharma/photon/utils/testutil/mocks"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func resetRedisTestState() {
	mutex.Lock()
	defer mutex.Unlock()
	instances = nil
	connectionConfigMap = nil
}

type mockRedis struct {
	mock.Mock
	client RedisClientInterface
}

func (m *mockRedis) GetClient() RedisClientInterface {
	args := m.Called()
	return args.Get(0).(RedisClientInterface)
}

func (m *mockRedis) GetRawClient() *redis.Client {
	args := m.Called()
	return args.Get(0).(*redis.Client)
}

func (m *mockRedis) SetClient(client RedisClientInterface) {
	m.client = client
}

func (m *mockRedis) Close() error {
	args := m.Called()
	return args.Error(0)
}

type mockRedisConnector struct {
	mock.Mock
}

func (m *mockRedisConnector) New(opts *redis.Options) RedisInterface {
	args := m.Called(opts)
	if mockRedis, ok := args.Get(0).(RedisInterface); ok {
		return mockRedis
	}
	return nil
}

func TestRedisConnector_New(t *testing.T) {
	connector := &RedisConnector{}

	opts := &redis.Options{
		Addr:     "localhost:6379",
		Username: "testuser",
		Password: "testpass",
		DB:       0,
		PoolSize: 10,
	}

	instance := connector.New(opts)

	assert.NotNil(t, instance, "Expected a non-nil RedisInterface instance")
	assert.NotNil(t, instance.GetClient(), "Expected client to be initialized")

	_, ok := instance.GetClient().(*redis.Client)
	assert.True(t, ok, "Expected internal client to be *redis.Client")
}

func TestSetConnectionConfig(t *testing.T) {
	SetConnectionConfig("test", &ConnectionConfig{
		Address:  "localhost:6379",
		Username: "testuser",
		Password: "testpass",
		Database: 0,
		PoolSize: 10,
	})

	assert.NotNil(t, GetConnectionConfig("test"))
}

func TestGetConnectionConfig_NilMap(t *testing.T) {
	resetRedisTestState()
	assert.Nil(t, GetConnectionConfig("missing"))
}

func TestConnect_NewConnection(t *testing.T) {
	resetRedisTestState()
	connector := new(mockRedisConnector)
	mockRedis := new(mockRedis)
	connector.On("New", mock.Anything).Return(mockRedis)

	SetConnectionConfig("test", &ConnectionConfig{
		Address: "localhost:6379",
	})

	connector.On("New", mock.Anything).Return(mockRedis)

	client, err := Connect(context.Background(), connector, "test")
	assert.NoError(t, err)
	assert.NotNil(t, client)
}

func TestConnect_NoConfigError(t *testing.T) {
	resetRedisTestState()
	connector := new(mockRedisConnector)
	mockRedis := new(mockRedis)
	connector.On("New", mock.Anything).Return(mockRedis)

	client, err := Connect(context.Background(), connector, "nonexistent")
	assert.Error(t, err)
	assert.Nil(t, client)
}

func TestConnect_NewInstanceError(t *testing.T) {
	resetRedisTestState()

	connector := new(mockRedisConnector)
	connector.On("New", mock.Anything).Return(nil)

	SetConnectionConfig("test-connect-new-instance-error", &ConnectionConfig{
		Address: "localhost:6379",
	})

	client, err := Connect(context.Background(), connector, "test-connect-new-instance-error")
	assert.Error(t, err)
	assert.Nil(t, client)
}

func TestConnect_EdgeCases(t *testing.T) {
	resetRedisTestState()

	connector := new(mockRedisConnector)
	mockRedis := new(mockRedis)
	connector.On("New", mock.Anything).Return(mockRedis)

	SetConnectionConfig("nil-context", &ConnectionConfig{Address: "localhost:6379"})
	var nilCtx context.Context
	client, err := Connect(nilCtx, connector, "nil-context")
	assert.NoError(t, err)
	assert.Equal(t, mockRedis, client)

	client, err = Connect(context.Background(), connector, "nil-context")
	assert.NoError(t, err)
	assert.Equal(t, mockRedis, client)

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	client, err = Connect(cancelled, connector, "nil-context")
	assert.Error(t, err)
	assert.Nil(t, client)
}

func TestConnect_NilConfigMap(t *testing.T) {
	resetRedisTestState()

	connector := new(mockRedisConnector)
	client, err := Connect(context.Background(), connector, "missing")
	assert.Error(t, err)
	assert.Nil(t, client)
}

func TestConnect_MissingConfigInNonEmptyMap(t *testing.T) {
	resetRedisTestState()

	SetConnectionConfig("other", &ConnectionConfig{Address: "localhost:6379"})
	connector := new(mockRedisConnector)

	client, err := Connect(context.Background(), connector, "missing")
	assert.Error(t, err)
	assert.Nil(t, client)
}

func TestGetConnectionConfig_CloneAndNilConfig(t *testing.T) {
	resetRedisTestState()

	SetConnectionConfig("nil-config", nil)
	assert.Nil(t, GetConnectionConfig("nil-config"))

	cfg := &ConnectionConfig{Address: "localhost:6379", Database: 2}
	SetConnectionConfig("clone", cfg)
	cfg.Address = "mutated:6379"

	got := GetConnectionConfig("clone")
	assert.NotNil(t, got)
	assert.Equal(t, "localhost:6379", got.Address)

	got.Address = "caller-mutated:6379"
	assert.Equal(t, "localhost:6379", GetConnectionConfig("clone").Address)
}

func TestNewInstance_Success(t *testing.T) {
	connector := new(mockRedisConnector)
	mockRedis := new(mockRedis)
	connector.On("New", mock.Anything).Return(mockRedis)

	client, err := newInstance(connector, &ConnectionConfig{
		Address: "localhost:6379",
	})

	assert.NoError(t, err)
	assert.NotNil(t, client)
}

func TestNewInstance_Failure(t *testing.T) {
	connector := new(mockRedisConnector)
	connector.On("New", mock.Anything).Return(nil)

	client, err := newInstance(connector, &ConnectionConfig{
		Address: "localhost:6379",
	})

	assert.Error(t, err)
	assert.Nil(t, client)
}

func TestRedis_GetClient(t *testing.T) {
	m := &Redis{}
	mockClient := new(mocks.MockRedisClient)
	m.SetClient(mockClient)

	assert.Equal(t, mockClient, m.GetClient())
}

func TestRedis_GetRawClient(t *testing.T) {
	m := &Redis{}
	assert.Nil(t, m.GetRawClient())
}

func TestRedis_Close_Success(t *testing.T) {
	mockClient := new(mocks.MockRedisClient)
	mockClient.On("Close").Return(nil)

	m := &Redis{}
	m.SetClient(mockClient)

	err := m.Close()
	assert.NoError(t, err)
}

func TestRedis_Close_Error(t *testing.T) {
	mockClient := new(mocks.MockRedisClient)
	mockClient.On("Close").Return(errors.New("close error"))

	m := &Redis{}
	m.SetClient(mockClient)

	err := m.Close()
	assert.Error(t, err)
}
