// Package redis provides functionality for establishing connections to Redis servers
// using the go-redis package "github.com/redis/go-redis/v9". It allows you to
// connect to specific Redis servers, configure connection parameters, and reuse existing
// connections. The package also handles configurations such as server addresses derived from
// the application configuration.
package redis

import (
	"context"
	"fmt"
	"sync"

	"github.com/redis/go-redis/v9"
)

var (
	mutex               sync.RWMutex
	instances           map[string]RedisInterface
	connectionConfigMap map[string]*ConnectionConfig
)

// RedisConnectorInterface defines methods to establish a connection
// to a Redis cluster and retrieve a client interface.
type RedisConnectorInterface interface {

	// New establishes a connection to the Redis server
	New(opt *redis.Options) RedisInterface
}

// RedisConnector represents a connector to a Redis instance
type RedisConnector struct{}

// New attempts to establish a connection to a Redis instance.
//
// Parameters:
// - opt: Redis connection options
//
// Returns:
// - An instance of RedisInterface, representing the connection
func (rc *RedisConnector) New(opt *redis.Options) RedisInterface {
	redisclient := redis.NewClient(opt)
	return &Redis{
		client:    redisclient,
		rawclient: redisclient,
	}
}

// RedisInterface defines methods to interact with a Redis server.
type RedisInterface interface {
	GetClient() RedisClientInterface
	GetRawClient() *redis.Client
	SetClient(client RedisClientInterface)
	Close() error
}

// Redis encapsulates the client instance for a Redis server.
type Redis struct {
	client    RedisClientInterface
	rawclient *redis.Client
}

// GetClient returns the underlying client for the Redis instance.
// This is not the real raw client, and rather still a client wrapped in an interface.
// Use GetRawClient() for real raw client that Redis itself provides.
func (r *Redis) GetClient() RedisClientInterface {
	return r.client
}

// GetRawClient returns the underlying client for Redis instance.
// This is the real raw connection without any wrapper interface (unlike `GetClient()`).
func (r *Redis) GetRawClient() *redis.Client {
	return r.rawclient
}

func (r *Redis) SetClient(client RedisClientInterface) {
	r.client = client
}

// Close shuts down the connection to Redis.
func (r *Redis) Close() error {
	return r.client.Close()
}

// Connect establishes a connection to a Redis server specified by the
// given serverName. If a connection to the server already exists, the
// existing connection is reused. This method uses a singleton pattern to
// ensure only one connection instance per serverName.
//
// Parameters:
//   - serverName: The name of the Redis server to connect to.
//
// Returns:
//   - RedisInterface: A pointer to the established Redis client connection.
//   - error: An error object that describes the reason for any connection
//     failures. It returns nil if the connection was successful.
//
// Note: This method is thread-safe and uses mutexes to handle concurrent access.
// Connect establishes or reuses a Redis connection for serverName.
// The context is checked before creating a new connection so startup callers can
// bind connection acquisition to their own cancellation/deadline policy.
func Connect(ctx context.Context, connector RedisConnectorInterface, serverName string) (RedisInterface, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	mutex.Lock()
	defer mutex.Unlock()

	if instances == nil {
		instances = make(map[string]RedisInterface)
	}
	if instances[serverName] != nil {
		return instances[serverName], nil
	}

	if connectionConfigMap == nil {
		return nil, fmt.Errorf("connection config for server '%s' not set", serverName)
	}

	config, ok := connectionConfigMap[serverName]
	if !ok {
		return nil, fmt.Errorf("connection config for server '%s' not set", serverName)
	}

	newConn, err := newInstance(connector, config)
	if err != nil {
		return nil, err
	}
	instances[serverName] = newConn

	return newConn, nil
}

// newInstance creates a new Redis client connection instance for a
// specified server using configurations derived from the given serverName.
func newInstance(connector RedisConnectorInterface, config *ConnectionConfig) (RedisInterface, error) {
	client := connector.New(&redis.Options{
		Addr:     config.Address,
		Username: config.Username,
		Password: config.Password,
		DB:       config.Database,
		PoolSize: config.PoolSize,
	})

	if client == nil {
		return nil, fmt.Errorf("failed to create redis client for address '%v'", config.Address)
	}

	return client, nil
}

// SetConnectionConfig sets the configuration for a Redis server.
func SetConnectionConfig(clusterName string, config *ConnectionConfig) {
	mutex.Lock()
	defer mutex.Unlock()

	if connectionConfigMap == nil {
		connectionConfigMap = make(map[string]*ConnectionConfig)
	}

	connectionConfigMap[clusterName] = cloneConnectionConfig(config)
}

// GetConnectionConfig retrieves the configuration for a Redis server
func GetConnectionConfig(clusterName string) *ConnectionConfig {
	mutex.RLock()
	defer mutex.RUnlock()

	if connectionConfigMap == nil {
		return nil
	}
	return cloneConnectionConfig(connectionConfigMap[clusterName])
}

func cloneConnectionConfig(config *ConnectionConfig) *ConnectionConfig {
	if config == nil {
		return nil
	}
	copied := *config
	return &copied
}
