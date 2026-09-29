// Package aerospikeclient provides a high-level wrapper around the Aerospike Go client.
//
// This package aims to simplify the process of connecting to an Aerospike cluster, performing
// CRUD operations, and managing cluster connections. It provides utilities for establishing
// connections with retry mechanisms, abstracting away the nuances of dealing with individual
// Aerospike nodes, bins, and sets.
//
// Key Features:
//
//   - Simplified Connection Management: Use the provided connector interface to establish and manage
//     connections to Aerospike clusters with built-in checks for connection health.
//
//   - CRUD Operations: The package encapsulates key CRUD operations like Get, Put, Delete making
//     interactions with Aerospike records straightforward.
//
//   - Configurable Connection Policies: Customize connection policies like timeouts and write policies
//     on a per-cluster basis.
//
// - Thread-Safety: Built-in mutex locks ensure thread-safe operations when dealing with cluster connections.
package aerospike

import (
	"fmt"
	"math"
	"sync"
	"time"

	aero "github.com/aerospike/aerospike-client-go/v8"
)

// AerospikeConnectorInterface defines the methods to be implemented
// for connecting and interfacing with an Aerospike database.
type AerospikeConnectorInterface interface {

	// Connect establishes a connection to the Aerospike cluster.
	Connect(p *aero.ClientPolicy, hosts []*aero.Host, wp *aero.WritePolicy) (AerospikeInterface, error)
}

// AerospikeConnector represents a connector to an Aerospike instance.
type AerospikeConnector struct {
	Aerospike *Aerospike
}

// Connect attempts to establish a connection to an Aerospike cluster.
//
// Parameters:
// - p: Client policy configuration.
// - hosts: An array of hosts to connect to.
// - wp: Default write policy for operations without a specific policy.
//
// Returns:
// - An instance of AerospikeInterface, representing the connection.
// - An error if the connection cannot be established or if there are any issues.
func (ac *AerospikeConnector) Connect(p *aero.ClientPolicy, hosts []*aero.Host, wp *aero.WritePolicy) (AerospikeInterface, error) {

	client, err := newClientHook(p, hosts...)

	if err == nil {
		client.DefaultWritePolicy = wp
	}

	ac.Aerospike = &Aerospike{
		client:    client,
		rawclient: client,
	}

	return ac.Aerospike, err
}

// AerospikeInterface defines methods to interact with an Aerospike database.
type AerospikeInterface interface {
	GetClient() AerospikeClientInterface
	GetRawClient() *aero.Client
	Close()
	IsConnected() bool
}

// Aerospike encapsulates the client instance for an Aerospike database.
type Aerospike struct {
	client    AerospikeClientInterface
	rawclient *aero.Client
}

// GetClient returns the underlying client for the Aerospike instance.
// This client is still wrapped by an interface. And to get real raw client
// from aerospike, use `GetRawClient()`.
func (a *Aerospike) GetClient() AerospikeClientInterface {
	return a.client
}

// GetClient returns the underlying raw client (aerov8.Client) for the Aerospike instance.
// This one is the real raw aerospike client without any wrapper interface (unlike `GetClient()`)
func (a *Aerospike) GetRawClient() *aero.Client {
	return a.rawclient
}

// IsConnected checks if the client is connected to an Aerospike cluster.
func (a *Aerospike) IsConnected() bool {
	return a.client.IsConnected()
}

// Close shuts down the connection to Aerospike.
func (a *Aerospike) Close() {
	a.client.Close()
}

var (
	mutex               sync.RWMutex
	instances           map[string]AerospikeInterface
	connectionConfigMap map[string]*ConnectionConfig
	newClientHook       = func(policy *aero.ClientPolicy, hosts ...*aero.Host) (*aero.Client, error) {
		return aero.NewClientWithPolicyAndHost(policy, hosts...)
	}
	newKeyHook = func(namespace, setName, key string) (*aero.Key, error) {
		return NewKey(namespace, setName, key)
	}
)

func SetConnectionConfig(clusterName string, config *ConnectionConfig) {
	mutex.Lock()
	defer mutex.Unlock()

	if connectionConfigMap == nil {
		connectionConfigMap = make(map[string]*ConnectionConfig)
	}

	connectionConfigMap[clusterName] = cloneConnectionConfig(config)
}

// Connect establishes a connection to an Aerospike cluster based on a cluster name.
// It uses the provided AerospikeConnectorInterface to create the connection.
// If a connection for the given clusterName already exists in the 'instances' map,
// it returns the existing connection. Otherwise, it creates a new connection and stores it in the 'instances' map.
// It also handles reconnecting if the connection exists but isn't active.
//
// Note: The function holds a single registry lock while creating the connection
// to avoid duplicate instance creation for the same cluster.
//
// Parameters:
//   - connector: An implementation of AerospikeConnectorInterface to establish the connection.
//   - clusterName: A string representing the name of the cluster to connect to.
//     It's used as a key in the 'instances' map to store and retrieve connections.
//
// Returns:
// - An instance of AerospikeInterface representing the connection.
// - An error if there are any issues establishing the connection.
func Connect(connector AerospikeConnectorInterface, clusterName string) (AerospikeInterface, error) {
	if connector == nil {
		return nil, fmt.Errorf("aerospike: connector is required")
	}

	mutex.Lock()
	defer mutex.Unlock()

	if instances == nil {
		instances = make(map[string]AerospikeInterface)
	}

	if instance := instances[clusterName]; instance != nil && instance.IsConnected() {
		return instance, nil
	}

	config, ok := connectionConfigMap[clusterName]
	if !ok || config == nil {
		return nil, fmt.Errorf("connection config for cluster '%s' not set", clusterName)
	}

	newConn, err := newInstance(connector, config)
	if err != nil {
		return nil, err
	}

	instances[clusterName] = newConn
	return newConn, nil
}

func newInstance(connector AerospikeConnectorInterface, config *ConnectionConfig) (AerospikeInterface, error) {
	if connector == nil {
		return nil, fmt.Errorf("aerospike: connector is required")
	}
	if config == nil {
		return nil, fmt.Errorf("aerospike: connection config is required")
	}

	hostslice := append([]string(nil), config.Hosts...)

	if len(hostslice) < MinRequiredSeedNode {
		err := fmt.Errorf("insufficient number of seed nodes %v provided", hostslice)
		return nil, err
	}

	hosts, hosterror := aero.NewHosts(hostslice...)

	if hosterror != nil {
		return nil, hosterror
	}

	clientPolicy := aero.NewClientPolicy()
	clientPolicy.Timeout = time.Duration(config.ConnectionTimeout) * time.Second
	if config.ConnectionTimeout <= 0 {
		clientPolicy.Timeout = time.Duration(DefaultConnectionTimeout) * time.Second
	}
	writePolicy := newDefaultWritePolicy(config, -1)

	return connector.Connect(clientPolicy, hosts, writePolicy)
}

// newInstance creates a new instance of an Aerospike connection based on the provided cluster name.
// It uses the provided AerospikeConnectorInterface to establish the connection.
//
// The function first retrieves the list of host addresses for the specified cluster.
// It checks to ensure the minimum required seed nodes (MIN_REQUIRED_SEED_NODE) are provided.
// If not, an error is returned.
//
// After verifying the host addresses, the function sets the connection and write policies,
// and then uses the 'connector' to establish the connection to the Aerospike cluster.
func GetClusterDefaultWritePolicy(clusterName string, ttl int64) *aero.WritePolicy {
	mutex.RLock()
	config := cloneConnectionConfig(connectionConfigMap[clusterName])
	mutex.RUnlock()
	return newDefaultWritePolicy(config, ttl)
}

// getClusterHostSlice retrieves a list of host addresses for the specified
// Aerospike cluster. The host addresses are fetched from the configuration
// using a constructed key based on the cluster name.
func getClusterHostSlice(clusterName string) []string {
	mutex.RLock()
	defer mutex.RUnlock()

	if config, ok := connectionConfigMap[clusterName]; ok && config != nil {
		return append([]string(nil), config.Hosts...)
	}
	return []string{}
}

// getConnectionTimeout fetches the connection timeout setting for a specified
// Aerospike cluster from the configuration. If the timeout setting is not found
// for the given cluster, a default timeout value (DEFAULT_CONNECTION_TIMEOUT) is used.
func getConnectionTimeout(clusterName string) time.Duration {
	mutex.RLock()
	defer mutex.RUnlock()

	if config, ok := connectionConfigMap[clusterName]; ok && config != nil {
		return time.Duration(config.ConnectionTimeout) * time.Second
	}
	return time.Duration(DefaultConnectionTimeout) * time.Second
}

// getDefaultRecordTTL retrieves the default time-to-live (TTL) setting for records
// associated with a specified Aerospike cluster from the configuration. If the TTL
// setting is not found for the given cluster, a default TTL value (DEFAULT_RECORD_TTL)
// is used.
func getDefaultRecordTTL(clusterName string) uint32 {
	mutex.RLock()
	defer mutex.RUnlock()

	if config, ok := connectionConfigMap[clusterName]; ok && config != nil {
		return config.DefaultTTL
	}
	return DefaultRecordTTL
}

func GetConnectionConfig(clusterName string) *ConnectionConfig {
	mutex.RLock()
	defer mutex.RUnlock()

	if connectionConfigMap == nil {
		return nil
	}
	return cloneConnectionConfig(connectionConfigMap[clusterName])
}

func CloseCluster(clusterName string) {
	mutex.Lock()
	defer mutex.Unlock()

	if instances == nil || instances[clusterName] == nil {
		return
	}
	instances[clusterName].Close()
	delete(instances, clusterName)
}

func CloseAll() {
	mutex.Lock()
	defer mutex.Unlock()

	for clusterName, instance := range instances {
		if instance != nil {
			instance.Close()
		}
		delete(instances, clusterName)
	}
}

func newDefaultWritePolicy(config *ConnectionConfig, ttl int64) *aero.WritePolicy {
	var aeroTTL uint32
	if ttl >= 0 && ttl <= math.MaxUint32 {
		aeroTTL = uint32(ttl) // #nosec G115 -- range checked above.
	} else {
		aeroTTL = DefaultRecordTTL
		if config != nil && config.DefaultTTL > 0 {
			aeroTTL = config.DefaultTTL
		}
	}

	writePolicy := aero.NewWritePolicy(0, aeroTTL)
	writePolicy.DurableDelete = true
	writePolicy.RecordExistsAction = aero.REPLACE
	writePolicy.Expiration = aeroTTL

	return writePolicy
}

func cloneConnectionConfig(config *ConnectionConfig) *ConnectionConfig {
	if config == nil {
		return nil
	}
	copied := *config
	copied.Hosts = append([]string(nil), config.Hosts...)
	return &copied
}

// ------ Aerospike utility function --------- //

// NewKey creates a new Aerospike key using the given namespace, set name, and key value.
// Parameters:
//   - namespace: The namespace of the Aerospike key.
//   - setName: The set name of the Aerospike key.
//   - key: The primary key value.
//
// Returns:
//   - *aero.Key: The created Aerospike key.
//   - error: Any error encountered during key creation.
func NewKey(namespace string, setName string, key string) (*aero.Key, error) {
	return aero.NewKey(namespace, setName, key)
}

// NewMultipleKeys creates multiple new Aerospike keys arranged in a list using the given namespace,
// set name, and key value.
//
// Parameters:
//   - namespace: The namespace of the Aerospike key.
//   - setName: The set name of the Aerospike key.
//   - keys: List of string keys to be used for creating Aerospike keys.
//
// Returns:
//   - []*aero.Key: The created Aerospike keys.
//   - error: Any error encountered during key creation.
func NewMultipleKey(namespace string, setName string, keys []string) ([]*aero.Key, error) {
	var aeroKeys []*aero.Key

	for _, k := range keys {
		aeroKey, err := newKeyHook(namespace, setName, k)
		if err != nil {
			return nil, fmt.Errorf("failed to create aerospike key for '%s': %w", k, err)
		}

		aeroKeys = append(aeroKeys, aeroKey)
	}

	return aeroKeys, nil
}

// NewBin creates a new Aerospike bin with the provided name and value.
// Parameters:
//   - name: The name of the bin.
//   - bin: The value to be stored in the bin.
//
// Returns:
//   - *aero.Bin: The created Aerospike bin.
func NewBin(name string, bin interface{}) *aero.Bin {
	return aero.NewBin(name, bin)
}
