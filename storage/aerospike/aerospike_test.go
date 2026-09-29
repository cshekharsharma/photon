package aerospike

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	aero "github.com/aerospike/aerospike-client-go/v8"
	"github.com/cshekharsharma/photon/utils/testutil/mocks"
	"github.com/cshekharsharma/photon/utils/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type mockedAerospikeConnector struct {
	mock.Mock
}

func (mac *mockedAerospikeConnector) Connect(p *aero.ClientPolicy, hosts []*aero.Host, wp *aero.WritePolicy) (AerospikeInterface, error) {
	args := mac.Called(p, hosts, wp)
	return args.Get(0).(AerospikeInterface), args.Error(1)
}

type mockedAerospike struct {
	mock.Mock
	closeCount int
}

func (ma *mockedAerospike) GetClient() AerospikeClientInterface {
	return nil
}

func (ma *mockedAerospike) GetRawClient() *aero.Client {
	return nil
}

func (a *mockedAerospike) IsConnected() bool {
	args := a.Called()
	return args.Get(0).(bool)
}

func (a *mockedAerospike) Close() {
	a.closeCount++
}

func resetAerospikeRegistry() {
	mutex.Lock()
	defer mutex.Unlock()
	instances = nil
	connectionConfigMap = nil
}

func setupMockAerospike() (*Aerospike, *mocks.MockAerospikeClient) {
	client := new(mocks.MockAerospikeClient)
	a := &Aerospike{client: client}
	return a, client
}

func TestAerospikeConnectorConnect(t *testing.T) {
	ac := &AerospikeConnector{}

	hosts, _ := aero.NewHosts("127.0.0.1:9999") // Dummy invalid host but syntactically correct
	clientPolicy := aero.NewClientPolicy()
	writePolicy := aero.NewWritePolicy(0, 0)

	aeroInterface, err := ac.Connect(clientPolicy, hosts, writePolicy)

	assert.NotNil(t, err)
	assert.NotNil(t, aeroInterface)
	assert.Nil(t, aeroInterface.GetClient())
}

func TestAerospikeConnectorConnect_SetsDefaultWritePolicyOnSuccess(t *testing.T) {
	orig := newClientHook
	defer func() { newClientHook = orig }()
	newClientHook = func(p *aero.ClientPolicy, hosts ...*aero.Host) (*aero.Client, error) {
		return &aero.Client{}, nil
	}

	ac := &AerospikeConnector{}
	hosts, _ := aero.NewHosts("127.0.0.1:3000")
	wp := aero.NewWritePolicy(0, 10)

	aeroInterface, err := ac.Connect(aero.NewClientPolicy(), hosts, wp)
	assert.NoError(t, err)
	assert.NotNil(t, aeroInterface)
	assert.Equal(t, wp, aeroInterface.GetRawClient().DefaultWritePolicy)
}

func TestGetClient(t *testing.T) {
	a, client := setupMockAerospike()
	assert.Equal(t, client, a.GetClient())
}

func TestGetRawClient(t *testing.T) {
	a, _ := setupMockAerospike()
	assert.Nil(t, a.GetRawClient())
}

func TestIsConnected(t *testing.T) {
	a, client := setupMockAerospike()

	client.On("IsConnected").Return(true)
	assert.True(t, a.IsConnected())

	client.AssertExpectations(t)
}

func TestClose(t *testing.T) {
	a, client := setupMockAerospike()

	client.On("Close").Return()
	a.Close()

	client.AssertExpectations(t)
}

func TestConnectInvalidSeed(t *testing.T) {
	mockedAero := new(mockedAerospike)
	mockedAeroConn := new(mockedAerospikeConnector)

	mockedAero.On("IsConnected").Return(true, nil)
	mockedAeroConn.On("Connect", mock.Anything, mock.Anything, mock.Anything).Return(mockedAero, nil)

	SetConnectionConfig("clust1", &ConnectionConfig{
		Hosts: []string{"127.0.0.1:3001"},
	})

	_, err := Connect(mockedAeroConn, "clust1")
	assert.NotNil(t, err)
}

func TestConnectSuccessful(t *testing.T) {
	mockedAero := new(mockedAerospike)
	mockedAeroConn := new(mockedAerospikeConnector)

	randombytes, _ := types.GetCryptoSafeRandomString(15)
	cluster := "clust-" + randombytes
	SetConnectionConfig(cluster, &ConnectionConfig{
		Hosts: []string{"127.0.0.1:3001", "127.0.0.1:3001", "127.0.0.1:3001"},
	})

	mockedAero.On("IsConnected").Return(true)
	mockedAeroConn.On("Connect", mock.Anything, mock.Anything, mock.Anything).Return(mockedAero, nil)
	_, err := Connect(mockedAeroConn, cluster)

	assert.NoError(t, err)
}

func TestConnectSingletonSuccessful(t *testing.T) {
	mockedAero := new(mockedAerospike)
	mockedAeroConn := new(mockedAerospikeConnector)

	randombytes, _ := types.GetCryptoSafeRandomString(15)
	cluster := "clust-" + randombytes
	SetConnectionConfig(cluster, &ConnectionConfig{
		Hosts: []string{"127.0.0.1:3001", "127.0.0.1:3001", "127.0.0.1:3001"},
	})

	mockedAero.On("IsConnected").Return(true)
	mockedAeroConn.On("Connect", mock.Anything, mock.Anything, mock.Anything).Return(mockedAero, nil)
	_, err := Connect(mockedAeroConn, cluster)

	assert.NoError(t, err)

	mockedAero.On("IsConnected").Return(true)
	mockedAeroConn.On("Connect", mock.Anything, mock.Anything, mock.Anything).Return(mockedAero, nil)
	_, err = Connect(mockedAeroConn, cluster)

	assert.NoError(t, err)
}

func TestConnectInvalidHostNames(t *testing.T) {
	mockedAero := new(mockedAerospike)
	mockedAeroConn := new(mockedAerospikeConnector)

	SetConnectionConfig("clust4", &ConnectionConfig{
		Hosts: []string{"127.0.0.1:3001", "127.0.0.1:3001", "abcdddd"},
	})

	mockedAero.On("IsConnected").Return(true)
	mockedAeroConn.On("Connect", mock.Anything, mock.Anything, mock.Anything).Return(mockedAero, nil)

	_, err2 := Connect(mockedAeroConn, "clust4")

	assert.NotNil(t, err2)
}

func TestConnectClientDisconnected(t *testing.T) {
	mockedAero := new(mockedAerospike)
	mockedAeroConn := new(mockedAerospikeConnector)

	SetConnectionConfig("clust5", &ConnectionConfig{
		Hosts: []string{"107.0.0.1:3001", "127.0.0.1:3001", "127.0.0.1:3001", "127.0.0.1:3001"},
	})

	mockedAero.On("IsConnected").Return(false)
	mockedAeroConn.On("Connect", mock.Anything, mock.Anything, mock.Anything).Return(mockedAero, nil)

	_, err2 := Connect(mockedAeroConn, "clust5")

	assert.Nil(t, err2)
}

func TestConnectClientDisconnected_ReconnectError(t *testing.T) {
	mutex.Lock()
	instances = map[string]AerospikeInterface{}
	mutex.Unlock()
	cluster := "clust-reconnect-error"

	down := new(mockedAerospike)
	down.On("IsConnected").Return(false)
	mutex.Lock()
	instances[cluster] = down
	mutex.Unlock()

	SetConnectionConfig(cluster, &ConnectionConfig{
		Hosts: []string{"127.0.0.1:3001", "127.0.0.1:3002"},
	})

	mockedAeroConn := new(mockedAerospikeConnector)
	mockedAeroConn.On("Connect", mock.Anything, mock.Anything, mock.Anything).Return((*mockedAerospike)(nil), errors.New("reconnect failed"))

	conn, err := Connect(mockedAeroConn, cluster)
	assert.Error(t, err)
	assert.Nil(t, conn)
}

func TestRegistryHardening(t *testing.T) {
	t.Run("nil connector", func(t *testing.T) {
		resetAerospikeRegistry()
		conn, err := Connect(nil, "missing")
		assert.Nil(t, conn)
		assert.Error(t, err)
	})

	t.Run("missing and nil config", func(t *testing.T) {
		resetAerospikeRegistry()
		connector := new(mockedAerospikeConnector)

		conn, err := Connect(connector, "missing")
		assert.Nil(t, conn)
		assert.Error(t, err)

		SetConnectionConfig("nil-config", nil)
		conn, err = Connect(connector, "nil-config")
		assert.Nil(t, conn)
		assert.Error(t, err)

		conn, err = newInstance(nil, &ConnectionConfig{})
		assert.Nil(t, conn)
		assert.Error(t, err)

		conn, err = newInstance(connector, nil)
		assert.Nil(t, conn)
		assert.Error(t, err)
	})

	t.Run("config is cloned on set and get", func(t *testing.T) {
		resetAerospikeRegistry()
		cfg := &ConnectionConfig{
			Hosts:             []string{"127.0.0.1:3001", "127.0.0.1:3002"},
			ConnectionTimeout: 7,
			DefaultTTL:        99,
		}
		SetConnectionConfig("clone", cfg)
		cfg.Hosts[0] = "mutated:3000"
		cfg.ConnectionTimeout = 1
		cfg.DefaultTTL = 1

		got := GetConnectionConfig("clone")
		assert.Equal(t, []string{"127.0.0.1:3001", "127.0.0.1:3002"}, got.Hosts)
		assert.Equal(t, int64(7), got.ConnectionTimeout)
		assert.Equal(t, uint32(99), got.DefaultTTL)

		got.Hosts[0] = "changed-again:3000"
		assert.Equal(t, "127.0.0.1:3001", GetConnectionConfig("clone").Hosts[0])
		assert.Nil(t, GetConnectionConfig("missing"))
		resetAerospikeRegistry()
		assert.Nil(t, GetConnectionConfig("missing"))
	})

	t.Run("policy helpers use config and defaults", func(t *testing.T) {
		resetAerospikeRegistry()
		SetConnectionConfig("policy", &ConnectionConfig{
			Hosts:             []string{"127.0.0.1:3001", "127.0.0.1:3002"},
			ConnectionTimeout: 3,
			DefaultTTL:        44,
		})

		assert.Equal(t, []string{"127.0.0.1:3001", "127.0.0.1:3002"}, getClusterHostSlice("policy"))
		assert.Equal(t, 3*time.Second, getConnectionTimeout("policy"))
		assert.Equal(t, uint32(44), getDefaultRecordTTL("policy"))
		assert.Equal(t, uint32(44), GetClusterDefaultWritePolicy("policy", -1).Expiration)
		assert.Equal(t, uint32(12), GetClusterDefaultWritePolicy("policy", 12).Expiration)
		assert.Equal(t, uint32(44), GetClusterDefaultWritePolicy("policy", int64(math.MaxUint32)+1).Expiration)
		assert.Equal(t, uint32(DefaultRecordTTL), GetClusterDefaultWritePolicy("missing", -1).Expiration)
		assert.Nil(t, cloneConnectionConfig(nil))

		SetConnectionConfig("nil-policy", nil)
		assert.Empty(t, getClusterHostSlice("nil-policy"))
		assert.Equal(t, time.Duration(DefaultConnectionTimeout)*time.Second, getConnectionTimeout("nil-policy"))
		assert.Equal(t, uint32(DefaultRecordTTL), getDefaultRecordTTL("nil-policy"))
	})

	t.Run("connect success close and concurrent access", func(t *testing.T) {
		resetAerospikeRegistry()
		cluster := "registry-concurrent"
		SetConnectionConfig(cluster, &ConnectionConfig{
			Hosts: []string{"127.0.0.1:3001", "127.0.0.1:3002"},
		})

		mockedAero := new(mockedAerospike)
		mockedAero.On("IsConnected").Return(true)
		connector := new(mockedAerospikeConnector)
		connector.On("Connect", mock.MatchedBy(func(p *aero.ClientPolicy) bool {
			return p.Timeout == time.Duration(DefaultConnectionTimeout)*time.Second
		}), mock.Anything, mock.MatchedBy(func(wp *aero.WritePolicy) bool {
			return wp != nil && wp.Expiration == DefaultRecordTTL
		})).Return(mockedAero, nil).Once()

		const workers = 8
		var wg sync.WaitGroup
		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				SetConnectionConfig(fmt.Sprintf("cluster-%d", i), &ConnectionConfig{
					Hosts: []string{"127.0.0.1:3001", "127.0.0.1:3002"},
				})
				_ = GetConnectionConfig(fmt.Sprintf("cluster-%d", i))
				conn, err := Connect(connector, cluster)
				assert.NoError(t, err)
				assert.Equal(t, mockedAero, conn)
			}(i)
		}
		wg.Wait()

		CloseCluster("missing")
		CloseCluster(cluster)
		assert.Equal(t, 1, mockedAero.closeCount)
		CloseCluster(cluster)

		mutex.Lock()
		instances = map[string]AerospikeInterface{"a": mockedAero, "nil": nil}
		mutex.Unlock()
		CloseAll()
		assert.Equal(t, 2, mockedAero.closeCount)
	})

	t.Run("new instance by cluster config", func(t *testing.T) {
		resetAerospikeRegistry()
		cluster := "new-instance"
		SetConnectionConfig(cluster, &ConnectionConfig{
			Hosts:             []string{"127.0.0.1:3001", "127.0.0.1:3002"},
			ConnectionTimeout: 2,
			DefaultTTL:        33,
		})

		mockedAero := new(mockedAerospike)
		connector := new(mockedAerospikeConnector)
		connector.On("Connect", mock.MatchedBy(func(p *aero.ClientPolicy) bool {
			return p.Timeout == 2*time.Second
		}), mock.Anything, mock.MatchedBy(func(wp *aero.WritePolicy) bool {
			return wp != nil && wp.Expiration == 33
		})).Return(mockedAero, nil).Once()

		conn, err := newInstance(connector, GetConnectionConfig(cluster))
		assert.NoError(t, err)
		assert.Equal(t, mockedAero, conn)

		conn, err = newInstance(connector, GetConnectionConfig("missing"))
		assert.Nil(t, conn)
		assert.Error(t, err)
	})
}

func TestConnectionTimeoutConfig(t *testing.T) {
	timeout := getConnectionTimeout("non-existent-cluster-3bewkr6")

	assert.NotNil(t, timeout)
	assert.Equal(t, 20*time.Second, timeout)
}

func TestGetDefaultRecordTTLConfig(t *testing.T) {
	ttl := getDefaultRecordTTL("non-existent-cluster-3bewkr6")

	assert.NotNil(t, ttl)
	assert.Equal(t, uint32(1800), ttl)
}

func TestGetClusterHostSlice_UnknownCluster(t *testing.T) {
	assert.Empty(t, getClusterHostSlice("non-existent-cluster-hosts"))
}

func TestNewKey(t *testing.T) {
	key, err := NewKey("n1", "s1", "k1")

	assert.NoError(t, err)
	assert.IsType(t, &aero.Key{}, key)
}

func TestNewBin(t *testing.T) {
	bin := NewBin("n1", "s1")

	assert.IsType(t, &aero.Bin{}, bin)
}

func TestNewMultipleKey(t *testing.T) {
	namespace := "testNamespace"
	setName := "testSet"
	keys := []string{"key1", "key2", "key3"}

	aeroKeys, err := NewMultipleKey(namespace, setName, keys)

	assert.NoError(t, err)
	assert.Len(t, aeroKeys, len(keys))
	for i, aeroKey := range aeroKeys {
		assert.IsType(t, &aero.Key{}, aeroKey)
		assert.Equal(t, keys[i], aeroKey.Value().String())
	}
}

func TestNewMultipleKey_Error(t *testing.T) {
	orig := newKeyHook
	defer func() { newKeyHook = orig }()
	newKeyHook = func(namespace, setName, key string) (*aero.Key, error) {
		if key == "bad-key" {
			return nil, errors.New("bad key")
		}
		return aero.NewKey(namespace, setName, key)
	}

	_, err := NewMultipleKey("testNamespace", "testSet", []string{"ok", "bad-key"})
	assert.Error(t, err)
}
