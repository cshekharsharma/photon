package session

import (
	"context"
	"errors"
	"testing"
	"time"

	storageredis "github.com/cshekharsharma/photon/storage/redis"
	"github.com/stretchr/testify/assert"
)

type mockRedisClient struct {
	*storageredis.Client
	getFn   func(ctx context.Context, key string) *storageredis.StringCmd
	setFn   func(ctx context.Context, key string, value interface{}, expiration time.Duration) *storageredis.StatusCmd
	delFn   func(ctx context.Context, keys ...string) *storageredis.IntCmd
	pingFn  func(ctx context.Context) *storageredis.StatusCmd
	closeFn func() error
}

func (m *mockRedisClient) Get(ctx context.Context, key string) *storageredis.StringCmd {
	return m.getFn(ctx, key)
}

func (m *mockRedisClient) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *storageredis.StatusCmd {
	return m.setFn(ctx, key, value, expiration)
}

func (m *mockRedisClient) Del(ctx context.Context, keys ...string) *storageredis.IntCmd {
	return m.delFn(ctx, keys...)
}

func (m *mockRedisClient) Ping(ctx context.Context) *storageredis.StatusCmd {
	return m.pingFn(ctx)
}

func (m *mockRedisClient) Close() error {
	if m.closeFn == nil {
		return nil
	}
	return m.closeFn()
}

type mockConn struct {
	client   storageredis.RedisClientInterface
	closeErr error
}

func (m *mockConn) GetClient() storageredis.RedisClientInterface { return m.client }
func (m *mockConn) GetRawClient() *storageredis.Client           { return nil }
func (m *mockConn) SetClient(client storageredis.RedisClientInterface) {
	m.client = client
}
func (m *mockConn) Close() error { return m.closeErr }

func newTestRedisClient() *mockRedisClient {
	return &mockRedisClient{
		getFn: func(ctx context.Context, key string) *storageredis.StringCmd {
			return storageredis.NewStringResult("", storageredis.Nil)
		},
		setFn: func(ctx context.Context, key string, value interface{}, expiration time.Duration) *storageredis.StatusCmd {
			return storageredis.NewStatusResult("OK", nil)
		},
		delFn: func(ctx context.Context, keys ...string) *storageredis.IntCmd {
			return storageredis.NewIntResult(1, nil)
		},
		pingFn: func(ctx context.Context) *storageredis.StatusCmd {
			return storageredis.NewStatusResult("PONG", nil)
		},
	}
}

func TestNewRedisStore_PingSuccess(t *testing.T) {
	origSetConfig := redisSetConfig
	origConnect := redisConnect
	origClientFromConn := redisClientFromConn
	defer func() {
		redisSetConfig = origSetConfig
		redisConnect = origConnect
		redisClientFromConn = origClientFromConn
	}()

	redisSetConfig = func(name string, cfg *storageredis.ConnectionConfig) {}
	redisConnect = func(ctx context.Context, connector storageredis.RedisConnectorInterface, name string) (storageredis.RedisInterface, error) {
		return &mockConn{}, nil
	}

	redisClientFromConn = func(conn storageredis.RedisInterface) storageredis.RedisClientInterface { return newTestRedisClient() }

	store, closeFn, err := newRedisStore(StoreOptions{Name: "main", Address: "127.0.0.1:6379", PingOnInit: boolPtr(true)})
	assert.NoError(t, err)
	assert.NotNil(t, store)
	assert.NotNil(t, closeFn)
}

func TestNewRedisStore_PingError(t *testing.T) {
	origSetConfig := redisSetConfig
	origConnect := redisConnect
	origClientFromConn := redisClientFromConn
	defer func() {
		redisSetConfig = origSetConfig
		redisConnect = origConnect
		redisClientFromConn = origClientFromConn
	}()

	redisSetConfig = func(name string, cfg *storageredis.ConnectionConfig) {}
	redisConnect = func(ctx context.Context, connector storageredis.RedisConnectorInterface, name string) (storageredis.RedisInterface, error) {
		return &mockConn{}, nil
	}

	client := newTestRedisClient()
	client.pingFn = func(ctx context.Context) *storageredis.StatusCmd {
		return storageredis.NewStatusResult("", errors.New("ping fail"))
	}

	redisClientFromConn = func(conn storageredis.RedisInterface) storageredis.RedisClientInterface { return client }

	_, _, err := newRedisStore(StoreOptions{Name: "main", Address: "127.0.0.1:6379", PingOnInit: boolPtr(true)})
	assert.Error(t, err)
}

func TestNewRedisStore_ConnectError(t *testing.T) {
	origSetConfig := redisSetConfig
	origConnect := redisConnect
	defer func() {
		redisSetConfig = origSetConfig
		redisConnect = origConnect
	}()

	redisSetConfig = func(name string, cfg *storageredis.ConnectionConfig) {}
	redisConnect = func(ctx context.Context, connector storageredis.RedisConnectorInterface, name string) (storageredis.RedisInterface, error) {
		return nil, errors.New("connect fail")
	}

	_, _, err := newRedisStore(StoreOptions{Name: "main", Address: "127.0.0.1:6379", PingOnInit: boolPtr(false)})
	assert.Error(t, err)
}

func TestRedisStore_Find(t *testing.T) {
	store := &redisStore{client: newTestRedisClient(), prefix: "pref:"}

	b, found, err := store.Find("")
	assert.NoError(t, err)
	assert.False(t, found)
	assert.Nil(t, b)

	client := newTestRedisClient()
	client.getFn = func(ctx context.Context, key string) *storageredis.StringCmd {
		return storageredis.NewStringResult("value", nil)
	}
	store.client = client

	b, found, err = store.Find("token")
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, []byte("value"), b)

	client.getFn = func(ctx context.Context, key string) *storageredis.StringCmd {
		return storageredis.NewStringResult("", storageredis.Nil)
	}
	b, found, err = store.Find("token")
	assert.NoError(t, err)
	assert.False(t, found)
	assert.Nil(t, b)

	client.getFn = func(ctx context.Context, key string) *storageredis.StringCmd {
		return storageredis.NewStringResult("", errors.New("boom"))
	}
	_, _, err = store.Find("token")
	assert.Error(t, err)
}

func TestRedisStore_CommitAndDelete(t *testing.T) {
	var setKey string
	var setTTL time.Duration

	client := newTestRedisClient()
	client.setFn = func(ctx context.Context, key string, value interface{}, expiration time.Duration) *storageredis.StatusCmd {
		setKey = key
		setTTL = expiration
		return storageredis.NewStatusResult("OK", nil)
	}

	store := &redisStore{client: client, prefix: "p:"}

	assert.NoError(t, store.Commit("", []byte("x"), time.Now().Add(time.Hour)))

	expiry := time.Now().Add(5 * time.Second)
	assert.NoError(t, store.Commit("token", []byte("val"), expiry))
	assert.Equal(t, "p:token", setKey)
	assert.True(t, setTTL > 0)

	assert.NoError(t, store.Delete(""))
	assert.NoError(t, store.Delete("token"))

	store.prefix = ""
	assert.NoError(t, store.Commit("plain", []byte("val"), expiry))
	assert.Equal(t, "plain", setKey)
}

func TestRedisStore_Close(t *testing.T) {
	closed := false
	client := newTestRedisClient()
	client.closeFn = func() error { closed = true; return nil }

	store := &redisStore{client: client}
	assert.NoError(t, store.Close())
	assert.True(t, closed)
}

func TestExpiryToTTL(t *testing.T) {
	assert.Equal(t, time.Duration(0), expiryToTTL(time.Time{}))
	assert.Equal(t, time.Second, expiryToTTL(time.Now().Add(-1*time.Second)))
	assert.True(t, expiryToTTL(time.Now().Add(time.Minute)) > 0)
}

func TestRedisClientFromConn_DefaultFuncUsesGetClient(t *testing.T) {
	defaultFn := redisClientFromConn
	client := newTestRedisClient()
	conn := &mockConn{client: client}

	got := defaultFn(conn)
	assert.Equal(t, client, got)
}
