package session

import (
	"context"
	"errors"
	"testing"

	storageredis "github.com/cshekharsharma/photon/storage/redis"
	"github.com/stretchr/testify/assert"
)

func TestBuildStore_Memory(t *testing.T) {
	cfg, err := applyDefaults(&Config{})
	assert.NoError(t, err)

	store, closeFn, err := buildStore(cfg)
	assert.NoError(t, err)
	assert.NotNil(t, store)
	assert.Nil(t, closeFn)
}

func TestBuildStore_Redis(t *testing.T) {
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

	redisClientFromConn = func(conn storageredis.RedisInterface) storageredis.RedisClientInterface {
		return newTestRedisClient()
	}

	cfg, err := applyDefaults(&Config{Store: StoreConfig{Type: StoreRedis}})
	assert.NoError(t, err)

	store, closeFn, err := buildStore(cfg)
	assert.NoError(t, err)
	assert.NotNil(t, store)
	assert.NotNil(t, closeFn)
	assert.NoError(t, closeFn())
}

func TestBuildStore_RedisError(t *testing.T) {
	origSetConfig := redisSetConfig
	origConnect := redisConnect
	defer func() {
		redisSetConfig = origSetConfig
		redisConnect = origConnect
	}()

	redisSetConfig = func(name string, cfg *storageredis.ConnectionConfig) {}
	redisConnect = func(ctx context.Context, connector storageredis.RedisConnectorInterface, name string) (storageredis.RedisInterface, error) {
		return nil, errors.New("redis fail")
	}

	cfg, err := applyDefaults(&Config{Store: StoreConfig{Type: StoreRedis}})
	assert.NoError(t, err)

	_, _, err = buildStore(cfg)
	assert.Error(t, err)
}

func TestBuildStore_Invalid(t *testing.T) {
	cfg := &Config{Store: StoreConfig{Type: StoreType("bad")}}
	_, _, err := buildStore(cfg)
	assert.ErrorIs(t, err, ErrInvalidStore)
}
