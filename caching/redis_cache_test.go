package caching

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/cshekharsharma/photon/storage/redis"
	"github.com/cshekharsharma/photon/utils/testutil/mocks"
	redisv9 "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type mockRedis struct {
	mock.Mock
	client redis.RedisClientInterface
}

func (m *mockRedis) GetClient() redis.RedisClientInterface {
	args := m.Called()
	return args.Get(0).(redis.RedisClientInterface)
}

func (m *mockRedis) GetRawClient() *redisv9.Client {
	args := m.Called()
	return args.Get(0).(*redisv9.Client)
}

func (m *mockRedis) SetClient(client redis.RedisClientInterface) {
	m.client = client
}

func (m *mockRedis) Close() error {
	args := m.Called()
	return args.Error(0)
}

// -------- Test -------- //

func setupRedisCache() (*RedisCache, *mockRedis, *mocks.MockRedisClient) {
	mockClient := new(mocks.MockRedisClient)
	mockRedis := new(mockRedis)
	mockRedis.On("GetClient").Return(mockClient)

	cache := &RedisCache{
		client:      mockRedis,
		namespace:   "testns",
		collection:  "testcol",
		clusterName: "testcluster",
	}

	return cache, mockRedis, mockClient
}

func mustBool(value bool, err error) bool {
	if err != nil {
		panic(err)
	}
	return value
}

func mustAny(value any, err error) any {
	if err != nil {
		panic(err)
	}
	return value
}

func mustMap(value map[string]any, err error) map[string]any {
	if err != nil {
		panic(err)
	}
	return value
}

func mustMapBool(value map[string]bool, err error) map[string]bool {
	if err != nil {
		panic(err)
	}
	return value
}

func TestRedisCache_Exists(t *testing.T) {
	cache, mockRedis, mockClient := setupRedisCache()

	ctx := context.Background()
	mockClient.On("Exists", ctx, mock.Anything).Return(redisv9.NewIntResult(1, nil))

	req := &ExistsRequest{
		Key: "mykey",
	}

	req.SetNamespace("testns")
	req.SetCollection("testcol")

	exists, err := cache.Exists(context.Background(), req)

	assert.NoError(t, err)
	assert.True(t, exists)

	mockRedis.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestRedisCache_Get(t *testing.T) {
	cache, mockRedis, mockClient := setupRedisCache()

	ctx := context.Background()
	expectedValue := []byte("value")
	mockClient.On("Get", ctx, mock.Anything).Return(redisv9.NewStringResult(string(expectedValue), nil))

	req := &GetRequest{
		Key: "mykey",
	}

	req.SetNamespace("testns")
	req.SetCollection("testcol")

	value, err := cache.Get(context.Background(), req)

	assert.NoError(t, err)
	assert.Equal(t, expectedValue, value)

	mockRedis.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestRedisCache_Get_NotFound(t *testing.T) {
	cache, mockRedis, mockClient := setupRedisCache()

	ctx := context.Background()
	mockClient.On("Get", ctx, mock.Anything).Return(redisv9.NewStringResult("", redisv9.Nil))

	req := &GetRequest{Key: "mykey"}
	req.SetNamespace("testns")
	req.SetCollection("testcol")

	value, err := cache.Get(context.Background(), req)
	assert.NoError(t, err)
	assert.Nil(t, value)

	mockRedis.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestRedisCache_Get_Error(t *testing.T) {
	cache, mockRedis, mockClient := setupRedisCache()

	ctx := context.Background()
	mockClient.On("Get", ctx, mock.Anything).Return(redisv9.NewStringResult("", errors.New("boom")))

	req := &GetRequest{Key: "mykey"}
	req.SetNamespace("testns")
	req.SetCollection("testcol")

	value, err := cache.Get(context.Background(), req)
	assert.Error(t, err)
	assert.Nil(t, value)

	mockRedis.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestRedisCache_Set(t *testing.T) {
	cache, mockRedis, mockClient := setupRedisCache()

	ctx := context.Background()
	mockClient.On("Set", ctx, mock.Anything, mock.Anything, mock.Anything).Return(redisv9.NewStatusResult("OK", nil))

	req := &SetRequest{
		Key:   "mykey",
		Value: "testval",
		TTL:   100,
	}

	req.SetNamespace("testns")
	req.SetCollection("testcol")

	success, err := cache.Set(context.Background(), req)

	assert.NoError(t, err)
	assert.True(t, success)

	mockRedis.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestRedisCache_Set_SerialiseError(t *testing.T) {
	cache, _, _ := setupRedisCache()

	req := &SetRequest{
		Key:   "mykey",
		Value: make(chan int),
		TTL:   1,
	}
	req.SetNamespace("testns")
	req.SetCollection("testcol")

	success, err := cache.Set(context.Background(), req)
	assert.Error(t, err)
	assert.False(t, success)
}

func TestRedisCache_Delete(t *testing.T) {
	cache, mockRedis, mockClient := setupRedisCache()

	ctx := context.Background()
	mockClient.On("Del", ctx, mock.Anything).Return(redisv9.NewIntResult(1, nil))

	req := &DeleteRequest{
		Key: "mykey",
	}

	req.SetNamespace("testns")
	req.SetCollection("testcol")

	success, err := cache.Delete(context.Background(), req)

	assert.NoError(t, err)
	assert.True(t, success)

	mockRedis.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestRedisCache_MultiGet(t *testing.T) {
	cache, mockRedis, mockClient := setupRedisCache()

	ctx := context.Background()
	mockClient.On("MGet", ctx, mock.Anything).Return(redisv9.NewSliceResult([]interface{}{"v1", nil, "v3"}, nil))

	req := &MultiGetRequest{
		Keys: []string{"k1", "k2", "k3"},
	}

	req.SetNamespace("testns")
	req.SetCollection("testcol")

	result, err := cache.MultiGet(context.Background(), req)

	assert.NoError(t, err)
	assert.Equal(t, "v1", result["k1"])
	assert.Equal(t, "v3", result["k3"])
	assert.Nil(t, result["k2"])

	mockRedis.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestRedisCache_MultiGet_Error(t *testing.T) {
	cache, mockRedis, mockClient := setupRedisCache()

	ctx := context.Background()
	mockClient.On("MGet", ctx, mock.Anything).Return(redisv9.NewSliceResult(nil, errors.New("boom")))

	req := &MultiGetRequest{Keys: []string{"k1"}}
	req.SetNamespace("testns")
	req.SetCollection("testcol")

	result, err := cache.MultiGet(context.Background(), req)
	assert.Error(t, err)
	assert.Nil(t, result)

	mockRedis.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestRedisCache_MultiSet(t *testing.T) {
	cache, mockRedis, mockClient := setupRedisCache()

	ctx := context.Background()
	orig := redisPipelineSet
	t.Cleanup(func() { redisPipelineSet = orig })
	redisPipelineSet = func(gotCtx context.Context, client redis.RedisClientInterface, commands []redisSetCommand) ([]*redisv9.StatusCmd, error) {
		assert.Equal(t, ctx, gotCtx)
		assert.Equal(t, mockClient, client)
		assert.Len(t, commands, 2)

		seen := make(map[string]redisSetCommand, len(commands))
		for _, command := range commands {
			seen[command.key] = command
			assert.Equal(t, 3600*time.Second, command.expiration)
		}
		assert.Equal(t, []byte("v1"), seen["testns:testcol:k1"].value)
		assert.Equal(t, []byte("v2"), seen["testns:testcol:k2"].value)

		return []*redisv9.StatusCmd{
			redisv9.NewStatusResult("OK", nil),
			redisv9.NewStatusResult("OK", nil),
		}, nil
	}

	req := &MultiSetRequest{
		ValueMap: map[string]any{
			"k1": "v1",
			"k2": "v2",
		},
		TTL: 3600,
	}

	req.SetNamespace("testns")
	req.SetCollection("testcol")

	result, err := cache.MultiSet(context.Background(), req)

	assert.NoError(t, err)
	assert.True(t, result["k1"])
	assert.True(t, result["k2"])

	mockRedis.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestRedisCache_MultiSet_SerialiseError(t *testing.T) {
	cache, mockRedis, mockClient := setupRedisCache()

	orig := redisPipelineSet
	t.Cleanup(func() { redisPipelineSet = orig })
	redisPipelineSet = func(ctx context.Context, client redis.RedisClientInterface, commands []redisSetCommand) ([]*redisv9.StatusCmd, error) {
		assert.Equal(t, mockClient, client)
		assert.Len(t, commands, 1)
		assert.Equal(t, "testns:testcol:k2", commands[0].key)
		assert.Equal(t, time.Duration(0), commands[0].expiration)
		return []*redisv9.StatusCmd{
			redisv9.NewStatusResult("OK", nil),
		}, nil
	}

	req := &MultiSetRequest{
		ValueMap: map[string]any{
			"k1": make(chan int),
			"k2": "v2",
		},
	}
	req.SetNamespace("testns")
	req.SetCollection("testcol")

	result, err := cache.MultiSet(context.Background(), req)
	assert.Error(t, err)
	assert.False(t, result["k1"])
	assert.True(t, result["k2"])

	mockRedis.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestRedisCache_MultiSet_Error(t *testing.T) {
	cache, mockRedis, mockClient := setupRedisCache()

	orig := redisPipelineSet
	t.Cleanup(func() { redisPipelineSet = orig })
	redisPipelineSet = func(ctx context.Context, client redis.RedisClientInterface, commands []redisSetCommand) ([]*redisv9.StatusCmd, error) {
		assert.Equal(t, mockClient, client)
		assert.Len(t, commands, 1)
		return []*redisv9.StatusCmd{
			redisv9.NewStatusResult("", errors.New("boom")),
		}, errors.New("boom")
	}

	req := &MultiSetRequest{
		ValueMap: map[string]any{
			"k1": "v1",
		},
	}
	req.SetNamespace("testns")
	req.SetCollection("testcol")

	result, err := cache.MultiSet(context.Background(), req)
	assert.Error(t, err)
	assert.False(t, result["k1"])

	mockRedis.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestRedisCache_MultiSet_CommandErrorWithoutPipelineError(t *testing.T) {
	cache, mockRedis, mockClient := setupRedisCache()

	orig := redisPipelineSet
	t.Cleanup(func() { redisPipelineSet = orig })
	redisPipelineSet = func(ctx context.Context, client redis.RedisClientInterface, commands []redisSetCommand) ([]*redisv9.StatusCmd, error) {
		assert.Equal(t, mockClient, client)
		return []*redisv9.StatusCmd{
			redisv9.NewStatusResult("", errors.New("command failed")),
		}, nil
	}

	req := &MultiSetRequest{ValueMap: map[string]any{"k1": "v1"}}
	req.SetNamespace("testns")
	req.SetCollection("testcol")

	result, err := cache.MultiSet(context.Background(), req)
	assert.Error(t, err)
	assert.False(t, result["k1"])

	mockRedis.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestRedisCache_MultiSet_MissingStatus(t *testing.T) {
	cache, mockRedis, mockClient := setupRedisCache()

	orig := redisPipelineSet
	t.Cleanup(func() { redisPipelineSet = orig })
	redisPipelineSet = func(ctx context.Context, client redis.RedisClientInterface, commands []redisSetCommand) ([]*redisv9.StatusCmd, error) {
		assert.Equal(t, mockClient, client)
		return []*redisv9.StatusCmd{nil}, nil
	}

	req := &MultiSetRequest{ValueMap: map[string]any{"k1": "v1"}}
	req.SetNamespace("testns")
	req.SetCollection("testcol")

	result, err := cache.MultiSet(context.Background(), req)
	assert.NoError(t, err)
	assert.False(t, result["k1"])

	mockRedis.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestRedisCache_MultiSet_NoSerializableCommands(t *testing.T) {
	cache, _, _ := setupRedisCache()

	req := &MultiSetRequest{ValueMap: map[string]any{"k1": make(chan int)}}
	req.SetNamespace("testns")
	req.SetCollection("testcol")

	result, err := cache.MultiSet(context.Background(), req)
	assert.Error(t, err)
	assert.False(t, result["k1"])

}

func TestRedisPipelineSet(t *testing.T) {
	mockClient := new(mocks.MockRedisClient)
	statuses, err := redisPipelineSet(context.Background(), mockClient, nil)
	assert.Nil(t, statuses)
	assert.ErrorContains(t, err, "does not support pipelining")

	client := redisv9.NewClient(&redisv9.Options{
		Addr:         "127.0.0.1:0",
		DialTimeout:  time.Millisecond,
		ReadTimeout:  time.Millisecond,
		WriteTimeout: time.Millisecond,
	})
	defer func() {
		assert.NoError(t, client.Close())
	}()

	statuses, err = redisPipelineSet(context.Background(), client, []redisSetCommand{
		{key: "k", value: []byte("v"), expiration: time.Second},
	})
	assert.Len(t, statuses, 1)
	assert.Error(t, err)
}

func TestRedisCache_MultiDelete(t *testing.T) {
	cache, mockRedis, mockClient := setupRedisCache()

	ctx := context.Background()
	mockClient.On("Del", ctx, []string{"testns:testcol:k1"}).Return(redisv9.NewIntResult(1, nil)).Once()
	mockClient.On("Del", ctx, []string{"testns:testcol:k2"}).Return(redisv9.NewIntResult(1, nil)).Once()

	req := &MultiDeleteRequest{
		Keys: []string{"k1", "k2"},
	}

	req.SetNamespace("testns")
	req.SetCollection("testcol")

	result, err := cache.MultiDelete(context.Background(), req)

	assert.NoError(t, err)
	assert.True(t, result["k1"])
	assert.True(t, result["k2"])

	mockRedis.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestRedisCache_MultiDelete_PartialMiss(t *testing.T) {
	cache, mockRedis, mockClient := setupRedisCache()

	ctx := context.Background()
	mockClient.On("Del", ctx, []string{"testns:testcol:k1"}).Return(redisv9.NewIntResult(1, nil)).Once()
	mockClient.On("Del", ctx, []string{"testns:testcol:k2"}).Return(redisv9.NewIntResult(0, nil)).Once()

	req := &MultiDeleteRequest{
		Keys: []string{"k1", "k2"},
	}

	req.SetNamespace("testns")
	req.SetCollection("testcol")

	result, err := cache.MultiDelete(context.Background(), req)

	assert.NoError(t, err)
	assert.True(t, result["k1"])
	assert.False(t, result["k2"])

	mockRedis.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestRedisCache_MultiDelete_Error(t *testing.T) {
	cache, mockRedis, mockClient := setupRedisCache()

	ctx := context.Background()
	mockClient.On("Del", ctx, []string{"testns:testcol:k1"}).Return(redisv9.NewIntResult(0, errors.New("delete failed"))).Once()

	req := &MultiDeleteRequest{Keys: []string{"k1"}}
	req.SetNamespace("testns")
	req.SetCollection("testcol")

	result, err := cache.MultiDelete(context.Background(), req)
	assert.Error(t, err)
	assert.False(t, result["k1"])

	mockRedis.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestRedisCache_ContextMethodsNilContext(t *testing.T) {
	cache, mockRedis, mockClient := setupRedisCache()
	ctx := context.Background()
	var nilCtx context.Context

	mockClient.On("Exists", ctx, mock.Anything).Return(redisv9.NewIntResult(1, nil)).Once()
	mockClient.On("Get", ctx, mock.Anything).Return(redisv9.NewStringResult("value", nil)).Once()
	mockClient.On("Set", ctx, mock.Anything, mock.Anything, time.Second).Return(redisv9.NewStatusResult("OK", nil)).Once()
	mockClient.On("Del", ctx, []string{"testns:testcol:k"}).Return(redisv9.NewIntResult(1, nil)).Twice()
	mockClient.On("MGet", ctx, mock.Anything).Return(redisv9.NewSliceResult([]interface{}{"v"}, nil)).Once()
	mockClient.On("IncrBy", ctx, mock.Anything, int64(1)).Return(redisv9.NewIntResult(1, nil)).Once()
	mockClient.On("DecrBy", ctx, mock.Anything, int64(1)).Return(redisv9.NewIntResult(0, nil)).Once()
	mockClient.On("Append", ctx, mock.Anything, mock.Anything).Return(redisv9.NewIntResult(2, nil)).Once()
	mockClient.On("TTL", ctx, mock.Anything).Return(redisv9.NewDurationResult(time.Second, nil)).Once()
	mockClient.On("Expire", ctx, mock.Anything, time.Second).Return(redisv9.NewBoolResult(true, nil)).Once()

	orig := redisPipelineSet
	t.Cleanup(func() { redisPipelineSet = orig })
	redisPipelineSet = func(gotCtx context.Context, client redis.RedisClientInterface, commands []redisSetCommand) ([]*redisv9.StatusCmd, error) {
		assert.Equal(t, ctx, gotCtx)
		assert.Equal(t, mockClient, client)
		assert.Len(t, commands, 1)
		return []*redisv9.StatusCmd{redisv9.NewStatusResult("OK", nil)}, nil
	}

	assert.True(t, mustBool(cache.Exists(nilCtx, &ExistsRequest{cacheRequest: cacheRequest{Namespace: "testns", Collection: "testcol"}, Key: "k"})))
	assert.Equal(t, []byte("value"), mustAny(cache.Get(nilCtx, &GetRequest{cacheRequest: cacheRequest{Namespace: "testns", Collection: "testcol"}, Key: "k"})))
	assert.True(t, mustBool(cache.Set(nilCtx, &SetRequest{cacheRequest: cacheRequest{Namespace: "testns", Collection: "testcol"}, Key: "k", Value: "v", TTL: 1})))
	assert.True(t, mustBool(cache.Delete(nilCtx, &DeleteRequest{cacheRequest: cacheRequest{Namespace: "testns", Collection: "testcol"}, Key: "k"})))
	assert.NotNil(t, mustMap(cache.MultiGet(nilCtx, &MultiGetRequest{cacheRequest: cacheRequest{Namespace: "testns", Collection: "testcol"}, Keys: []string{"k"}})))
	assert.NotNil(t, mustMapBool(cache.MultiSet(nilCtx, &MultiSetRequest{cacheRequest: cacheRequest{Namespace: "testns", Collection: "testcol"}, ValueMap: map[string]any{"k": "v"}})))
	assert.NotNil(t, mustMapBool(cache.MultiDelete(nilCtx, &MultiDeleteRequest{cacheRequest: cacheRequest{Namespace: "testns", Collection: "testcol"}, Keys: []string{"k"}})))
	assert.NoError(t, cache.Increment(nilCtx, &IncrementRequest{cacheRequest: cacheRequest{Namespace: "testns", Collection: "testcol"}, Key: "k", Value: 1}))
	assert.NoError(t, cache.Decrement(nilCtx, &DecrementRequest{cacheRequest: cacheRequest{Namespace: "testns", Collection: "testcol"}, Key: "k", Value: 1}))
	assert.NoError(t, cache.Append(nilCtx, &AppendRequest{cacheRequest: cacheRequest{Namespace: "testns", Collection: "testcol"}, Key: "k", Value: "v"}))
	ttl, err := cache.GetTTL(nilCtx, &GetTTLRequest{cacheRequest: cacheRequest{Namespace: "testns", Collection: "testcol"}, Key: "k"})
	assert.NoError(t, err)
	assert.Equal(t, int64(1), ttl)
	assert.NoError(t, cache.SetTTL(nilCtx, &SetTTLRequest{cacheRequest: cacheRequest{Namespace: "testns", Collection: "testcol"}, Key: "k", TTL: 1}))

	mockRedis.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestRedisCache_ContextMethodsCanceled(t *testing.T) {
	mockRedis := new(mockRedis)
	cache := &RedisCache{
		client:     mockRedis,
		namespace:  "testns",
		collection: "testcol",
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	request := cacheRequest{Namespace: "testns", Collection: "testcol"}

	calls := []struct {
		name string
		run  func() error
	}{
		{"Exists", func() error {
			_, err := cache.Exists(ctx, &ExistsRequest{cacheRequest: request, Key: "k"})
			return err
		}},
		{"Get", func() error {
			_, err := cache.Get(ctx, &GetRequest{cacheRequest: request, Key: "k"})
			return err
		}},
		{"Set", func() error {
			_, err := cache.Set(ctx, &SetRequest{cacheRequest: request, Key: "k", Value: "v"})
			return err
		}},
		{"Delete", func() error {
			_, err := cache.Delete(ctx, &DeleteRequest{cacheRequest: request, Key: "k"})
			return err
		}},
		{"MultiGet", func() error {
			_, err := cache.MultiGet(ctx, &MultiGetRequest{cacheRequest: request, Keys: []string{"k"}})
			return err
		}},
		{"MultiSet", func() error {
			_, err := cache.MultiSet(ctx, &MultiSetRequest{cacheRequest: request, ValueMap: map[string]any{"k": "v"}})
			return err
		}},
		{"MultiDelete", func() error {
			_, err := cache.MultiDelete(ctx, &MultiDeleteRequest{cacheRequest: request, Keys: []string{"k"}})
			return err
		}},
		{"Increment", func() error {
			return cache.Increment(ctx, &IncrementRequest{cacheRequest: request, Key: "k", Value: 1})
		}},
		{"Decrement", func() error {
			return cache.Decrement(ctx, &DecrementRequest{cacheRequest: request, Key: "k", Value: 1})
		}},
		{"Append", func() error {
			return cache.Append(ctx, &AppendRequest{cacheRequest: request, Key: "k", Value: "v"})
		}},
		{"GetTTL", func() error {
			_, err := cache.GetTTL(ctx, &GetTTLRequest{cacheRequest: request, Key: "k"})
			return err
		}},
		{"SetTTL", func() error {
			return cache.SetTTL(ctx, &SetTTLRequest{cacheRequest: request, Key: "k", TTL: 1})
		}},
	}

	for _, tc := range calls {
		t.Run(tc.name, func(t *testing.T) {
			assert.ErrorIs(t, tc.run(), context.Canceled)
		})
	}
	mockRedis.AssertNotCalled(t, "GetClient")
}

func TestRedisCache_Increment(t *testing.T) {
	cache, mockRedis, mockClient := setupRedisCache()

	ctx := context.Background()
	mockClient.On("IncrBy", ctx, mock.Anything, int64(5)).Return(redisv9.NewIntResult(5, nil))

	req := &IncrementRequest{
		Key:   "k",
		Value: 5,
	}

	req.SetNamespace("testns")
	req.SetCollection("testcol")

	err := cache.Increment(context.Background(), req)

	assert.NoError(t, err)

	mockRedis.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestRedisCache_Decrement(t *testing.T) {
	cache, mockRedis, mockClient := setupRedisCache()

	ctx := context.Background()
	mockClient.On("DecrBy", ctx, mock.Anything, int64(3)).Return(redisv9.NewIntResult(2, nil))

	req := &DecrementRequest{
		Key:   "k",
		Value: 3,
	}

	req.SetNamespace("testns")
	req.SetCollection("testcol")

	err := cache.Decrement(context.Background(), req)

	assert.NoError(t, err)

	mockRedis.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestRedisCache_Append(t *testing.T) {
	cache, mockRedis, mockClient := setupRedisCache()

	ctx := context.Background()
	mockClient.On("Append", ctx, mock.Anything, mock.Anything).Return(redisv9.NewIntResult(6, nil))

	req := &AppendRequest{
		Key:   "k",
		Value: "suffix",
	}

	req.SetNamespace("testns")
	req.SetCollection("testcol")

	err := cache.Append(context.Background(), req)

	assert.NoError(t, err)

	mockRedis.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestRedisCache_Append_Error(t *testing.T) {
	cache, mockRedis, mockClient := setupRedisCache()

	ctx := context.Background()
	mockClient.On("Append", ctx, mock.Anything, mock.Anything).Return(redisv9.NewIntResult(0, errors.New("boom")))

	req := &AppendRequest{
		Key:   "k",
		Value: "v",
	}
	req.SetNamespace("testns")
	req.SetCollection("testcol")

	err := cache.Append(context.Background(), req)
	assert.Error(t, err)

	mockRedis.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestRedisCache_Append_SerialiseError(t *testing.T) {
	cache, _, _ := setupRedisCache()

	orig := redisSerializeValue
	t.Cleanup(func() { redisSerializeValue = orig })
	redisSerializeValue = func(*RedisCache, any) ([]byte, error) {
		return nil, errors.New("bad serialize")
	}

	req := &AppendRequest{
		Key:   "k",
		Value: "v",
	}
	req.SetNamespace("testns")
	req.SetCollection("testcol")

	err := cache.Append(context.Background(), req)
	assert.Error(t, err)
}

func TestRedisCache_GetTTL(t *testing.T) {
	cache, mockRedis, mockClient := setupRedisCache()

	ctx := context.Background()
	mockClient.On("TTL", ctx, mock.Anything).Return(redisv9.NewDurationResult(120*time.Second, nil))

	req := &GetTTLRequest{
		Key: "k",
	}

	req.SetNamespace("testns")
	req.SetCollection("testcol")

	ttl, err := cache.GetTTL(context.Background(), req)

	assert.NoError(t, err)
	assert.Equal(t, int64(120), ttl)

	mockRedis.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestRedisCache_GetTTL_Error(t *testing.T) {
	cache, mockRedis, mockClient := setupRedisCache()

	ctx := context.Background()
	mockClient.On("TTL", ctx, mock.Anything).Return(redisv9.NewDurationResult(0, errors.New("boom")))

	req := &GetTTLRequest{Key: "k"}
	req.SetNamespace("testns")
	req.SetCollection("testcol")

	ttl, err := cache.GetTTL(context.Background(), req)
	assert.Error(t, err)
	assert.Equal(t, int64(0), ttl)

	mockRedis.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestRedisCache_SetTTL(t *testing.T) {
	cache, mockRedis, mockClient := setupRedisCache()

	ctx := context.Background()
	mockClient.On("Expire", ctx, mock.Anything, mock.Anything).Return(redisv9.NewBoolResult(true, nil))

	req := &SetTTLRequest{
		Key: "k",
		TTL: 300,
	}

	req.SetNamespace("testns")
	req.SetCollection("testcol")

	err := cache.SetTTL(context.Background(), req)

	assert.NoError(t, err)

	mockRedis.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestRedisGetSerialisedValue(t *testing.T) {
	cache := &RedisCache{}

	t.Run("string input", func(t *testing.T) {
		input := "hello"
		expected := []byte("hello")

		result, err := cache.getSerialisedValue(input)

		assert.NoError(t, err)
		assert.Equal(t, expected, result)
	})

	t.Run("[]byte input", func(t *testing.T) {
		input := []byte("world")
		expected := []byte("world")

		result, err := cache.getSerialisedValue(input)

		assert.NoError(t, err)
		assert.Equal(t, expected, result)
	})

	t.Run("json serializable struct input", func(t *testing.T) {
		input := struct {
			Name string `json:"name"`
		}{
			Name: "test",
		}

		expected, _ := json.Marshal(input)

		result, err := cache.getSerialisedValue(input)

		assert.NoError(t, err)
		assert.Equal(t, expected, result)
	})

	t.Run("non json serializable input (channels, funcs)", func(t *testing.T) {
		input := make(chan int) // Channels cannot be marshaled to JSON

		result, err := cache.getSerialisedValue(input)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.EqualError(t, err, "value should be one of {string, byteArray, json serialisable} datatype")
	})
}

type badRedisConnector struct{}

func (b badRedisConnector) New(opt *redisv9.Options) redis.RedisInterface {
	return nil
}

func TestNewRedisCache_Error(t *testing.T) {
	_, err := NewRedisCache(&Options{
		Provider:    ProviderRedis,
		Cluster:     "test-cluster-bad",
		Namespace:   "test-ns",
		Collection:  "test-col",
		Hosts:       []string{"127.0.0.1:6379"},
		ConnTimeout: 5,
		DefaultTTL:  60,
	}, badRedisConnector{})
	assert.Error(t, err)
}
