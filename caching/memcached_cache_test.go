package caching

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"testing"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/cshekharsharma/photon/storage/memcached"
	"github.com/cshekharsharma/photon/utils/testutil/mocks"
	"github.com/cshekharsharma/photon/utils/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// --- Mocks --- //
type MockMemcachedConnector struct {
	mock.Mock
}

func (m *MockMemcachedConnector) New(server ...string) memcached.MemcachedInterface {
	args := m.Called(server)
	if args.Get(0) == nil {
		return nil
	}
	return args.Get(0).(memcached.MemcachedInterface)
}

type MockMemcached struct {
	mock.Mock
}

func (m *MockMemcached) GetClient() memcached.MemcachedClientInterface {
	args := m.Called()
	return args.Get(0).(memcached.MemcachedClientInterface)
}

func (m *MockMemcached) SetClient(client memcached.MemcachedClientInterface) {
	m.Called(client)
}

func (m *MockMemcached) Close() error {
	args := m.Called()
	return args.Error(0)
}

// --- Helper to create test cache instance --- //
func setupTestCache(t *testing.T) (*MemcachedCache, *mocks.MockMemcachedClient) {
	t.Helper()

	mockConnector := new(MockMemcachedConnector)
	mockMemcached := new(MockMemcached)
	mockClient := new(mocks.MockMemcachedClient)
	mockMemcached.On("GetClient").Return(mockClient)
	mockConnector.On("New", mock.Anything).Return(mockMemcached)

	randomString, _ := types.GetCryptoSafeRandomString(16)
	mcache, _ := NewMemcachedCache(&Options{
		Provider:    ProviderMemcached,
		Hosts:       []string{"127.0.0.1:11911"},
		ConnTimeout: 10,
		DefaultTTL:  100,
		Cluster:     "test-cluster" + randomString,
		Namespace:   "test-ns" + randomString,
		Collection:  "test-coll" + randomString,
	}, mockConnector)

	return mcache, mockClient
}

func TestMemcachedExists_Success(t *testing.T) {
	cache, client := setupTestCache(t)
	client.On("Get", mock.Anything).Return(&memcache.Item{Value: []byte("val")}, nil)
	exists, err := cache.Exists(context.Background(), &ExistsRequest{Key: "key"})
	assert.NoError(t, err)
	assert.True(t, exists)
}

func TestMemcachedExists_Error(t *testing.T) {
	cache, client := setupTestCache(t)
	client.On("Get", mock.Anything).Return(nil, errors.New("some error"))
	exists, err := cache.Exists(context.Background(), &ExistsRequest{Key: "key"})
	assert.Error(t, err)
	assert.False(t, exists)
}

func TestMemcachedExists_CacheMiss(t *testing.T) {
	cache, client := setupTestCache(t)
	client.On("Get", mock.Anything).Return(nil, memcache.ErrCacheMiss)
	exists, err := cache.Exists(context.Background(), &ExistsRequest{Key: "key"})
	assert.NoError(t, err)
	assert.False(t, exists)
}

func TestMemcachedGet_Success(t *testing.T) {
	cache, client := setupTestCache(t)
	client.On("Get", mock.Anything).Return(&memcache.Item{Value: []byte("hello")}, nil)
	val, err := cache.Get(context.Background(), &GetRequest{Key: "key"})
	assert.NoError(t, err)
	assert.Equal(t, []byte("hello"), val)
}

func TestMemcachedGet_Error(t *testing.T) {
	cache, client := setupTestCache(t)
	client.On("Get", mock.Anything).Return(nil, errors.New("error"))
	val, err := cache.Get(context.Background(), &GetRequest{Key: "key"})
	assert.Error(t, err)
	assert.Nil(t, val)
}

func TestMemcachedGet_CacheMiss(t *testing.T) {
	cache, client := setupTestCache(t)
	client.On("Get", mock.Anything).Return(nil, memcache.ErrCacheMiss)
	val, err := cache.Get(context.Background(), &GetRequest{Key: "key"})
	assert.NoError(t, err)
	assert.Nil(t, val)
}

func TestMemcachedSet_Success(t *testing.T) {
	cache, client := setupTestCache(t)
	client.On("Set", mock.Anything).Return(nil)
	success, err := cache.Set(context.Background(), &SetRequest{Key: "key", Value: "val"})
	assert.NoError(t, err)
	assert.True(t, success)
}

func TestMemcachedSet_Error(t *testing.T) {
	cache, client := setupTestCache(t)
	client.On("Set", mock.Anything).Return(errors.New("error"))
	success, err := cache.Set(context.Background(), &SetRequest{Key: "key", Value: "val"})
	assert.Error(t, err)
	assert.False(t, success)
}

func TestMemcachedSet_SerialiseError(t *testing.T) {
	cache, _ := setupTestCache(t)

	orig := memcachedSerializeValue
	t.Cleanup(func() { memcachedSerializeValue = orig })
	memcachedSerializeValue = func(*MemcachedCache, any) ([]byte, error) {
		return nil, errors.New("bad serialize")
	}

	success, err := cache.Set(context.Background(), &SetRequest{Key: "key", Value: "val"})
	assert.Error(t, err)
	assert.False(t, success)
}

func TestMemcachedNumericRangeValidation(t *testing.T) {
	cache, _ := setupTestCache(t)

	if ok, err := cache.Set(context.Background(), &SetRequest{Key: "key", Value: "val", TTL: int64(math.MaxInt32) + 1}); err == nil || ok {
		t.Fatalf("expected out-of-range Set TTL to fail, ok=%v err=%v", ok, err)
	}
	if _, err := cache.MultiSet(context.Background(), &MultiSetRequest{ValueMap: map[string]any{"k": "v"}, TTL: -1}); err == nil {
		t.Fatalf("expected negative MultiSet TTL to fail")
	}
	if err := cache.Increment(context.Background(), &IncrementRequest{Key: "key", Value: -1}); err == nil {
		t.Fatalf("expected negative increment to fail")
	}
	if err := cache.Decrement(context.Background(), &DecrementRequest{Key: "key", Value: -1}); err == nil {
		t.Fatalf("expected negative decrement to fail")
	}
	if err := cache.SetTTL(context.Background(), &SetTTLRequest{Key: "key", TTL: int64(math.MaxInt32) + 1}); err == nil {
		t.Fatalf("expected out-of-range SetTTL to fail")
	}
}

func TestMemcachedDelete_Success(t *testing.T) {
	cache, client := setupTestCache(t)
	client.On("Delete", mock.Anything).Return(nil)
	ok, err := cache.Delete(context.Background(), &DeleteRequest{Key: "key"})
	assert.NoError(t, err)
	assert.True(t, ok)
}

func TestMemcachedDelete_Error(t *testing.T) {
	cache, client := setupTestCache(t)
	client.On("Delete", mock.Anything).Return(errors.New("error"))
	ok, err := cache.Delete(context.Background(), &DeleteRequest{Key: "key"})
	assert.Error(t, err)
	assert.False(t, ok)
}

func TestMemcachedDelete_CacheMiss(t *testing.T) {
	cache, client := setupTestCache(t)
	client.On("Delete", mock.Anything).Return(memcache.ErrCacheMiss)
	ok, err := cache.Delete(context.Background(), &DeleteRequest{Key: "key"})
	assert.NoError(t, err)
	assert.False(t, ok)
}

func TestMemcachedMultiGet_Success(t *testing.T) {
	cache, client := setupTestCache(t)
	client.On("GetMulti", mock.Anything).Return(map[string]*memcache.Item{"k1": {Value: []byte("v1")}}, nil)
	res, err := cache.MultiGet(context.Background(), &MultiGetRequest{Keys: []string{"k1"}})
	assert.NoError(t, err)
	assert.Equal(t, []byte("v1"), res["k1"])
}

func TestMemcachedMultiGet_Error(t *testing.T) {
	cache, client := setupTestCache(t)
	client.On("GetMulti", mock.Anything).Return(nil, errors.New("error"))
	res, err := cache.MultiGet(context.Background(), &MultiGetRequest{Keys: []string{"k1"}})
	assert.Error(t, err)
	assert.Nil(t, res)
}

func TestMemcachedMultiSet_Success(t *testing.T) {
	cache, client := setupTestCache(t)
	client.On("Set", mock.Anything).Return(nil)
	res, err := cache.MultiSet(context.Background(), &MultiSetRequest{ValueMap: map[string]any{"k1": "v1"}})
	assert.NoError(t, err)
	assert.True(t, res["k1"])
}

func TestMemcachedMultiSet_Error(t *testing.T) {
	cache, client := setupTestCache(t)
	client.On("Set", mock.Anything).Return(errors.New("error"))
	res, err := cache.MultiSet(context.Background(), &MultiSetRequest{ValueMap: map[string]any{"k1": "v1"}})
	assert.Error(t, err)
	assert.False(t, res["k1"])
}

func TestMemcachedMultiSet_SerialiseError(t *testing.T) {
	cache, _ := setupTestCache(t)
	res, err := cache.MultiSet(context.Background(), &MultiSetRequest{ValueMap: map[string]any{"k1": make(chan int)}})
	assert.Error(t, err)
	assert.False(t, res["k1"])
}

func TestMemcachedMultiDelete_Success(t *testing.T) {
	cache, client := setupTestCache(t)
	client.On("Delete", mock.Anything).Return(nil)
	res, err := cache.MultiDelete(context.Background(), &MultiDeleteRequest{Keys: []string{"k1"}})
	assert.NoError(t, err)
	assert.True(t, res["k1"])
}

func TestMemcachedMultiDelete_Error(t *testing.T) {
	cache, client := setupTestCache(t)
	client.On("Delete", mock.Anything).Return(errors.New("error"))
	res, err := cache.MultiDelete(context.Background(), &MultiDeleteRequest{Keys: []string{"k1"}})
	assert.Error(t, err)
	assert.Nil(t, res)
}

func TestMemcachedMultiDelete_CacheMiss(t *testing.T) {
	cache, client := setupTestCache(t)
	client.On("Delete", mock.Anything).Return(memcache.ErrCacheMiss)
	res, err := cache.MultiDelete(context.Background(), &MultiDeleteRequest{Keys: []string{"k1"}})
	assert.NoError(t, err)
	assert.False(t, res["k1"])
}

func TestMemcachedIncrement_Success(t *testing.T) {
	cache, client := setupTestCache(t)
	client.On("Increment", mock.Anything, mock.Anything).Return(uint64(1), nil)
	err := cache.Increment(context.Background(), &IncrementRequest{Key: "key", Value: 1})
	assert.NoError(t, err)
}

func TestMemcachedIncrement_Error(t *testing.T) {
	cache, client := setupTestCache(t)
	client.On("Increment", mock.Anything, mock.Anything).Return(uint64(0), errors.New("error"))
	err := cache.Increment(context.Background(), &IncrementRequest{Key: "key", Value: 1})
	assert.Error(t, err)
}

func TestMemcachedDecrement_Success(t *testing.T) {
	cache, client := setupTestCache(t)
	client.On("Decrement", mock.Anything, mock.Anything).Return(uint64(1), nil)
	err := cache.Decrement(context.Background(), &DecrementRequest{Key: "key", Value: 1})
	assert.NoError(t, err)
}

func TestMemcachedDecrement_Error(t *testing.T) {
	cache, client := setupTestCache(t)
	client.On("Decrement", mock.Anything, mock.Anything).Return(uint64(0), errors.New("error"))
	err := cache.Decrement(context.Background(), &DecrementRequest{Key: "key", Value: 1})
	assert.Error(t, err)
}

func TestMemcachedAppend_Success(t *testing.T) {
	cache, client := setupTestCache(t)
	client.On("Append", mock.Anything).Return(nil)
	err := cache.Append(context.Background(), &AppendRequest{Key: "key", Value: "v"})
	assert.NoError(t, err)
}

func TestMemcachedAppend_Error(t *testing.T) {
	cache, client := setupTestCache(t)
	client.On("Append", mock.Anything).Return(errors.New("error"))
	err := cache.Append(context.Background(), &AppendRequest{Key: "key", Value: "v"})
	assert.Error(t, err)
}

func TestMemcachedAppend_SerialiseError(t *testing.T) {
	cache, _ := setupTestCache(t)

	orig := memcachedSerializeValue
	t.Cleanup(func() { memcachedSerializeValue = orig })
	memcachedSerializeValue = func(*MemcachedCache, any) ([]byte, error) {
		return nil, errors.New("bad serialize")
	}

	err := cache.Append(context.Background(), &AppendRequest{Key: "key", Value: "v"})
	assert.Error(t, err)
}

func TestMemcachedGetTTL_Unsupported(t *testing.T) {
	cache, _ := setupTestCache(t)
	_, err := cache.GetTTL(context.Background(), &GetTTLRequest{Key: "key"})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "GetTTL not supported")
}

func TestMemcachedContextMethodsCanceled(t *testing.T) {
	backend := new(MockMemcached)
	cache := &MemcachedCache{
		memc:       backend,
		namespace:  "test-ns",
		collection: "test-coll",
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	request := cacheRequest{Namespace: "test-ns", Collection: "test-coll"}

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
	backend.AssertNotCalled(t, "GetClient")
}

func TestMemcachedSetTTL_Success(t *testing.T) {
	cache, client := setupTestCache(t)
	client.On("Touch", mock.Anything, mock.Anything).Return(nil)
	err := cache.SetTTL(context.Background(), &SetTTLRequest{Key: "key", TTL: 10})
	assert.NoError(t, err)
}

func TestMemcachedSetTTL_Error(t *testing.T) {
	cache, client := setupTestCache(t)
	client.On("Touch", mock.Anything, mock.Anything).Return(errors.New("error"))
	err := cache.SetTTL(context.Background(), &SetTTLRequest{Key: "key", TTL: 10})
	assert.Error(t, err)
}

func TestMemcachedGetSerialisedValue(t *testing.T) {
	cache := &MemcachedCache{}

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

func TestNewMemcachedCache_Error(t *testing.T) {
	mockConnector := new(MockMemcachedConnector)
	mockConnector.On("New", mock.Anything).Return(nil)

	_, err := NewMemcachedCache(&Options{
		Provider:    ProviderMemcached,
		Hosts:       []string{"127.0.0.1:11911", "127.0.0.2:11911"},
		ConnTimeout: 10,
		DefaultTTL:  100,
		Cluster:     "test-cluster-error",
		Namespace:   "test-ns",
		Collection:  "test-coll",
	}, mockConnector)
	assert.Error(t, err)
}
