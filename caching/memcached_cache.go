package caching

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/cshekharsharma/photon/storage/memcached" // Update to your actual storage import
)

// MemcachedCache provides a caching layer backed by Memcached.
//
// It implements the generic Cache interface and supports basic cache operations
// like Get, Set, Delete, Increment, Decrement, Append, and batch operations.
//
// Keys are internally formatted using a "namespace:collection:key" pattern
// to allow logical grouping and separation across different applications
// or modules sharing the same Memcached cluster.
//
// MemcachedCache ensures efficient use of connection pooling,
// safe concurrent access, and serializes non-string/[]byte values to JSON
// before storing.
//
// Limitations:
//   - Native TTL retrieval (GetTTL) is not supported by Memcached.
//   - Values are always stored as byte slices ([]byte) internally.
type MemcachedCache struct {
	memc        memcached.MemcachedInterface
	clusterName string
	namespace   string
	collection  string
}

var memcachedSerializeValue = func(c *MemcachedCache, val any) ([]byte, error) {
	return c.getSerialisedValue(val)
}

// NewMemcachedCache initializes a new MemcachedCache instance.
// It connects to the Memcached cluster specified in the options and prepares the client.
func NewMemcachedCache(opts *Options, connector memcached.MemcachedConnectorInterface) (*MemcachedCache, error) {
	memcached.SetConnectionConfig(opts.Cluster, &memcached.ConnectionConfig{
		Addresses: opts.Hosts,
		Timeout:   time.Duration(opts.ConnTimeout) * time.Second,
	})

	client, err := memcached.Connect(connector, opts.Cluster)
	if err != nil {
		return nil, err
	}

	return &MemcachedCache{
		memc:        client,
		clusterName: opts.Cluster,
		namespace:   opts.Namespace,
		collection:  opts.Collection,
	}, nil
}

// Exists checks if a given key exists in the cache.
// Returns true if the key is found, false otherwise.
func (c *MemcachedCache) Exists(ctx context.Context, request *ExistsRequest) (bool, error) {
	if _, err := checkedCacheContext(ctx); err != nil {
		return false, err
	}

	key := c.formatKey(request.Namespace, request.Collection, request.Key)
	_, err := c.memc.GetClient().Get(key)

	if err == memcache.ErrCacheMiss {
		return false, nil
	}

	if err != nil {
		return false, err
	}

	return true, nil
}

// Get retrieves the value associated with a given key.
// Returns nil if the key is not found.
func (c *MemcachedCache) Get(ctx context.Context, request *GetRequest) (any, error) {
	if _, err := checkedCacheContext(ctx); err != nil {
		return nil, err
	}

	key := c.formatKey(request.Namespace, request.Collection, request.Key)
	item, err := c.memc.GetClient().Get(key)

	if err == memcache.ErrCacheMiss {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	return item.Value, nil
}

// Set stores a value in the cache with optional TTL expiration.
// Returns true if the operation succeeds.
func (c *MemcachedCache) Set(ctx context.Context, request *SetRequest) (bool, error) {
	if _, err := checkedCacheContext(ctx); err != nil {
		return false, err
	}

	key := c.formatKey(request.Namespace, request.Collection, request.Key)
	expiration, err := memcachedTTL(request.TTL)
	if err != nil {
		return false, err
	}

	bytesValue, err := memcachedSerializeValue(c, request.Value)
	if err != nil {
		return false, err
	}

	item := &memcache.Item{
		Key:        key,
		Value:      bytesValue,
		Expiration: expiration,
	}

	err = c.memc.GetClient().Set(item)
	return (err == nil), err
}

// Delete removes a key from the cache.
// Returns true if the key was successfully deleted.
func (c *MemcachedCache) Delete(ctx context.Context, request *DeleteRequest) (bool, error) {
	if _, err := checkedCacheContext(ctx); err != nil {
		return false, err
	}

	key := c.formatKey(request.Namespace, request.Collection, request.Key)
	err := c.memc.GetClient().Delete(key)

	if err == memcache.ErrCacheMiss {
		return false, nil
	}

	return (err == nil), err
}

// MultiGet retrieves multiple values for a list of keys.
// Returns a map of keys to values, missing keys will be absent.
func (c *MemcachedCache) MultiGet(ctx context.Context, request *MultiGetRequest) (map[string]any, error) {
	if _, err := checkedCacheContext(ctx); err != nil {
		return nil, err
	}

	formattedKeys := make([]string, len(request.Keys))

	for i, key := range request.Keys {
		formattedKeys[i] = c.formatKey(request.Namespace, request.Collection, key)
	}

	items, err := c.memc.GetClient().GetMulti(formattedKeys)
	if err != nil {
		return nil, err
	}

	result := make(map[string]any)
	for k, v := range items {
		// Extract original key
		parts := strings.SplitN(k, ":", 3)
		originalKey := parts[len(parts)-1]
		result[originalKey] = v.Value
	}

	return result, nil
}

// MultiSet sets multiple key-value pairs into the cache in batch.
// Returns a map of success status per key.
func (c *MemcachedCache) MultiSet(ctx context.Context, request *MultiSetRequest) (map[string]bool, error) {
	if _, err := checkedCacheContext(ctx); err != nil {
		return nil, err
	}
	expiration, err := memcachedTTL(request.TTL)
	if err != nil {
		return nil, err
	}

	result := make(map[string]bool)
	var lastError error

	for key, value := range request.ValueMap {
		bytesValue, err := memcachedSerializeValue(c, value)
		if err != nil {
			result[key] = false
			lastError = err
			continue
		}

		item := &memcache.Item{
			Key:        c.formatKey(request.Namespace, request.Collection, key),
			Value:      bytesValue,
			Expiration: expiration,
		}

		err = c.memc.GetClient().Set(item)
		if err != nil {
			result[key] = false
			lastError = err
		} else {
			result[key] = true
		}
	}

	return result, lastError
}

// MultiDelete deletes multiple keys from the cache.
// Returns a map of success status per key.
func (c *MemcachedCache) MultiDelete(ctx context.Context, request *MultiDeleteRequest) (map[string]bool, error) {
	if _, err := checkedCacheContext(ctx); err != nil {
		return nil, err
	}

	result := make(map[string]bool)

	for _, key := range request.Keys {
		formattedKey := c.formatKey(request.Namespace, request.Collection, key)
		err := c.memc.GetClient().Delete(formattedKey)

		switch err {
		case nil:
			result[key] = true
		case memcache.ErrCacheMiss:
			result[key] = false
		default:
			return nil, err
		}
	}

	return result, nil
}

// Increment atomically increases a key's numeric value by the specified amount.
// Returns an error if the operation fails or key doesn't exist.
func (c *MemcachedCache) Increment(ctx context.Context, request *IncrementRequest) error {
	if _, err := checkedCacheContext(ctx); err != nil {
		return err
	}

	key := c.formatKey(request.Namespace, request.Collection, request.Key)
	delta, err := memcachedDelta(request.Value)
	if err != nil {
		return err
	}

	_, err = c.memc.GetClient().Increment(key, delta)
	if err != nil {
		return err
	}

	return nil
}

// Decrement atomically decreases a key's numeric value by the specified amount.
// Returns an error if the operation fails or key doesn't exist.
func (c *MemcachedCache) Decrement(ctx context.Context, request *DecrementRequest) error {
	if _, err := checkedCacheContext(ctx); err != nil {
		return err
	}

	key := c.formatKey(request.Namespace, request.Collection, request.Key)
	delta, err := memcachedDelta(request.Value)
	if err != nil {
		return err
	}

	_, err = c.memc.GetClient().Decrement(key, delta)
	if err != nil {
		return err
	}

	return nil
}

// Append appends the given value to an existing key's value.
// Returns an error if the key does not exist or append fails.
func (c *MemcachedCache) Append(ctx context.Context, request *AppendRequest) error {
	if _, err := checkedCacheContext(ctx); err != nil {
		return err
	}

	key := c.formatKey(request.Namespace, request.Collection, request.Key)

	valBytes, err := memcachedSerializeValue(c, request.Value)
	if err != nil {
		return err
	}

	err = c.memc.GetClient().Append(&memcache.Item{
		Key:   key,
		Value: valBytes,
	})

	if err != nil {
		return err
	}

	return nil
}

// GetTTL is not supported in Memcached.
// Always returns an error indicating unsupported operation.
func (c *MemcachedCache) GetTTL(ctx context.Context, request *GetTTLRequest) (int64, error) {
	if _, err := checkedCacheContext(ctx); err != nil {
		return 0, err
	}

	return 0, errors.New("GetTTL not supported in Memcached")
}

// SetTTL updates the expiration time (TTL) of an existing key.
// Internally uses the Memcached Touch() operation.
func (c *MemcachedCache) SetTTL(ctx context.Context, request *SetTTLRequest) error {
	if _, err := checkedCacheContext(ctx); err != nil {
		return err
	}

	key := c.formatKey(request.Namespace, request.Collection, request.Key)
	expiration, err := memcachedTTL(request.TTL)
	if err != nil {
		return err
	}
	return c.memc.GetClient().Touch(key, expiration)
}

// formatKey constructs a full key using namespace, collection, and user key.
// Ensures uniqueness and separation across logical groupings.
func (c *MemcachedCache) formatKey(namespace, collection, key string) string {
	return fmt.Sprintf("%s:%s:%s", namespace, collection, key)
}

// getSerialisedValue converts a value to []byte for storage.
// Accepts string, []byte, or JSON-serializable objects.
func (c *MemcachedCache) getSerialisedValue(val any) ([]byte, error) {
	var bytesValue []byte

	switch v := val.(type) {
	case []byte:
		bytesValue = v

	case string:
		bytesValue = []byte(v)

	default:
		var jsonErr error
		bytesValue, jsonErr = (json.Marshal(val))
		if jsonErr != nil {
			return bytesValue, errors.New("value should be one of {string, byteArray, json serialisable} datatype")
		}
	}

	return bytesValue, nil
}

func memcachedTTL(ttl int64) (int32, error) {
	if ttl < 0 || ttl > math.MaxInt32 {
		return 0, fmt.Errorf("memcached: TTL out of range")
	}
	return int32(ttl), nil // #nosec G115 -- range checked above.
}

func memcachedDelta(value int64) (uint64, error) {
	if value < 0 {
		return 0, fmt.Errorf("memcached: delta cannot be negative")
	}
	return uint64(value), nil // #nosec G115 -- range checked above.
}
