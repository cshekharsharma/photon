package caching

import (
	"context"
	"errors"
	"time"

	"github.com/cshekharsharma/photon/storage/flashdb"
	"github.com/cshekharsharma/photon/utils/types"
)

const FlashDbDefaultShardCount = 128               // sensible default for concurrency
const FlashDbMaxSizeInBytes = 1 << 30              // 1 GiB max storage size
const FlashDbDefaultJanitorEvery = 2 * time.Minute // default janitor interval

// flashDBStore defines the minimal interface we need from flashdb for our caching layer.
// This allows easier mocking/testing.
type flashDBStore interface {
	Close()
	Exists(key string) (bool, error)
	Get(key string) (any, error)
	Set(key string, value map[string]any, ttl time.Duration) (bool, error)
	Delete(key string) (bool, error)
	MultiGet(keys []string) (map[string]any, error)
	MultiDelete(keys []string) (map[string]bool, error)
	Increment(key string, fields map[string]int64) error
	Decrement(key string, fields map[string]int64) error
	Append(key string, fields map[string]string) error
	GetTTL(key string) (int64, error)
	SetTTL(key string, ttl time.Duration) error
}

// FlashDBCache is a cache layer built on top of flashdb.
// It implements the Cache interface and provides a simple in-memory cache.
type FlashDBCache struct {
	store      flashDBStore
	defaultTTL time.Duration // applied when SetRequest.TTL == 0
}

var flashdbNew = flashdb.New

// NewFlashDBCache builds an in-process cache using flashdb as the provider.
// We derive sensible flashdb defaults from caching.Options.
func NewFlashDBCache(opts Options) (*FlashDBCache, error) {
	fdbOpts := flashdb.Options{
		Shards:         FlashDbDefaultShardCount,
		MaxStorageSize: FlashDbMaxSizeInBytes,
		JanitorEvery:   FlashDbDefaultJanitorEvery,
		DefaultTTL:     secondsToDuration(types.MaxInt64(0, opts.DefaultTTL)),
		LFUDepth:       4,
		LFUWidth:       1 << 16, // 65536
		LFUAgingEvery:  200_000, // 200k ops
	}

	store, err := flashdbNew(fdbOpts)
	if err != nil {
		return nil, err
	}

	return &FlashDBCache{
		store:      store,
		defaultTTL: secondsToDuration(opts.DefaultTTL),
	}, nil
}

// Close releases resources held by the FlashDBCache.
func (c *FlashDBCache) Close() {
	c.store.Close()
}

// composeKey constructs a namespaced key for flashdb.
// Returns an error if the key is empty.
func composeKey(ns, coll, key string) (string, error) {
	if key == "" {
		return "", errors.New("flashdbcache: empty key provided")
	}

	// Namespace/collection can be empty; we still compose deterministically.
	// Format: ns::coll::key (no escaping for simplicity; upstream should pre-sanitize)
	if ns == "" && coll == "" {
		return key, nil
	}
	if ns == "" {
		return coll + "::" + key, nil
	}
	if coll == "" {
		return ns + "::" + key, nil
	}
	return ns + "::" + coll + "::" + key, nil
}

var composeKeyFn = composeKey

// Exists checks if a key exists in the FlashDBCache.
// Returns true if the key is found, false otherwise.
func (c *FlashDBCache) Exists(ctx context.Context, req *ExistsRequest) (bool, error) {
	if _, err := checkedCacheContext(ctx); err != nil {
		return false, err
	}

	if req == nil {
		return false, errors.New("flashdbcache: nil ExistsRequest")
	}

	key, err := composeKeyFn(req.Namespace, req.Collection, req.Key)
	if err != nil {
		return false, err
	}

	return c.store.Exists(key)
}

// Get retrieves the value associated with a key from the FlashDBCache.
// Returns nil if the key is not found.
func (c *FlashDBCache) Get(ctx context.Context, req *GetRequest) (any, error) {
	if _, err := checkedCacheContext(ctx); err != nil {
		return nil, err
	}

	if req == nil {
		return nil, errors.New("flashdbcache: nil GetRequest")
	}

	k, err := composeKeyFn(req.Namespace, req.Collection, req.Key)
	if err != nil {
		return nil, err
	}

	v, err := c.store.Get(k)
	if err != nil {
		return nil, err
	}

	if len(req.Fields) == 0 {
		return v, nil
	}

	m, ok := v.(map[string]any)
	if !ok {
		return v, nil
	}

	out := make(map[string]any, len(req.Fields))
	for _, f := range req.Fields {
		if vv, ok := m[f]; ok {
			out[f] = vv
		}
	}

	return out, nil
}

// Set stores the value for a key in the FlashDBCache.
// Returns true if the operation succeeded, false otherwise.
func (c *FlashDBCache) Set(ctx context.Context, req *SetRequest) (bool, error) {
	if _, err := checkedCacheContext(ctx); err != nil {
		return false, err
	}

	if req == nil {
		return false, errors.New("flashdbcache: invalid SetRequest")
	}

	k, err := composeKeyFn(req.Namespace, req.Collection, req.Key)
	if err != nil {
		return false, err
	}

	var payload map[string]any

	switch {
	case req.Fields != nil:
		payload = req.Fields

	case req.Value != nil:
		valueMap, ok := req.Value.(map[string]any)
		if ok {
			payload = valueMap
		} else {
			payload = map[string]any{
				"bin": req.Value,
			} // wrap in "bin" key
		}

	default:
		// Allow setting an empty map to create the key (handy for counters appended later).
		payload = map[string]any{}
	}

	ttl := secondsToDuration(req.TTL)
	if ttl == 0 {
		ttl = c.defaultTTL
	}

	return c.store.Set(k, payload, ttl)
}

// Delete removes a key from the FlashDBCache.
// Returns true if the key was successfully deleted, false otherwise.
func (c *FlashDBCache) Delete(ctx context.Context, req *DeleteRequest) (bool, error) {
	if _, err := checkedCacheContext(ctx); err != nil {
		return false, err
	}

	if req == nil {
		return false, errors.New("flashdbcache: invalid DeleteRequest")
	}

	k, err := composeKeyFn(req.Namespace, req.Collection, req.Key)
	if err != nil {
		return false, err
	}

	return c.store.Delete(k)
}

// MultiGet retrieves multiple keys from the FlashDBCache.
// Returns a map of keys to their values; missing keys will be absent.
func (c *FlashDBCache) MultiGet(ctx context.Context, req *MultiGetRequest) (map[string]any, error) {
	if _, err := checkedCacheContext(ctx); err != nil {
		return nil, err
	}

	if req == nil {
		return nil, errors.New("flashdbcache: invalid MultiGetRequest")
	}

	if len(req.Keys) == 0 {
		return map[string]any{}, nil // nothing to do
	}

	fullKeys := make([]string, 0, len(req.Keys))
	keyMap := make(map[string]string, len(req.Keys)) // fullKey -> original key

	for _, k := range req.Keys {
		if k == "" {
			continue // skip empty keys
		}
		fk, err := composeKeyFn(req.Namespace, req.Collection, k)
		if err != nil {
			continue // skip malformed keys
		}
		fullKeys = append(fullKeys, fk)
		keyMap[fk] = k
	}

	if len(fullKeys) == 0 {
		return map[string]any{}, nil
	}

	values, err := c.store.MultiGet(fullKeys)
	if err != nil {
		return nil, err
	}

	// Map back to original keys.
	out := make(map[string]any, len(values))
	for fk, v := range values {
		if origKey, ok := keyMap[fk]; ok {
			out[origKey] = v
		}
	}

	return out, nil
}

// MultiSet sets multiple keys in the FlashDBCache.
// Returns a map indicating success status for each key.
func (c *FlashDBCache) MultiSet(ctx context.Context, req *MultiSetRequest) (map[string]bool, error) {
	if _, err := checkedCacheContext(ctx); err != nil {
		return nil, err
	}

	if req == nil {
		return nil, errors.New("flashdbcache: invalid MultiSetRequest")
	}

	if (len(req.FieldsMap) == 0) && (len(req.ValueMap) == 0) {
		return map[string]bool{}, nil
	}

	ttl := secondsToDuration(req.TTL)
	if ttl == 0 {
		ttl = c.defaultTTL
	}

	result := make(map[string]bool)
	var firstErr error

	recordErr := func(err error) {
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}

	// Process all keys from FieldsMap (these take precedence over ValueMap).
	for key, fields := range req.FieldsMap {
		if key == "" {
			result[key] = false
			continue
		}

		fullKey, err := composeKeyFn(req.Namespace, req.Collection, key)
		if err != nil {
			result[key] = false
			recordErr(err)
			continue
		}

		payload := fields
		if payload == nil {
			payload = map[string]any{}
		}

		ok, err := c.store.Set(fullKey, payload, ttl)
		result[key] = ok
		recordErr(err)
	}

	// Process keys that are only in ValueMap (FieldsMap already handled).
	for key, val := range req.ValueMap {
		if _, already := result[key]; already {
			continue
		}

		if key == "" {
			result[key] = false
			continue
		}

		fullKey, err := composeKeyFn(req.Namespace, req.Collection, key)
		if err != nil {
			result[key] = false
			recordErr(err)
			continue
		}

		var payload map[string]any
		if m, ok := val.(map[string]any); ok {
			payload = m
		} else {
			payload = map[string]any{
				"bin": val,
			}
		}

		ok, err := c.store.Set(fullKey, payload, ttl)
		result[key] = ok
		recordErr(err)
	}

	return result, firstErr
}

// MultiSet sets multiple keys in the FlashDBCache.
// Returns a map indicating success status for each key.
func (c *FlashDBCache) MultiDelete(ctx context.Context, req *MultiDeleteRequest) (map[string]bool, error) {
	if _, err := checkedCacheContext(ctx); err != nil {
		return nil, err
	}

	if req == nil {
		return nil, errors.New("flashdbcache: invalid MultiDeleteRequest")
	}

	if len(req.Keys) == 0 {
		return map[string]bool{}, nil
	}

	fullKeys := make([]string, 0, len(req.Keys))
	keyMap := make(map[string]string, len(req.Keys)) // fullKey -> original key

	for _, key := range req.Keys {
		if key == "" {
			continue // skip empty keys
		}

		fk, err := composeKeyFn(req.Namespace, req.Collection, key)
		if err != nil {
			continue // skip malformed keys
		}

		fullKeys = append(fullKeys, fk)
		keyMap[fk] = key
	}

	if len(fullKeys) == 0 {
		return map[string]bool{}, nil
	}

	deletedMap, err := c.store.MultiDelete(fullKeys)
	if err != nil {
		return nil, err
	}

	// Map back to original (logical) keys.
	out := make(map[string]bool, len(deletedMap))
	for fk, ok := range deletedMap {
		if origKey, exists := keyMap[fk]; exists {
			out[origKey] = ok
		}
	}

	return out, nil
}

// Increment increases numeric fields in the cache.
// If Fields is provided, those specific fields are incremented.
func (c *FlashDBCache) Increment(ctx context.Context, req *IncrementRequest) error {
	if _, err := checkedCacheContext(ctx); err != nil {
		return err
	}

	if req == nil {
		return errors.New("flashdbcache: invalid IncrementRequest")
	}

	k, err := composeKeyFn(req.Namespace, req.Collection, req.Key)
	if err != nil {
		return err
	}

	switch {
	case req.Fields != nil:
		return c.store.Increment(k, req.Fields)

	case req.Value != 0:
		valueMap := map[string]int64{"bin": req.Value}
		return c.store.Increment(k, valueMap)

	default:
		return nil // no-op (nothing to increment)
	}
}

// Decrement decreases numeric fields in the cache.
// If Fields is provided, those specific fields are decremented.
func (c *FlashDBCache) Decrement(ctx context.Context, req *DecrementRequest) error {
	if _, err := checkedCacheContext(ctx); err != nil {
		return err
	}

	if req == nil {
		return errors.New("flashdbcache: invalid DecrementRequest")
	}

	k, err := composeKeyFn(req.Namespace, req.Collection, req.Key)
	if err != nil {
		return err
	}

	switch {
	case req.Fields != nil:
		return c.store.Decrement(k, req.Fields)

	case req.Value != 0:
		valueMap := map[string]int64{"bin": req.Value}
		return c.store.Decrement(k, valueMap)

	default:
		// no-op (nothing to decrement)
		return nil
	}
}

// Increment increases numeric fields in the cache.
// If Fields is provided, those specific fields are incremented.
func (c *FlashDBCache) Append(ctx context.Context, req *AppendRequest) error {
	if _, err := checkedCacheContext(ctx); err != nil {
		return err
	}

	if req == nil {
		return errors.New("flashdbcache: invalid AppendRequest")
	}

	k, err := composeKeyFn(req.Namespace, req.Collection, req.Key)
	if err != nil {
		return err
	}

	switch {
	case req.Fields != nil:
		return c.store.Append(k, req.Fields)

	case req.Value != "":
		valueMap := map[string]string{"bin": req.Value}
		return c.store.Append(k, valueMap)

	default:
		return nil // do nothing, since no point appending an empty string or nil map
	}
}

// GetTTL retrieves the remaining TTL for a key in seconds.
// Returns 0 if the key does not have a TTL or does not exist.
func (c *FlashDBCache) GetTTL(ctx context.Context, req *GetTTLRequest) (int64, error) {
	if _, err := checkedCacheContext(ctx); err != nil {
		return 0, err
	}

	if req == nil {
		return 0, errors.New("flashdbcache: invalid GetTTLRequest")
	}

	k, err := composeKeyFn(req.Namespace, req.Collection, req.Key)
	if err != nil {
		return 0, err
	}

	return c.store.GetTTL(k)
}

// SetTTL sets a new TTL for a key.
// If TTL is 0 or negative, the key will have no expiration.
func (c *FlashDBCache) SetTTL(ctx context.Context, req *SetTTLRequest) error {
	if _, err := checkedCacheContext(ctx); err != nil {
		return err
	}

	if req == nil {
		return errors.New("flashdbcache: invalid SetTTLRequest")
	}

	k, err := composeKeyFn(req.Namespace, req.Collection, req.Key)
	if err != nil {
		return err
	}

	ttl := secondsToDuration(req.TTL)
	if ttl == 0 {
		ttl = c.defaultTTL // fall back to cache default if provided
	}

	return c.store.SetTTL(k, ttl)
}

//////////////////////////////////////////////
// Utility functions
//////////////////////////////////////////////

// secondsToDuration converts seconds (as int64) to time.Duration.
// Zero or negative values become 0 (no TTL).
func secondsToDuration(s int64) time.Duration {
	if s <= 0 {
		return 0
	}

	return time.Duration(s) * time.Second
}
