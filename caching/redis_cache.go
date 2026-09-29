package caching

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/cshekharsharma/photon/storage/redis"
	redisv9 "github.com/redis/go-redis/v9"
)

// RedisCache provides a caching layer backed by Redis.
type RedisCache struct {
	client      redis.RedisInterface
	clusterName string
	namespace   string
	collection  string
}

var redisSerializeValue = func(r *RedisCache, val any) ([]byte, error) {
	return r.getSerialisedValue(val)
}

type redisSetCommand struct {
	key        string
	value      []byte
	expiration time.Duration
}

var redisPipelineSet = func(ctx context.Context, client redis.RedisClientInterface, commands []redisSetCommand) ([]*redisv9.StatusCmd, error) {
	pipelined, ok := client.(interface {
		Pipeline() redisv9.Pipeliner
	})
	if !ok {
		return nil, errors.New("redis client does not support pipelining")
	}

	pipe := pipelined.Pipeline()
	results := make([]*redisv9.StatusCmd, 0, len(commands))
	for _, command := range commands {
		results = append(results, pipe.Set(ctx, command.key, command.value, command.expiration))
	}

	_, err := pipe.Exec(ctx)
	return results, err
}

// NewRedisCache initializes a new RedisCache instance using the provided options.
func NewRedisCache(opts *Options, connector redis.RedisConnectorInterface) (*RedisCache, error) {
	redis.SetConnectionConfig(opts.Cluster, &redis.ConnectionConfig{
		Address:  opts.Hosts[0], // Redis typically uses a single address
		Username: opts.Username,
		Password: opts.Password,
	})

	client, err := redis.Connect(context.Background(), connector, opts.Cluster)
	if err != nil {
		return nil, err
	}

	return &RedisCache{
		client:      client,
		clusterName: opts.Cluster,
		namespace:   opts.Namespace,
		collection:  opts.Collection,
	}, nil
}

// Exists checks if the key exists in Redis.
func (r *RedisCache) Exists(ctx context.Context, request *ExistsRequest) (bool, error) {
	ctx, err := checkedCacheContext(ctx)
	if err != nil {
		return false, err
	}
	key := r.formatKey(request.Namespace, request.Collection, request.Key)

	count, err := r.client.GetClient().Exists(ctx, key).Result()
	return count == 1, err
}

// Get retrieves a value from Redis.
func (r *RedisCache) Get(ctx context.Context, request *GetRequest) (any, error) {
	ctx, err := checkedCacheContext(ctx)
	if err != nil {
		return nil, err
	}
	key := r.formatKey(request.Namespace, request.Collection, request.Key)

	result, err := r.client.GetClient().Get(ctx, key).Bytes()
	if err == redisv9.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return result, nil
}

// Set stores a key-value pair in Redis with optional TTL.
func (r *RedisCache) Set(ctx context.Context, request *SetRequest) (bool, error) {
	ctx, err := checkedCacheContext(ctx)
	if err != nil {
		return false, err
	}
	key := r.formatKey(request.Namespace, request.Collection, request.Key)

	valueBytes, err := redisSerializeValue(r, request.Value)
	if err != nil {
		return false, err
	}

	status := r.client.GetClient().Set(ctx, key, valueBytes, time.Duration(request.TTL)*time.Second)
	return status.Err() == nil, status.Err()
}

// Delete removes a key from Redis.
func (r *RedisCache) Delete(ctx context.Context, request *DeleteRequest) (bool, error) {
	ctx, err := checkedCacheContext(ctx)
	if err != nil {
		return false, err
	}
	key := r.formatKey(request.Namespace, request.Collection, request.Key)

	count, err := r.client.GetClient().Del(ctx, key).Result()
	return count > 0, err
}

// MultiGet retrieves multiple keys in batch.
func (r *RedisCache) MultiGet(ctx context.Context, request *MultiGetRequest) (map[string]any, error) {
	ctx, err := checkedCacheContext(ctx)
	if err != nil {
		return nil, err
	}
	formattedKeys := make([]string, len(request.Keys))

	for i, k := range request.Keys {
		formattedKeys[i] = r.formatKey(request.Namespace, request.Collection, k)
	}

	values, err := r.client.GetClient().MGet(ctx, formattedKeys...).Result()
	if err != nil {
		return nil, err
	}

	result := make(map[string]any)
	for i, val := range values {
		if val != nil {
			originalKey := request.Keys[i]
			result[originalKey] = val
		}
	}

	return result, nil
}

// MultiSet stores multiple key-value pairs in one batch.
func (r *RedisCache) MultiSet(ctx context.Context, request *MultiSetRequest) (map[string]bool, error) {
	ctx, err := checkedCacheContext(ctx)
	if err != nil {
		return nil, err
	}
	result := make(map[string]bool)
	logicalKeys := make([]string, 0, len(request.ValueMap))
	commands := make([]redisSetCommand, 0, len(request.ValueMap))
	var firstErr error

	for k, v := range request.ValueMap {
		valueBytes, err := redisSerializeValue(r, v)
		if err != nil {
			result[k] = false
			if firstErr == nil {
				firstErr = err
			}
			continue
		}

		logicalKeys = append(logicalKeys, k)
		commands = append(commands, redisSetCommand{
			key:        r.formatKey(request.Namespace, request.Collection, k),
			value:      valueBytes,
			expiration: time.Duration(request.TTL) * time.Second,
		})
	}

	if len(commands) == 0 {
		return result, firstErr
	}

	statuses, err := redisPipelineSet(ctx, r.client.GetClient(), commands)
	if err != nil && firstErr == nil {
		firstErr = err
	}

	for i, logicalKey := range logicalKeys {
		if i >= len(statuses) || statuses[i] == nil {
			result[logicalKey] = false
			continue
		}
		err := statuses[i].Err()
		result[logicalKey] = err == nil
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return result, firstErr
}

// MultiDelete deletes multiple keys.
func (r *RedisCache) MultiDelete(ctx context.Context, request *MultiDeleteRequest) (map[string]bool, error) {
	ctx, err := checkedCacheContext(ctx)
	if err != nil {
		return nil, err
	}
	result := make(map[string]bool)
	var firstErr error

	for _, k := range request.Keys {
		key := r.formatKey(request.Namespace, request.Collection, k)
		deleted, err := r.client.GetClient().Del(ctx, key).Result()
		result[k] = err == nil && deleted > 0
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return result, firstErr
}

// Increment increases the numeric value for a key.
func (r *RedisCache) Increment(ctx context.Context, request *IncrementRequest) error {
	ctx, err := checkedCacheContext(ctx)
	if err != nil {
		return err
	}
	key := r.formatKey(request.Namespace, request.Collection, request.Key)

	_, err = r.client.GetClient().IncrBy(ctx, key, int64(request.Value)).Result()
	return err
}

// Decrement decreases the numeric value for a key.
func (r *RedisCache) Decrement(ctx context.Context, request *DecrementRequest) error {
	ctx, err := checkedCacheContext(ctx)
	if err != nil {
		return err
	}
	key := r.formatKey(request.Namespace, request.Collection, request.Key)

	_, err = r.client.GetClient().DecrBy(ctx, key, int64(request.Value)).Result()
	return err
}

// Append appends data to a string key.
func (r *RedisCache) Append(ctx context.Context, request *AppendRequest) error {
	ctx, err := checkedCacheContext(ctx)
	if err != nil {
		return err
	}
	key := r.formatKey(request.Namespace, request.Collection, request.Key)

	valueBytes, err := redisSerializeValue(r, request.Value)
	if err != nil {
		return err
	}

	_, err = r.client.GetClient().Append(ctx, key, string(valueBytes)).Result()
	return err
}

// GetTTL returns the remaining TTL for a key.
func (r *RedisCache) GetTTL(ctx context.Context, request *GetTTLRequest) (int64, error) {
	ctx, err := checkedCacheContext(ctx)
	if err != nil {
		return 0, err
	}
	key := r.formatKey(request.Namespace, request.Collection, request.Key)

	duration, err := r.client.GetClient().TTL(ctx, key).Result()
	if err != nil {
		return 0, err
	}
	return int64(duration.Seconds()), nil
}

// SetTTL updates the TTL for a key.
func (r *RedisCache) SetTTL(ctx context.Context, request *SetTTLRequest) error {
	ctx, err := checkedCacheContext(ctx)
	if err != nil {
		return err
	}
	key := r.formatKey(request.Namespace, request.Collection, request.Key)

	_, err = r.client.GetClient().Expire(ctx, key, time.Duration(request.TTL)*time.Second).Result()
	return err
}

// formatKey creates a full Redis key using namespace, collection, and key.
func (r *RedisCache) formatKey(namespace, collection, key string) string {
	return fmt.Sprintf("%s:%s:%s", namespace, collection, key)
}

// getSerialisedValue serializes the value into []byte.
func (r *RedisCache) getSerialisedValue(val any) ([]byte, error) {
	switch v := val.(type) {
	case []byte:
		return v, nil
	case string:
		return []byte(v), nil
	default:
		b, err := json.Marshal(val)
		if err != nil {
			return nil, errors.New("value should be one of {string, byteArray, json serialisable} datatype")
		}
		return b, nil
	}
}
