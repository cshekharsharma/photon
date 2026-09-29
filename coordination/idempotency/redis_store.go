package idempotency

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	photonredis "github.com/cshekharsharma/photon/storage/redis"
	redisv9 "github.com/redis/go-redis/v9"
)

const (
	defaultRedisKeyPrefix  = "idem"
	defaultRedisKeyVersion = "v1"
	redisStateProcessing   = "processing"
	redisStateCompleted    = "completed"
)

type redisScript interface {
	Run(ctx context.Context, client redisv9.Scripter, keys []string, args ...interface{}) *redisv9.Cmd
}

// RedisStoreOptions configures a Redis-backed idempotency store.
type RedisStoreOptions struct {
	Client     photonredis.RedisInterface
	KeyPrefix  string
	KeyVersion string
}

// RedisStore implements Store using Redis Lua scripts for atomic state changes.
type RedisStore struct {
	client         photonredis.RedisInterface
	keyPrefix      string
	keyVersion     string
	reserveScript  redisScript
	completeScript redisScript
	releaseScript  redisScript
}

type redisRecord struct {
	State       string          `json:"s"`
	Fingerprint string          `json:"f"`
	OwnerToken  string          `json:"o,omitempty"`
	Response    *StoredResponse `json:"r,omitempty"`
}

// NewRedisStore creates a Redis-backed Store.
func NewRedisStore(opts RedisStoreOptions) (*RedisStore, error) {
	if opts.Client == nil || opts.Client.GetRawClient() == nil {
		return nil, fmt.Errorf("%w: redis client is required", ErrInvalidRequest)
	}
	prefix := strings.TrimSpace(opts.KeyPrefix)
	if prefix == "" {
		prefix = defaultRedisKeyPrefix
	}
	version := strings.TrimSpace(opts.KeyVersion)
	if version == "" {
		version = defaultRedisKeyVersion
	}

	return &RedisStore{
		client:         opts.Client,
		keyPrefix:      prefix,
		keyVersion:     version,
		reserveScript:  redisv9.NewScript(redisReserveScript),
		completeScript: redisv9.NewScript(redisCompleteScript),
		releaseScript:  redisv9.NewScript(redisReleaseScript),
	}, nil
}

// Reserve atomically claims a scoped key or returns its current idempotency state.
func (s *RedisStore) Reserve(ctx context.Context, req Request) (Decision, error) {
	if s == nil {
		return Decision{}, fmt.Errorf("%w: redis store cannot be nil", ErrInvalidRequest)
	}
	if err := validateReserve(ctx, req); err != nil {
		return Decision{}, err
	}

	record, _ := json.Marshal(redisRecord{
		State:       redisStateProcessing,
		Fingerprint: req.Fingerprint,
		OwnerToken:  req.OwnerToken,
	})

	result, err := s.reserveScript.Run(
		ctx,
		s.client.GetRawClient(),
		[]string{s.redisKey(req.Scope, req.Key)},
		string(record),
		req.Fingerprint,
		req.TTL.Milliseconds(),
	).Result()
	if err != nil {
		return Decision{}, fmt.Errorf("%w: reserve script: %v", ErrStoreUnavailable, err)
	}
	return parseReserveResult(result)
}

// Complete atomically stores a completed response for the active owner token.
func (s *RedisStore) Complete(ctx context.Context, req CompleteRequest) error {
	if s == nil {
		return fmt.Errorf("%w: redis store cannot be nil", ErrInvalidRequest)
	}
	if err := validateComplete(ctx, req); err != nil {
		return err
	}

	record, _ := json.Marshal(redisRecord{
		State:       redisStateCompleted,
		Fingerprint: req.Fingerprint,
		Response:    &req.Response,
	})

	result, err := s.completeScript.Run(
		ctx,
		s.client.GetRawClient(),
		[]string{s.redisKey(req.Scope, req.Key)},
		string(record),
		req.Fingerprint,
		req.OwnerToken,
		req.TTL.Milliseconds(),
	).Result()
	if err != nil {
		return fmt.Errorf("%w: complete script: %v", ErrStoreUnavailable, err)
	}
	return parseCompleteResult(result)
}

// Release removes only a matching processing record.
func (s *RedisStore) Release(ctx context.Context, req ReleaseRequest) error {
	if s == nil {
		return fmt.Errorf("%w: redis store cannot be nil", ErrInvalidRequest)
	}
	if err := validateRelease(ctx, req); err != nil {
		return err
	}

	if _, err := s.releaseScript.Run(
		ctx,
		s.client.GetRawClient(),
		[]string{s.redisKey(req.Scope, req.Key)},
		req.Fingerprint,
		req.OwnerToken,
	).Result(); err != nil {
		return fmt.Errorf("%w: release script: %v", ErrStoreUnavailable, err)
	}
	return nil
}

func (s *RedisStore) redisKey(scope, key string) string {
	return fmt.Sprintf("%s:%s:{%s}:%s", s.keyPrefix, s.keyVersion, hashPart(scope), hashPart(key))
}

func hashPart(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func parseReserveResult(value any) (Decision, error) {
	values, err := redisArray(value, 1)
	if err != nil {
		return Decision{}, err
	}
	status, err := redisString(values[0])
	if err != nil {
		return Decision{}, fmt.Errorf("%w: invalid reserve status: %v", ErrStoreUnavailable, err)
	}

	switch Status(status) {
	case StatusReserved:
		return Decision{Status: StatusReserved}, nil
	case StatusInFlight:
		return Decision{Status: StatusInFlight}, nil
	case StatusMismatch:
		return Decision{Status: StatusMismatch}, nil
	case StatusCompleted:
		if len(values) != 2 {
			return Decision{}, fmt.Errorf("%w: completed result missing response", ErrStoreUnavailable)
		}
		raw, err := redisString(values[1])
		if err != nil {
			return Decision{}, fmt.Errorf("%w: invalid completed response: %v", ErrStoreUnavailable, err)
		}
		response, err := decodeStoredResponse(raw)
		if err != nil {
			return Decision{}, err
		}
		return Decision{Status: StatusCompleted, Response: response}, nil
	default:
		return Decision{}, fmt.Errorf("%w: unknown reserve status %q", ErrStoreUnavailable, status)
	}
}

func parseCompleteResult(value any) error {
	values, err := redisArray(value, 2)
	if err != nil {
		return err
	}
	ok, err := redisInt64(values[0])
	if err != nil {
		return fmt.Errorf("%w: invalid complete result: %v", ErrStoreUnavailable, err)
	}
	if ok == 1 {
		return nil
	}
	reason, _ := redisString(values[1])
	if reason == "mismatch" {
		return ErrMismatch
	}
	return ErrNotReserved
}

func decodeStoredResponse(raw string) (*StoredResponse, error) {
	var record redisRecord
	if err := json.Unmarshal([]byte(raw), &record); err != nil {
		return nil, fmt.Errorf("%w: decode completed record: %v", ErrStoreUnavailable, err)
	}
	if record.State != redisStateCompleted || record.Response == nil {
		return nil, fmt.Errorf("%w: completed record has no response", ErrStoreUnavailable)
	}
	return record.Response, nil
}

func redisArray(value any, minLen int) ([]interface{}, error) {
	values, ok := value.([]interface{})
	if !ok {
		return nil, fmt.Errorf("%w: unexpected script return type %T", ErrStoreUnavailable, value)
	}
	if len(values) < minLen {
		return nil, fmt.Errorf("%w: unexpected script return length %d", ErrStoreUnavailable, len(values))
	}
	return values, nil
}

func redisString(value any) (string, error) {
	switch typed := value.(type) {
	case string:
		return typed, nil
	case []byte:
		return string(typed), nil
	default:
		return "", fmt.Errorf("unexpected type %T", value)
	}
}

func redisInt64(value any) (int64, error) {
	n, ok := value.(int64)
	if !ok {
		return 0, fmt.Errorf("unexpected type %T", value)
	}
	return n, nil
}
