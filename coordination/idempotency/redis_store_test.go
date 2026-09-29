package idempotency

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	photonredis "github.com/cshekharsharma/photon/storage/redis"
	redisv9 "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestRedisStoreReserveDecisions(t *testing.T) {
	t.Parallel()

	responseRecord := mustJSON(t, redisRecord{
		State:       redisStateCompleted,
		Fingerprint: "fp",
		Response: &StoredResponse{
			StatusCode: 201,
			Header:     map[string][]string{"Content-Type": {"application/json"}},
			Body:       []byte(`{"ok":true}`),
			Replayable: true,
		},
	})

	tests := []struct {
		name     string
		result   any
		status   Status
		response *StoredResponse
	}{
		{name: "reserved", result: []interface{}{string(StatusReserved)}, status: StatusReserved},
		{name: "in flight", result: []interface{}{string(StatusInFlight)}, status: StatusInFlight},
		{name: "mismatch", result: []interface{}{string(StatusMismatch)}, status: StatusMismatch},
		{
			name:     "completed",
			result:   []interface{}{string(StatusCompleted), responseRecord},
			status:   StatusCompleted,
			response: &StoredResponse{StatusCode: 201, Header: map[string][]string{"Content-Type": {"application/json"}}, Body: []byte(`{"ok":true}`), Replayable: true},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			store := newTestRedisStore(t)
			script := &testRedisScript{result: tt.result}
			store.reserveScript = script

			decision, err := store.Reserve(context.Background(), validRequest())

			require.NoError(t, err)
			require.Equal(t, tt.status, decision.Status)
			require.Equal(t, tt.response, decision.Response)
			require.Len(t, script.calls, 1)
			require.True(t, strings.HasPrefix(script.calls[0].keys[0], "idem:v1:{"))
			require.Len(t, script.calls[0].args, 3)
		})
	}
}

func TestRedisStoreKeyVersion(t *testing.T) {
	t.Parallel()

	store, err := NewRedisStore(RedisStoreOptions{
		Client:     &testRedis{raw: redisv9.NewClient(&redisv9.Options{Addr: "127.0.0.1:0"})},
		KeyPrefix:  "custom",
		KeyVersion: "v9",
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = store.client.Close()
	})

	require.True(t, strings.HasPrefix(store.redisKey("scope", "key"), "custom:v9:{"))
}

func TestStoreStrategy(t *testing.T) {
	t.Parallel()

	store, err := NewStore(RedisStrategy{Options: RedisStoreOptions{
		Client: &testRedis{raw: redisv9.NewClient(&redisv9.Options{Addr: "127.0.0.1:0"})},
	}})
	require.NoError(t, err)
	require.NotNil(t, store)
	require.NoError(t, store.(*RedisStore).client.Close())

	store, err = NewStore(nil)
	require.ErrorIs(t, err, ErrInvalidRequest)
	require.Nil(t, store)

	store, err = NewStore(nilStoreStrategy{})
	require.ErrorIs(t, err, ErrInvalidRequest)
	require.Nil(t, store)

	store, err = NewStore(errorStoreStrategy{})
	require.ErrorIs(t, err, ErrStoreUnavailable)
	require.Nil(t, store)
}

func TestRedisStoreCompleteAndRelease(t *testing.T) {
	t.Parallel()

	store := newTestRedisStore(t)
	complete := &testRedisScript{result: []interface{}{int64(1), "ok"}}
	release := &testRedisScript{result: int64(1)}
	store.completeScript = complete
	store.releaseScript = release

	err := store.Complete(context.Background(), validCompleteRequest())
	require.NoError(t, err)
	require.Len(t, complete.calls, 1)
	require.Len(t, complete.calls[0].args, 4)

	err = store.Release(context.Background(), validReleaseRequest())
	require.NoError(t, err)
	require.Len(t, release.calls, 1)
	require.Len(t, release.calls[0].args, 2)
}

func TestRedisStoreCompletionFencingErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		result any
		want   error
	}{
		{name: "wrong owner token", result: []interface{}{int64(0), "mismatch"}, want: ErrMismatch},
		{name: "expired or replaced", result: []interface{}{int64(0), "missing"}, want: ErrNotReserved},
		{name: "completed by another state", result: []interface{}{int64(0), "state"}, want: ErrNotReserved},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			store := newTestRedisStore(t)
			store.completeScript = &testRedisScript{result: tt.result}

			err := store.Complete(context.Background(), validCompleteRequest())

			require.ErrorIs(t, err, tt.want)
		})
	}
}

func TestRedisStoreValidationAndStoreErrors(t *testing.T) {
	t.Parallel()

	_, err := NewRedisStore(RedisStoreOptions{})
	require.ErrorIs(t, err, ErrInvalidRequest)

	store := newTestRedisStore(t)
	_, err = (*RedisStore)(nil).Reserve(context.Background(), validRequest())
	require.ErrorIs(t, err, ErrInvalidRequest)
	err = (*RedisStore)(nil).Complete(context.Background(), validCompleteRequest())
	require.ErrorIs(t, err, ErrInvalidRequest)
	err = (*RedisStore)(nil).Release(context.Background(), validReleaseRequest())
	require.ErrorIs(t, err, ErrInvalidRequest)

	var nilCtx context.Context
	_, err = store.Reserve(nilCtx, validRequest())
	require.ErrorIs(t, err, ErrNilContext)
	err = store.Complete(nilCtx, validCompleteRequest())
	require.ErrorIs(t, err, ErrNilContext)
	err = store.Release(nilCtx, validReleaseRequest())
	require.ErrorIs(t, err, ErrNilContext)

	bad := validRequest()
	bad.Key = " "
	_, err = store.Reserve(context.Background(), bad)
	require.ErrorIs(t, err, ErrInvalidRequest)

	bad = validRequest()
	bad.Scope = ""
	_, err = store.Reserve(context.Background(), bad)
	require.ErrorIs(t, err, ErrInvalidRequest)

	bad = validRequest()
	bad.Fingerprint = ""
	_, err = store.Reserve(context.Background(), bad)
	require.ErrorIs(t, err, ErrInvalidRequest)

	bad = validRequest()
	bad.OwnerToken = ""
	_, err = store.Reserve(context.Background(), bad)
	require.ErrorIs(t, err, ErrInvalidRequest)

	bad = validRequest()
	bad.TTL = 0
	_, err = store.Reserve(context.Background(), bad)
	require.ErrorIs(t, err, ErrInvalidRequest)

	badComplete := validCompleteRequest()
	badComplete.TTL = 0
	err = store.Complete(context.Background(), badComplete)
	require.ErrorIs(t, err, ErrInvalidRequest)

	badComplete = validCompleteRequest()
	badComplete.OwnerToken = ""
	err = store.Complete(context.Background(), badComplete)
	require.ErrorIs(t, err, ErrInvalidRequest)

	badRelease := validReleaseRequest()
	badRelease.Fingerprint = ""
	err = store.Release(context.Background(), badRelease)
	require.ErrorIs(t, err, ErrInvalidRequest)

	store.reserveScript = &testRedisScript{err: errors.New("down")}
	_, err = store.Reserve(context.Background(), validRequest())
	require.ErrorIs(t, err, ErrStoreUnavailable)

	store.completeScript = &testRedisScript{err: errors.New("down")}
	err = store.Complete(context.Background(), validCompleteRequest())
	require.ErrorIs(t, err, ErrStoreUnavailable)

	store.releaseScript = &testRedisScript{err: errors.New("down")}
	err = store.Release(context.Background(), validReleaseRequest())
	require.ErrorIs(t, err, ErrStoreUnavailable)
}

func TestRedisStoreParseFailures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		script redisScript
		call   func(*RedisStore) error
		want   error
	}{
		{
			name:   "reserve empty result",
			script: &testRedisScript{result: []interface{}{}},
			call: func(store *RedisStore) error {
				_, err := store.Reserve(context.Background(), validRequest())
				return err
			},
			want: ErrStoreUnavailable,
		},
		{
			name:   "reserve invalid status type",
			script: &testRedisScript{result: []interface{}{int64(1)}},
			call: func(store *RedisStore) error {
				_, err := store.Reserve(context.Background(), validRequest())
				return err
			},
			want: ErrStoreUnavailable,
		},
		{
			name:   "reserve unexpected type",
			script: &testRedisScript{result: "bad"},
			call: func(store *RedisStore) error {
				_, err := store.Reserve(context.Background(), validRequest())
				return err
			},
			want: ErrStoreUnavailable,
		},
		{
			name:   "reserve unknown status",
			script: &testRedisScript{result: []interface{}{"unknown"}},
			call: func(store *RedisStore) error {
				_, err := store.Reserve(context.Background(), validRequest())
				return err
			},
			want: ErrStoreUnavailable,
		},
		{
			name:   "reserve completed missing payload",
			script: &testRedisScript{result: []interface{}{string(StatusCompleted)}},
			call: func(store *RedisStore) error {
				_, err := store.Reserve(context.Background(), validRequest())
				return err
			},
			want: ErrStoreUnavailable,
		},
		{
			name:   "reserve invalid completed json",
			script: &testRedisScript{result: []interface{}{string(StatusCompleted), "{"}},
			call: func(store *RedisStore) error {
				_, err := store.Reserve(context.Background(), validRequest())
				return err
			},
			want: ErrStoreUnavailable,
		},
		{
			name:   "reserve invalid completed response type",
			script: &testRedisScript{result: []interface{}{string(StatusCompleted), int64(1)}},
			call: func(store *RedisStore) error {
				_, err := store.Reserve(context.Background(), validRequest())
				return err
			},
			want: ErrStoreUnavailable,
		},
		{
			name: "reserve completed record without response",
			script: &testRedisScript{result: []interface{}{
				string(StatusCompleted),
				mustJSON(t, redisRecord{State: redisStateCompleted, Fingerprint: "fp"}),
			}},
			call: func(store *RedisStore) error {
				_, err := store.Reserve(context.Background(), validRequest())
				return err
			},
			want: ErrStoreUnavailable,
		},
		{
			name:   "complete invalid result",
			script: &testRedisScript{result: []interface{}{"bad", "ok"}},
			call: func(store *RedisStore) error {
				return store.Complete(context.Background(), validCompleteRequest())
			},
			want: ErrStoreUnavailable,
		},
		{
			name:   "complete short result",
			script: &testRedisScript{result: []interface{}{int64(0)}},
			call: func(store *RedisStore) error {
				return store.Complete(context.Background(), validCompleteRequest())
			},
			want: ErrStoreUnavailable,
		},
		{
			name:   "complete non-string failure reason",
			script: &testRedisScript{result: []interface{}{int64(0), int64(1)}},
			call: func(store *RedisStore) error {
				return store.Complete(context.Background(), validCompleteRequest())
			},
			want: ErrNotReserved,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			store := newTestRedisStore(t)
			store.reserveScript = tt.script
			store.completeScript = tt.script

			err := tt.call(store)

			require.ErrorIs(t, err, tt.want)
		})
	}
}

func TestRedisStoreByteStringAndOwnerToken(t *testing.T) {
	t.Parallel()

	responseRecord := mustJSON(t, redisRecord{
		State:       redisStateCompleted,
		Fingerprint: "fp",
		Response:    &StoredResponse{StatusCode: 200, Replayable: true},
	})
	store := newTestRedisStore(t)
	store.reserveScript = &testRedisScript{result: []interface{}{[]byte(StatusCompleted), []byte(responseRecord)}}

	decision, err := store.Reserve(context.Background(), validRequest())

	require.NoError(t, err)
	require.Equal(t, StatusCompleted, decision.Status)
	require.True(t, decision.Response.Replayable)
	require.NotEmpty(t, NewOwnerToken())
}

func TestRedisStoreConcurrentReserveHasOneWinner(t *testing.T) {
	t.Parallel()

	store := newTestRedisStore(t)
	var winners atomic.Int64
	store.reserveScript = &testRedisScript{
		run: func() any {
			if winners.Add(1) == 1 {
				return []interface{}{string(StatusReserved)}
			}
			return []interface{}{string(StatusInFlight)}
		},
	}

	var wg sync.WaitGroup
	results := make(chan Status, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			decision, err := store.Reserve(context.Background(), validRequest())
			require.NoError(t, err)
			results <- decision.Status
		}()
	}
	wg.Wait()
	close(results)

	var reserved int
	for status := range results {
		if status == StatusReserved {
			reserved++
		}
	}
	require.Equal(t, 1, reserved)
}

func TestRedisStoreIntegrationScripts(t *testing.T) {
	addr := os.Getenv("PHOTON_REDIS_INTEGRATION_ADDR")
	if addr == "" {
		t.Skip("set PHOTON_REDIS_INTEGRATION_ADDR to run Redis Lua integration tests")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	raw := redisv9.NewClient(&redisv9.Options{Addr: addr})
	t.Cleanup(func() {
		_ = raw.Close()
	})
	require.NoError(t, raw.Ping(ctx).Err())
	require.NoError(t, raw.FlushDB(ctx).Err())

	store, err := NewRedisStore(RedisStoreOptions{
		Client:     &testRedis{raw: raw},
		KeyPrefix:  "idemtest",
		KeyVersion: "v1",
	})
	require.NoError(t, err)

	req := validRequest()
	req.Key = "integration-key"
	req.Scope = "POST /integration"

	decision, err := store.Reserve(ctx, req)
	require.NoError(t, err)
	require.Equal(t, StatusReserved, decision.Status)

	decision, err = store.Reserve(ctx, req)
	require.NoError(t, err)
	require.Equal(t, StatusInFlight, decision.Status)

	mismatch := req
	mismatch.Fingerprint = "different"
	decision, err = store.Reserve(ctx, mismatch)
	require.NoError(t, err)
	require.Equal(t, StatusMismatch, decision.Status)

	err = store.Complete(ctx, CompleteRequest{
		Key:         req.Key,
		Scope:       req.Scope,
		Fingerprint: req.Fingerprint,
		TTL:         req.TTL,
		OwnerToken:  req.OwnerToken,
		Response:    StoredResponse{StatusCode: 201, Body: []byte("created"), Replayable: true},
	})
	require.NoError(t, err)

	decision, err = store.Reserve(ctx, req)
	require.NoError(t, err)
	require.Equal(t, StatusCompleted, decision.Status)
	require.Equal(t, 201, decision.Response.StatusCode)
	require.Equal(t, []byte("created"), decision.Response.Body)
}

func validRequest() Request {
	return Request{
		Key:         "key",
		Scope:       "POST /orders",
		Fingerprint: "fp",
		TTL:         time.Minute,
		OwnerToken:  "owner",
	}
}

func validCompleteRequest() CompleteRequest {
	req := validRequest()
	return CompleteRequest{
		Key:         req.Key,
		Scope:       req.Scope,
		Fingerprint: req.Fingerprint,
		TTL:         req.TTL,
		OwnerToken:  req.OwnerToken,
		Response: StoredResponse{
			StatusCode: 200,
			Body:       []byte("ok"),
			Replayable: true,
		},
	}
}

func validReleaseRequest() ReleaseRequest {
	req := validRequest()
	return ReleaseRequest{
		Key:         req.Key,
		Scope:       req.Scope,
		Fingerprint: req.Fingerprint,
		OwnerToken:  req.OwnerToken,
	}
}

func newTestRedisStore(t *testing.T) *RedisStore {
	t.Helper()
	store, err := NewRedisStore(RedisStoreOptions{Client: &testRedis{raw: redisv9.NewClient(&redisv9.Options{Addr: "127.0.0.1:0"})}})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = store.client.Close()
	})
	return store
}

func mustJSON(t *testing.T, value any) string {
	t.Helper()
	data, err := json.Marshal(value)
	require.NoError(t, err)
	return string(data)
}

type testScriptCall struct {
	keys []string
	args []interface{}
}

type testRedisScript struct {
	mu     sync.Mutex
	result any
	err    error
	run    func() any
	calls  []testScriptCall
}

func (s *testRedisScript) Run(_ context.Context, _ redisv9.Scripter, keys []string, args ...interface{}) *redisv9.Cmd {
	s.mu.Lock()
	s.calls = append(s.calls, testScriptCall{keys: append([]string(nil), keys...), args: append([]interface{}(nil), args...)})
	result := s.result
	if s.run != nil {
		result = s.run()
	}
	err := s.err
	s.mu.Unlock()
	return redisv9.NewCmdResult(result, err)
}

type nilStoreStrategy struct{}

func (nilStoreStrategy) NewStore() (Store, error) {
	return nil, nil
}

type errorStoreStrategy struct{}

func (errorStoreStrategy) NewStore() (Store, error) {
	return nil, ErrStoreUnavailable
}

type testRedis struct {
	client photonredis.RedisClientInterface
	raw    *redisv9.Client
}

func (r *testRedis) GetClient() photonredis.RedisClientInterface {
	return r.client
}

func (r *testRedis) GetRawClient() *redisv9.Client {
	return r.raw
}

func (r *testRedis) SetClient(client photonredis.RedisClientInterface) {
	r.client = client
}

func (r *testRedis) Close() error {
	return r.raw.Close()
}
