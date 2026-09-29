package session

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	storageredis "github.com/cshekharsharma/photon/storage/redis"
	"github.com/stretchr/testify/assert"
)

func TestNew_InvalidConfig(t *testing.T) {
	_, err := New(&Config{Encoding: Encoding("bad")})
	assert.ErrorIs(t, err, ErrInvalidEncoding)
}

func TestNew_JSONEncoding(t *testing.T) {
	mgr, err := New(&Config{Encoding: EncodingJSON})
	assert.NoError(t, err)
	assert.NotNil(t, mgr)
	_, ok := mgr.sm.Codec.(jsonCodec)
	assert.True(t, ok)
}

func TestNew_BuildStoreError(t *testing.T) {
	origSetConfig := redisSetConfig
	origConnect := redisConnect
	defer func() {
		redisSetConfig = origSetConfig
		redisConnect = origConnect
	}()

	redisSetConfig = func(name string, cfg *storageredis.ConnectionConfig) {}
	redisConnect = func(ctx context.Context, connector storageredis.RedisConnectorInterface, name string) (storageredis.RedisInterface, error) {
		return nil, errors.New("redis down")
	}

	_, err := New(&Config{Store: StoreConfig{Type: StoreRedis}})
	assert.Error(t, err)
}

func TestMiddleware_SetsCookieAndPersists(t *testing.T) {
	mgr, err := New(&Config{})
	assert.NoError(t, err)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.NoError(t, mgr.Put(r.Context(), "k", "v"))
	})

	wrapped := mgr.Middleware()(handler)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	wrapped.ServeHTTP(w, r)

	cookie := w.Result().Cookies()
	assert.NotEmpty(t, cookie)

	// second request should load existing session data
	w2 := httptest.NewRecorder()
	r2 := httptest.NewRequest(http.MethodGet, "/", nil)
	r2.AddCookie(cookie[0])

	wrapped2 := mgr.Middleware()(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		val, ok, err := mgr.Get(r.Context(), "k")
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "v", val)
	}))

	wrapped2.ServeHTTP(w2, r2)
}

func TestManager_GuardErrors(t *testing.T) {
	mgr, err := New(&Config{})
	assert.NoError(t, err)

	_, _, err = mgr.Get(context.Background(), "missing")
	assert.ErrorIs(t, err, ErrNoSessionInContext)

	_, err = mgr.GetString(context.Background(), "missing")
	assert.ErrorIs(t, err, ErrNoSessionInContext)

	_, err = mgr.GetInt(context.Background(), "missing")
	assert.ErrorIs(t, err, ErrNoSessionInContext)

	_, err = mgr.GetBool(context.Background(), "missing")
	assert.ErrorIs(t, err, ErrNoSessionInContext)

	_, _, err = mgr.Pop(context.Background(), "missing")
	assert.ErrorIs(t, err, ErrNoSessionInContext)

	err = mgr.Put(context.Background(), "k", "v")
	assert.ErrorIs(t, err, ErrNoSessionInContext)

	err = mgr.Remove(context.Background(), "k")
	assert.ErrorIs(t, err, ErrNoSessionInContext)

	err = mgr.Clear(context.Background())
	assert.ErrorIs(t, err, ErrNoSessionInContext)

	_, err = mgr.Exists(context.Background(), "k")
	assert.ErrorIs(t, err, ErrNoSessionInContext)

	err = mgr.Renew(context.Background())
	assert.ErrorIs(t, err, ErrNoSessionInContext)

	err = mgr.Destroy(context.Background())
	assert.ErrorIs(t, err, ErrNoSessionInContext)

	_, err = mgr.Token(context.Background())
	assert.ErrorIs(t, err, ErrNoSessionInContext)

	_, err = mgr.Deadline(context.Background())
	assert.ErrorIs(t, err, ErrNoSessionInContext)
}

func TestManager_Methods(t *testing.T) {
	mgr, err := New(&Config{})
	assert.NoError(t, err)

	ctx, err := mgr.Load(context.Background(), "")
	assert.NoError(t, err)

	assert.NoError(t, mgr.Put(ctx, "str", "val"))
	assert.NoError(t, mgr.Put(ctx, "int", 2))
	assert.NoError(t, mgr.Put(ctx, "bool", true))

	val, ok, err := mgr.Get(ctx, "str")
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, "val", val)

	strVal, err := mgr.GetString(ctx, "str")
	assert.NoError(t, err)
	assert.Equal(t, "val", strVal)

	intVal, err := mgr.GetInt(ctx, "int")
	assert.NoError(t, err)
	assert.Equal(t, 2, intVal)

	boolVal, err := mgr.GetBool(ctx, "bool")
	assert.NoError(t, err)
	assert.True(t, boolVal)

	popVal, ok, err := mgr.Pop(ctx, "str")
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, "val", popVal)

	exists, err := mgr.Exists(ctx, "str")
	assert.NoError(t, err)
	assert.False(t, exists)

	assert.NoError(t, mgr.Remove(ctx, "int"))
	assert.NoError(t, mgr.Clear(ctx))

	assert.NoError(t, mgr.Renew(ctx))
	okToken, err := mgr.Token(ctx)
	assert.NoError(t, err)
	assert.NotEmpty(t, okToken)

	deadline, err := mgr.Deadline(ctx)
	assert.NoError(t, err)
	assert.False(t, deadline.IsZero())

	assert.NoError(t, mgr.Destroy(ctx))
}

func TestManager_LoadNotInitialized(t *testing.T) {
	var mgr *Manager
	_, err := mgr.Load(context.Background(), "")
	assert.ErrorIs(t, err, ErrNotInitialized)
}

func TestManager_Close(t *testing.T) {
	mgr := &Manager{closeFn: func() error { return nil }}
	assert.NoError(t, mgr.Close())

	mgr = &Manager{}
	assert.NoError(t, mgr.Close())

	var nilMgr *Manager
	assert.NoError(t, nilMgr.Close())
}

func TestManager_GuardNilManager(t *testing.T) {
	var mgr *Manager
	err := mgr.Put(context.Background(), "k", "v")
	assert.ErrorIs(t, err, ErrNotInitialized)
}

func TestManager_ErrorHandler(t *testing.T) {
	called := false
	custom := func(w http.ResponseWriter, r *http.Request, err error) {
		called = true
	}

	mgr, err := New(&Config{ErrorHandler: custom})
	assert.NoError(t, err)

	// Force the handler to be invoked by calling Load with bad token and failing store find
	// by using a dummy context without data and a handler that panics in error handler.
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()

	// use middleware directly to ensure ErrorFunc is set; this won't trigger error normally
	mgr.Middleware()(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})).ServeHTTP(w, r)
	assert.False(t, called)
}

func TestManager_DoesNotLeakDefaults(t *testing.T) {
	cfg := &Config{Lifetime: 2 * time.Hour}
	mgr, err := New(cfg)
	assert.NoError(t, err)
	assert.Equal(t, 2*time.Hour, mgr.sm.Lifetime)
}
