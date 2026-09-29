package middleware

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cshekharsharma/photon/coordination/idempotency"
	"github.com/stretchr/testify/require"
)

func TestIdempotencyStoresAndReplaysSuccessfulResponse(t *testing.T) {
	t.Parallel()

	store := &fakeIdempotencyStore{}
	var handlerCalls atomic.Int64
	handler := Idempotency(IdempotencyOptions{Store: store})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handlerCalls.Add(1)
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.Equal(t, `{"order":1}`, string(body))
		w.Header().Set("X-Result", "created")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"id":10}`))
	}))

	first := httptest.NewRequest(http.MethodPost, "/orders", strings.NewReader(`{"order":1}`))
	first.Header.Set(defaultIdempotencyHeader, "key-1")
	firstResponse := httptest.NewRecorder()
	handler.ServeHTTP(firstResponse, first)

	require.Equal(t, http.StatusCreated, firstResponse.Code)
	require.Equal(t, `{"id":10}`, firstResponse.Body.String())

	second := httptest.NewRequest(http.MethodPost, "/orders", strings.NewReader(`{"order":1}`))
	second.Header.Set(defaultIdempotencyHeader, "key-1")
	secondResponse := httptest.NewRecorder()
	handler.ServeHTTP(secondResponse, second)

	require.Equal(t, http.StatusCreated, secondResponse.Code)
	require.Equal(t, "created", secondResponse.Header().Get("X-Result"))
	require.Equal(t, `{"id":10}`, secondResponse.Body.String())
	require.Equal(t, int64(1), handlerCalls.Load())
	require.Equal(t, 1, store.completeCount())
	require.Equal(t, 0, store.releaseCount())
}

func TestIdempotencyStatusResponses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		store    idempotency.Store
		request  *http.Request
		wantCode int
	}{
		{
			name:     "missing key",
			store:    &fakeIdempotencyStore{},
			request:  httptest.NewRequest(http.MethodPost, "/orders", nil),
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "nil store",
			store:    nil,
			request:  requestWithKey(http.MethodPost, "/orders", nil),
			wantCode: http.StatusServiceUnavailable,
		},
		{
			name:     "in flight",
			store:    &fakeIdempotencyStore{reserveDecision: idempotency.Decision{Status: idempotency.StatusInFlight}},
			request:  requestWithKey(http.MethodPost, "/orders", nil),
			wantCode: http.StatusConflict,
		},
		{
			name:     "mismatch",
			store:    &fakeIdempotencyStore{reserveDecision: idempotency.Decision{Status: idempotency.StatusMismatch}},
			request:  requestWithKey(http.MethodPost, "/orders", nil),
			wantCode: http.StatusUnprocessableEntity,
		},
		{
			name:     "store unavailable",
			store:    &fakeIdempotencyStore{reserveErr: errors.New("down")},
			request:  requestWithKey(http.MethodPost, "/orders", nil),
			wantCode: http.StatusServiceUnavailable,
		},
		{
			name: "replay unavailable",
			store: &fakeIdempotencyStore{reserveDecision: idempotency.Decision{
				Status:   idempotency.StatusCompleted,
				Response: &idempotency.StoredResponse{StatusCode: http.StatusOK, Replayable: false},
			}},
			request:  requestWithKey(http.MethodPost, "/orders", nil),
			wantCode: http.StatusConflict,
		},
		{
			name:     "unknown store state",
			store:    &fakeIdempotencyStore{reserveDecision: idempotency.Decision{Status: "strange"}},
			request:  requestWithKey(http.MethodPost, "/orders", nil),
			wantCode: http.StatusServiceUnavailable,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			handlerCalled := false
			handler := Idempotency(IdempotencyOptions{Store: tt.store})(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
				handlerCalled = true
			}))

			response := httptest.NewRecorder()
			handler.ServeHTTP(response, tt.request)

			require.Equal(t, tt.wantCode, response.Code)
			require.False(t, handlerCalled)
		})
	}
}

func TestIdempotencyBypassAndInputFailures(t *testing.T) {
	t.Parallel()

	t.Run("safe method bypasses by default", func(t *testing.T) {
		t.Parallel()
		handler := Idempotency(IdempotencyOptions{Store: &fakeIdempotencyStore{}})(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNoContent)
		}))
		response := httptest.NewRecorder()

		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/orders", nil))

		require.Equal(t, http.StatusNoContent, response.Code)
	})

	t.Run("oversized request body returns bad request", func(t *testing.T) {
		t.Parallel()
		handler := Idempotency(IdempotencyOptions{
			Store:           &fakeIdempotencyStore{},
			MaxRequestBytes: 3,
		})(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			t.Fatal("handler should not run")
		}))
		response := httptest.NewRecorder()

		handler.ServeHTTP(response, requestWithKey(http.MethodPost, "/orders", strings.NewReader("abcd")))

		require.Equal(t, http.StatusBadRequest, response.Code)
	})

	t.Run("body read error returns bad request", func(t *testing.T) {
		t.Parallel()
		req := requestWithKey(http.MethodPost, "/orders", nil)
		req.Body = errReadCloser{}
		handler := Idempotency(IdempotencyOptions{Store: &fakeIdempotencyStore{}})(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			t.Fatal("handler should not run")
		}))
		response := httptest.NewRecorder()

		handler.ServeHTTP(response, req)

		require.Equal(t, http.StatusBadRequest, response.Code)
	})

	t.Run("fingerprint error returns bad request", func(t *testing.T) {
		t.Parallel()
		handler := Idempotency(IdempotencyOptions{
			Store: &fakeIdempotencyStore{},
			FingerprintFunc: func(*http.Request, []byte) (string, error) {
				return "", errors.New("bad fingerprint")
			},
		})(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			t.Fatal("handler should not run")
		}))
		response := httptest.NewRecorder()

		handler.ServeHTTP(response, requestWithKey(http.MethodPost, "/orders", nil))

		require.Equal(t, http.StatusBadRequest, response.Code)
	})

	t.Run("blank scope returns bad request", func(t *testing.T) {
		t.Parallel()
		handler := Idempotency(IdempotencyOptions{
			Store:     &fakeIdempotencyStore{},
			ScopeFunc: func(*http.Request) string { return " " },
		})(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			t.Fatal("handler should not run")
		}))
		response := httptest.NewRecorder()

		handler.ServeHTTP(response, requestWithKey(http.MethodPost, "/orders", nil))

		require.Equal(t, http.StatusBadRequest, response.Code)
	})
}

func TestIdempotencyDoesNotReplayUnstoredResponses(t *testing.T) {
	t.Parallel()

	store := &fakeIdempotencyStore{}
	var calls atomic.Int64
	handler := Idempotency(IdempotencyOptions{Store: store})(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		http.Error(w, "failed", http.StatusInternalServerError)
	}))

	for i := 0; i < 2; i++ {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, requestWithKey(http.MethodPost, "/orders", nil))
		require.Equal(t, http.StatusInternalServerError, response.Code)
	}

	require.Equal(t, int64(2), calls.Load())
	require.Equal(t, 0, store.completeCount())
	require.Equal(t, 2, store.releaseCount())
}

func TestIdempotencyOversizedResponseCompletesNonReplayable(t *testing.T) {
	t.Parallel()

	store := &fakeIdempotencyStore{}
	var calls atomic.Int64
	handler := Idempotency(IdempotencyOptions{
		Store:            store,
		MaxResponseBytes: 3,
	})(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		_, _ = w.Write([]byte("abcd"))
	}))

	first := httptest.NewRecorder()
	handler.ServeHTTP(first, requestWithKey(http.MethodPost, "/orders", nil))
	require.Equal(t, http.StatusOK, first.Code)
	require.Equal(t, "abcd", first.Body.String())

	second := httptest.NewRecorder()
	handler.ServeHTTP(second, requestWithKey(http.MethodPost, "/orders", nil))
	require.Equal(t, http.StatusConflict, second.Code)
	require.Equal(t, int64(1), calls.Load())
	require.False(t, store.completedResponse().Replayable)
}

func TestIdempotencyCompleteFailureFailsClosedBeforeSend(t *testing.T) {
	t.Parallel()

	store := &fakeIdempotencyStore{completeErr: errors.New("down")}
	handler := Idempotency(IdempotencyOptions{Store: store})(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("X-Should-Not-Leak", "yes")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte("created"))
	}))

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, requestWithKey(http.MethodPost, "/orders", nil))

	require.Equal(t, http.StatusServiceUnavailable, response.Code)
	require.Empty(t, response.Header().Get("X-Should-Not-Leak"))
	require.NotContains(t, response.Body.String(), "created")
	require.Equal(t, 1, store.completeCount())
}

func TestIdempotencyCompleteFailureAfterStreamingCannotRewriteResponse(t *testing.T) {
	t.Parallel()

	store := &fakeIdempotencyStore{completeErr: errors.New("down")}
	handler := Idempotency(IdempotencyOptions{
		Store:            store,
		MaxResponseBytes: 2,
	})(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte("created"))
	}))

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, requestWithKey(http.MethodPost, "/orders", nil))

	require.Equal(t, http.StatusCreated, response.Code)
	require.Equal(t, "created", response.Body.String())
	require.Equal(t, 1, store.completeCount())
}

func TestIdempotencyPanicReleasesAndRepanics(t *testing.T) {
	t.Parallel()

	store := &fakeIdempotencyStore{}
	handler := Idempotency(IdempotencyOptions{Store: store})(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic("boom")
	}))

	require.Panics(t, func() {
		handler.ServeHTTP(httptest.NewRecorder(), requestWithKey(http.MethodPost, "/orders", nil))
	})
	require.Equal(t, 1, store.releaseCount())
	require.Equal(t, 0, store.completeCount())
}

func TestIdempotencyOptionsAndHelpers(t *testing.T) {
	t.Parallel()

	req := httptest.NewRequest(http.MethodPatch, "/tenant/orders?q=1", strings.NewReader("body"))
	require.Equal(t, "PATCH /tenant/orders", defaultIdempotencyScope(req))
	require.Len(t, fingerprintWithScope(req, "tenant-1", []byte("body")), 64)

	nilURL := httptest.NewRequest(http.MethodPost, "/", nil)
	nilURL.URL = nil
	require.Equal(t, "/", requestRoutePath(nilURL))
	require.Equal(t, "/", requestRoutePath(&http.Request{URL: &url.URL{}}))

	bodyless := httptest.NewRequest(http.MethodPost, "/", nil)
	bodyless.Body = nil
	body, ok := readAndRestoreBody(bodyless, 1)
	require.True(t, ok)
	require.Nil(t, body)

	response := httptest.NewRecorder()
	replayStoredResponse(response, &idempotency.StoredResponse{Replayable: true, Body: []byte("ok")})
	require.Equal(t, http.StatusOK, response.Code)
	require.Equal(t, "ok", response.Body.String())

	cfg := normalizeIdempotencyOptions(IdempotencyOptions{
		HeaderName:       "X-Idempotency-Key",
		TTL:              time.Minute,
		Methods:          map[string]bool{http.MethodDelete: true},
		ScopeFunc:        func(*http.Request) string { return "scope" },
		FingerprintFunc:  func(*http.Request, []byte) (string, error) { return "fingerprint", nil },
		MaxRequestBytes:  10,
		MaxResponseBytes: 20,
		StoreStatus:      func(status int) bool { return status == http.StatusAccepted },
	})
	require.Equal(t, "X-Idempotency-Key", cfg.HeaderName)
	require.Equal(t, time.Minute, cfg.TTL)
	require.True(t, cfg.Methods[http.MethodDelete])
	require.Equal(t, "scope", cfg.ScopeFunc(req))
	fp, err := cfg.FingerprintFunc(req, nil)
	require.NoError(t, err)
	require.Equal(t, "fingerprint", fp)
	defaultFP, err := idempotencyFingerprint(normalizeIdempotencyOptions(IdempotencyOptions{}), req, "scope", []byte("body"))
	require.NoError(t, err)
	require.Len(t, defaultFP, 64)
	require.Equal(t, int64(10), cfg.MaxRequestBytes)
	require.Equal(t, int64(20), cfg.MaxResponseBytes)
	require.True(t, cfg.StoreStatus(http.StatusAccepted))
}

func TestBoundedResponseWriterOptionalInterfaces(t *testing.T) {
	t.Parallel()

	recorder := httptest.NewRecorder()
	writer := newBoundedResponseWriter(recorder, 10)
	writer.Flush()
	require.Equal(t, http.StatusOK, recorder.Code)
	_, _, err := writer.Hijack()
	require.Error(t, err)
	require.ErrorIs(t, writer.Push("/asset", nil), http.ErrNotSupported)

	hijacker := &fakeHijackResponseWriter{ResponseRecorder: httptest.NewRecorder()}
	writer = newBoundedResponseWriter(hijacker, 10)
	conn, rw, err := writer.Hijack()
	require.NoError(t, err)
	require.Nil(t, conn)
	require.Nil(t, rw)

	pusher := &fakePushResponseWriter{ResponseRecorder: httptest.NewRecorder()}
	writer = newBoundedResponseWriter(pusher, 10)
	require.NoError(t, writer.Push("/asset", nil))
	require.True(t, pusher.pushed)

	_, err = writer.Write([]byte("hello"))
	require.NoError(t, err)
	require.Equal(t, []byte("hello"), writer.body())
	require.Empty(t, pusher.Body.String())
	require.NoError(t, writer.send())
	require.Equal(t, "hello", pusher.Body.String())

	writer.WriteHeader(http.StatusAccepted)
	writer.WriteHeader(http.StatusCreated)
	require.Equal(t, http.StatusOK, writer.statusCode())

	quiet := newBoundedResponseWriter(httptest.NewRecorder(), 1)
	require.Equal(t, http.StatusOK, quiet.statusCode())
	require.False(t, quiet.capture([]byte("ab")))
	quiet.startStreaming([]byte("ab"))
	quiet.startStreaming([]byte("c"))
	require.False(t, quiet.replayable())
	require.Nil(t, quiet.body())

	prefixRecorder := httptest.NewRecorder()
	prefix := newBoundedResponseWriter(prefixRecorder, 3)
	_, err = prefix.Write([]byte("abc"))
	require.NoError(t, err)
	_, err = prefix.Write([]byte("d"))
	require.NoError(t, err)
	require.Equal(t, "abcd", prefixRecorder.Body.String())

	empty := newBoundedResponseWriter(httptest.NewRecorder(), 10)
	require.NoError(t, empty.send())
	require.NoError(t, empty.send())

	stream := newBoundedResponseWriter(httptest.NewRecorder(), 10)
	stream.Flush()
	_, err = stream.Write([]byte("x"))
	require.NoError(t, err)

	errWriter := newBoundedResponseWriter(errorResponseWriter{}, 1)
	_, err = errWriter.Write([]byte("overflow"))
	require.Error(t, err)
	require.Error(t, errWriter.send())

	errWriterWithPrefix := newBoundedResponseWriter(errorResponseWriter{}, 3)
	_, err = errWriterWithPrefix.Write([]byte("abc"))
	require.NoError(t, err)
	_, err = errWriterWithPrefix.Write([]byte("d"))
	require.Error(t, err)
	require.False(t, errWriterWithPrefix.capture([]byte("e")))
}

func requestWithKey(method, target string, body io.Reader) *http.Request {
	req := httptest.NewRequest(method, target, body)
	req.Header.Set(defaultIdempotencyHeader, "key-1")
	return req
}

type fakeIdempotencyStore struct {
	mu              sync.Mutex
	reserveDecision idempotency.Decision
	reserveErr      error
	completeErr     error
	releaseErr      error
	completed       *idempotency.StoredResponse
	completes       int
	releases        int
}

func (s *fakeIdempotencyStore) Reserve(_ context.Context, _ idempotency.Request) (idempotency.Decision, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.reserveErr != nil {
		return idempotency.Decision{}, s.reserveErr
	}
	if s.reserveDecision.Status != "" {
		return s.reserveDecision, nil
	}
	if s.completed != nil {
		return idempotency.Decision{Status: idempotency.StatusCompleted, Response: cloneStoredResponse(s.completed)}, nil
	}
	return idempotency.Decision{Status: idempotency.StatusReserved}, nil
}

func (s *fakeIdempotencyStore) Complete(_ context.Context, req idempotency.CompleteRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.completes++
	if s.completeErr != nil {
		return s.completeErr
	}
	s.completed = cloneStoredResponse(&req.Response)
	return nil
}

func (s *fakeIdempotencyStore) Release(context.Context, idempotency.ReleaseRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.releases++
	return s.releaseErr
}

func (s *fakeIdempotencyStore) completeCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.completes
}

func (s *fakeIdempotencyStore) releaseCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.releases
}

func (s *fakeIdempotencyStore) completedResponse() *idempotency.StoredResponse {
	s.mu.Lock()
	defer s.mu.Unlock()
	return cloneStoredResponse(s.completed)
}

func cloneStoredResponse(response *idempotency.StoredResponse) *idempotency.StoredResponse {
	if response == nil {
		return nil
	}
	cloned := *response
	cloned.Body = append([]byte(nil), response.Body...)
	cloned.Header = cloneHeader(response.Header)
	return &cloned
}

type errReadCloser struct{}

func (errReadCloser) Read([]byte) (int, error) {
	return 0, errors.New("read failed")
}

func (errReadCloser) Close() error {
	return nil
}

type fakeHijackResponseWriter struct {
	*httptest.ResponseRecorder
}

func (w *fakeHijackResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return nil, nil, nil
}

type fakePushResponseWriter struct {
	*httptest.ResponseRecorder
	pushed bool
}

func (w *fakePushResponseWriter) Push(string, *http.PushOptions) error {
	w.pushed = true
	return nil
}

type errorResponseWriter struct{}

func (errorResponseWriter) Header() http.Header {
	return make(http.Header)
}

func (errorResponseWriter) Write([]byte) (int, error) {
	return 0, errors.New("write failed")
}

func (errorResponseWriter) WriteHeader(int) {}
