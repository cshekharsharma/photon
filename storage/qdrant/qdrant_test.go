package qdrant

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type tempNetErr struct {
	timeout   bool
	temporary bool
}

func (e tempNetErr) Error() string   { return "temp net error" }
func (e tempNetErr) Timeout() bool   { return e.timeout }
func (e tempNetErr) Temporary() bool { return e.temporary }

type qdrantCloseErrorBody struct {
	*strings.Reader
}

func (b qdrantCloseErrorBody) Close() error {
	return errors.New("close failed")
}

func TestNewRESTClient_ValidationAndDefaults(t *testing.T) {
	if _, err := NewRESTClient(nil); err == nil {
		t.Fatalf("expected error for nil config")
	}
	if _, err := NewRESTClient(&ConnectionConfig{BaseURL: ""}); err == nil {
		t.Fatalf("expected error for empty BaseURL")
	}
	if _, err := NewRESTClient(&ConnectionConfig{BaseURL: "example.com"}); err == nil {
		t.Fatalf("expected error for invalid BaseURL")
	}
	badURLs := []string{
		"ftp://example.com",
		"http://user:pass@example.com",
		"http://example.com?q=1",
		"http://example.com#frag",
	}
	for _, raw := range badURLs {
		if _, err := NewRESTClient(&ConnectionConfig{BaseURL: raw}); err == nil {
			t.Fatalf("expected error for unsafe BaseURL %q", raw)
		}
	}

	cfg := &ConnectionConfig{BaseURL: "http://example.com/api/", RetryJitter: 2}
	c, err := NewRESTClient(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c.baseURL != "http://example.com/api" {
		t.Fatalf("expected baseURL trimmed, got %q", c.baseURL)
	}
	if c.retryJitter != 0.2 {
		t.Fatalf("expected jitter default to 0.2, got %v", c.retryJitter)
	}
	if c.httpClient.Timeout <= 0 {
		t.Fatalf("expected default timeout")
	}
	got, err := c.requestURL("/collections/test?wait=true")
	if err != nil {
		t.Fatalf("requestURL: %v", err)
	}
	if got != "http://example.com/api/collections/test?wait=true" {
		t.Fatalf("unexpected request URL: %s", got)
	}
	if _, err := c.requestURL("relative"); err == nil {
		t.Fatalf("expected relative path error")
	}
}

func TestRESTClient_RedirectPolicy(t *testing.T) {
	c, err := NewRESTClient(&ConnectionConfig{BaseURL: "https://example.com"})
	if err != nil {
		t.Fatalf("NewRESTClient: %v", err)
	}

	sameHostReq, _ := http.NewRequest(http.MethodGet, "https://example.com/next", nil)
	firstReq, _ := http.NewRequest(http.MethodGet, "https://example.com/start", nil)
	if err := c.httpClient.CheckRedirect(sameHostReq, []*http.Request{firstReq}); err != nil {
		t.Fatalf("expected same-host redirect to be allowed: %v", err)
	}

	otherHostReq, _ := http.NewRequest(http.MethodGet, "https://other.example/next", nil)
	if err := c.httpClient.CheckRedirect(otherHostReq, []*http.Request{firstReq}); !errors.Is(err, http.ErrUseLastResponse) {
		t.Fatalf("expected cross-host redirect to stop, got %v", err)
	}
}

func TestCryptoFloat64_ReadError(t *testing.T) {
	originalRead := cryptoRandRead
	cryptoRandRead = func([]byte) (int, error) {
		return 0, errors.New("entropy unavailable")
	}
	t.Cleanup(func() { cryptoRandRead = originalRead })

	if got := cryptoFloat64(); got != 1 {
		t.Fatalf("expected fallback value 1, got %v", got)
	}
}

func TestRESTClientMethods_HappyPath(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		switch r.URL.Path {
		case "/healthz":
			if r.Method != http.MethodGet {
				t.Fatalf("health method: %s", r.Method)
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"ok"}`))
		case "/collections/test/points":
			if r.Method != http.MethodPut {
				t.Fatalf("upsert method: %s", r.Method)
			}
			if r.URL.RawQuery != "wait=true" {
				t.Fatalf("expected wait=true")
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{}`))
		case "/collections/test/points/delete":
			if r.Method != http.MethodPost {
				t.Fatalf("delete points method: %s", r.Method)
			}
			if r.URL.RawQuery != "wait=true" {
				t.Fatalf("expected wait=true")
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{}`))
		case "/collections/test/points/search":
			if r.Method != http.MethodPost {
				t.Fatalf("search method: %s", r.Method)
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"result":[{"id":1,"score":0.5}],"status":"ok"}`))
		case "/collections/test":
			switch r.Method {
			case http.MethodPut, http.MethodDelete:
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{}`))
			case http.MethodGet:
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{"result":{"status":"green"},"status":"ok"}`))
			default:
				t.Fatalf("unexpected method: %s", r.Method)
			}
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	c := &restClient{
		baseURL:          srv.URL,
		httpClient:       srv.Client(),
		retryMaxAttempts: 1,
		retryBaseDelay:   0,
		retryMaxDelay:    0,
		retryJitter:      0,
	}

	ctx := context.Background()
	if err := c.Health(ctx); err != nil {
		t.Fatalf("health: %v", err)
	}
	if err := c.UpsertPoints(ctx, UpsertPointsRequest{Collection: "test", Points: []Point{{ID: PointIDInt(1), Vector: []float32{1}}}, Wait: true}); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	if err := c.DeletePoints(ctx, DeletePointsRequest{Collection: "test", IDs: []PointID{PointIDInt(1)}, Wait: true}); err != nil {
		t.Fatalf("delete points: %v", err)
	}
	_, err := c.Search(ctx, SearchRequest{Collection: "test", Vector: []float32{1}, Limit: 1, Offset: 0, WithPayload: true, WithVector: true, ScoreThreshold: func() *float32 { v := float32(0.1); return &v }(), Filter: json.RawMessage(`{"must":[]}`)})
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if err := c.CreateCollection(ctx, CreateCollectionRequest{Collection: "test", Config: CollectionConfig{Vectors: VectorsConfig{Single: &VectorParams{Size: 4, Distance: DistanceCosine}}}}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	if err := c.DeleteCollection(ctx, "test"); err != nil {
		t.Fatalf("delete collection: %v", err)
	}
	if _, err := c.GetCollectionInfo(ctx, "test"); err != nil {
		t.Fatalf("get collection info: %v", err)
	}
	if atomic.LoadInt32(&calls) == 0 {
		t.Fatalf("expected server to be called")
	}
}

func TestRESTClientMethods_InvalidRequests(t *testing.T) {
	c := &restClient{}
	ctx := context.Background()
	if err := c.UpsertPoints(ctx, UpsertPointsRequest{}); err == nil {
		t.Fatalf("expected error for invalid upsert")
	}
	if err := c.DeletePoints(ctx, DeletePointsRequest{}); err == nil {
		t.Fatalf("expected error for invalid delete points")
	}
	if _, err := c.Search(ctx, SearchRequest{}); err == nil {
		t.Fatalf("expected error for invalid search")
	}
	if err := c.CreateCollection(ctx, CreateCollectionRequest{}); err == nil {
		t.Fatalf("expected error for invalid create collection")
	}
	if err := c.DeleteCollection(ctx, ""); err == nil {
		t.Fatalf("expected error for invalid delete collection")
	}
	if _, err := c.GetCollectionInfo(ctx, ""); err == nil {
		t.Fatalf("expected error for invalid get collection info")
	}
}

func TestSearch_InvalidFilterJSON(t *testing.T) {
	c := &restClient{}
	_, err := c.Search(context.Background(), SearchRequest{Collection: "c", Vector: []float32{1}, Limit: 1, Filter: json.RawMessage(`{bad json}`)})
	if err == nil {
		t.Fatalf("expected error for invalid filter JSON")
	}
}

func TestDoJSON_EmptyBodyOK(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	c := &restClient{baseURL: srv.URL, httpClient: srv.Client(), retryMaxAttempts: 1}
	var out map[string]any
	if err := c.doJSON(context.Background(), http.MethodGet, "/empty", nil, &out); err != nil {
		t.Fatalf("expected no error on empty body, got: %v", err)
	}
}

func TestDoJSON_RetryOn429(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c := atomic.AddInt32(&calls, 1)
		if c == 1 {
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte("rate limited"))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()

	c := &restClient{baseURL: srv.URL, httpClient: srv.Client(), retryMaxAttempts: 2, retryBaseDelay: 0, retryMaxDelay: 0}
	var out struct {
		OK bool `json:"ok"`
	}
	if err := c.doJSON(context.Background(), http.MethodGet, "/retry", nil, &out); err != nil {
		t.Fatalf("expected success after retry, got: %v", err)
	}
	if atomic.LoadInt32(&calls) != 2 {
		t.Fatalf("expected 2 calls, got %d", atomic.LoadInt32(&calls))
	}
}

func TestDoJSON_HTTPErrorMapping(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte("nope"))
	}))
	defer srv.Close()

	c := &restClient{baseURL: srv.URL, httpClient: srv.Client(), retryMaxAttempts: 1}
	var out map[string]any
	err := c.doJSON(context.Background(), http.MethodGet, "/nf", nil, &out)
	if err == nil {
		t.Fatalf("expected error")
	}
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound")
	}
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) || httpErr.StatusCode != http.StatusNotFound {
		t.Fatalf("expected HTTPError with status 404")
	}
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestDoJSON_RetryableNetError(t *testing.T) {
	var calls int32
	rt := roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		c := atomic.AddInt32(&calls, 1)
		if c == 1 {
			return nil, tempNetErr{timeout: true}
		}
		body := io.NopCloser(strings.NewReader(`{"ok":true}`))
		return &http.Response{StatusCode: http.StatusOK, Body: body, Header: make(http.Header)}, nil
	})

	c := &restClient{
		baseURL:          "http://example.com",
		httpClient:       &http.Client{Transport: rt},
		retryMaxAttempts: 2,
		retryBaseDelay:   0,
		retryMaxDelay:    0,
		retryJitter:      0,
	}
	var out struct {
		OK bool `json:"ok"`
	}
	if err := c.doJSON(context.Background(), http.MethodGet, "/x", nil, &out); err != nil {
		t.Fatalf("expected success after retry, got: %v", err)
	}
	if atomic.LoadInt32(&calls) != 2 {
		t.Fatalf("expected 2 calls, got %d", atomic.LoadInt32(&calls))
	}
}

func TestDoJSON_NonRetryableNetError(t *testing.T) {
	var calls int32
	rt := roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		atomic.AddInt32(&calls, 1)
		return nil, errors.New("boom")
	})

	c := &restClient{
		baseURL:          "http://example.com",
		httpClient:       &http.Client{Transport: rt},
		retryMaxAttempts: 2,
		retryBaseDelay:   0,
		retryMaxDelay:    0,
	}
	var out map[string]any
	if err := c.doJSON(context.Background(), http.MethodGet, "/x", nil, &out); err == nil {
		t.Fatalf("expected error")
	}
	if atomic.LoadInt32(&calls) != 1 {
		t.Fatalf("expected 1 call, got %d", atomic.LoadInt32(&calls))
	}
}

func TestBackoffAndSleepAndRetryable(t *testing.T) {
	c := &restClient{retryBaseDelay: 100 * time.Millisecond, retryMaxDelay: 200 * time.Millisecond, retryJitter: 0}
	d := c.backoff(1)
	if d != 100*time.Millisecond {
		t.Fatalf("unexpected backoff: %v", d)
	}
	d = c.backoff(3)
	if d != 200*time.Millisecond {
		t.Fatalf("expected clamped backoff, got: %v", d)
	}
	c.retryJitter = 0.5
	d = c.backoff(2)
	if d < 100*time.Millisecond || d > 300*time.Millisecond {
		t.Fatalf("backoff out of expected jitter range: %v", d)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := sleepCtx(ctx, 10*time.Millisecond); err == nil {
		t.Fatalf("expected context error")
	}
	if err := sleepCtx(context.Background(), 0); err != nil {
		t.Fatalf("unexpected sleep error: %v", err)
	}

	if !isRetryableNetErr(tempNetErr{timeout: true}) {
		t.Fatalf("expected retryable net error")
	}
	if !isRetryableNetErr(errors.New("connection reset by peer")) {
		t.Fatalf("expected retryable string error")
	}
	if isRetryableNetErr(errors.New("boom")) {
		t.Fatalf("expected non-retryable error")
	}
}

func TestDoJSON_RequestFailedWhenNoAttempts(t *testing.T) {
	c := &restClient{retryMaxAttempts: 0}
	if err := c.doJSON(context.Background(), http.MethodGet, "/x", nil, nil); err == nil {
		t.Fatalf("expected request failed error")
	}
}

func TestDoJSON_InvalidJSONResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("{invalid"))
	}))
	defer srv.Close()

	c := &restClient{baseURL: srv.URL, httpClient: srv.Client(), retryMaxAttempts: 1}
	var out map[string]any
	if err := c.doJSON(context.Background(), http.MethodGet, "/bad", nil, &out); err == nil {
		t.Fatalf("expected unmarshal error")
	}
}

func TestDoJSON_InvalidRequestJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	c := &restClient{baseURL: srv.URL, httpClient: srv.Client(), retryMaxAttempts: 1}
	bad := func() {}
	if err := c.doJSON(context.Background(), http.MethodPost, "/bad", bad, nil); err == nil {
		t.Fatalf("expected marshal error")
	}
}

func TestDoJSON_ReadBodyError(t *testing.T) {
	rt := roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(&errReader{}),
			Header:     make(http.Header),
		}, nil
	})

	c := &restClient{baseURL: "http://example.com", httpClient: &http.Client{Transport: rt}, retryMaxAttempts: 2, retryBaseDelay: 0, retryMaxDelay: 0}
	var out map[string]any
	if err := c.doJSON(context.Background(), http.MethodGet, "/x", nil, &out); err == nil {
		t.Fatalf("expected read error")
	}
}

type errReader struct{}

func (r *errReader) Read(p []byte) (int, error) { return 0, io.ErrUnexpectedEOF }

func TestDoJSON_StatusRetryThenSuccess(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c := atomic.AddInt32(&calls, 1)
		if c == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte("server error"))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()

	c := &restClient{baseURL: srv.URL, httpClient: srv.Client(), retryMaxAttempts: 2, retryBaseDelay: 0, retryMaxDelay: 0}
	var out map[string]any
	if err := c.doJSON(context.Background(), http.MethodGet, "/x", nil, &out); err != nil {
		t.Fatalf("expected success after retry: %v", err)
	}
}

func TestDoJSON_RequestBuildError(t *testing.T) {
	c := &restClient{baseURL: "http://example.com", httpClient: &http.Client{}, retryMaxAttempts: 1}
	if err := c.doJSON(context.Background(), http.MethodGet, "relative", nil, nil); err == nil {
		t.Fatalf("expected request build error")
	}
}

func TestNewRequest_ErrorBranches(t *testing.T) {
	c := &restClient{baseURL: "://bad"}
	if _, err := c.newRequest(context.Background(), http.MethodGet, "/x", nil, false); err == nil {
		t.Fatalf("expected invalid base URL error")
	}

	c.baseURL = "http://example.com"
	if _, err := c.newRequest(context.Background(), "\n", "/x", nil, false); err == nil {
		t.Fatalf("expected invalid method error")
	}
}

func TestIsRetryableNetErr_NetErrorInterface(t *testing.T) {
	var ne net.Error = tempNetErr{timeout: true, temporary: false}
	if !isRetryableNetErr(ne) {
		t.Fatalf("expected retryable for timeout net error")
	}

	ne = tempNetErr{timeout: false, temporary: true}
	if !isRetryableNetErr(ne) {
		t.Fatalf("expected retryable for temporary net error")
	}

	if isRetryableNetErr(context.Canceled) {
		t.Fatalf("expected context cancellation to be non-retryable")
	}
}

func TestDoJSON_ContentTypeHeader(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Content-Type") != "application/json" {
			t.Fatalf("expected Content-Type application/json")
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	c := &restClient{baseURL: srv.URL, httpClient: srv.Client(), retryMaxAttempts: 1}
	payload := map[string]string{"k": "v"}
	if err := c.doJSON(context.Background(), http.MethodPost, "/ct", payload, nil); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDoJSON_APIKeyHeader(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("api-key") != "secret" {
			t.Fatalf("expected api-key header")
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	c := &restClient{baseURL: srv.URL, httpClient: srv.Client(), apiKey: "secret", retryMaxAttempts: 1}
	if err := c.doJSON(context.Background(), http.MethodGet, "/k", nil, nil); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDoJSON_ZeroPayload(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buf := new(bytes.Buffer)
		_, _ = io.Copy(buf, r.Body)
		if buf.Len() != 0 {
			t.Fatalf("expected empty body")
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	c := &restClient{baseURL: srv.URL, httpClient: srv.Client(), retryMaxAttempts: 1}
	if err := c.doJSON(context.Background(), http.MethodGet, "/z", nil, nil); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSearch_NoFilterNoThreshold(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/collections/c/points/search" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		if _, ok := payload["filter"]; ok {
			t.Fatalf("did not expect filter in payload")
		}
		if _, ok := payload["score_threshold"]; ok {
			t.Fatalf("did not expect score_threshold in payload")
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"result":[],"status":"ok"}`))
	}))
	defer srv.Close()

	c := &restClient{baseURL: srv.URL, httpClient: srv.Client(), retryMaxAttempts: 1}
	_, err := c.Search(context.Background(), SearchRequest{
		Collection: "c",
		Vector:     []float32{1, 2},
		Limit:      1,
	})
	if err != nil {
		t.Fatalf("search: %v", err)
	}
}

func TestCreateCollection_InvalidConfig(t *testing.T) {
	c := &restClient{}
	err := c.CreateCollection(context.Background(), CreateCollectionRequest{
		Collection: "ok",
		Config:     CollectionConfig{},
	})
	if err == nil {
		t.Fatalf("expected error from invalid config")
	}
}

func TestGetCollectionInfo_ErrorFromHTTP(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`boom`))
	}))
	defer srv.Close()

	c := &restClient{baseURL: srv.URL, httpClient: srv.Client(), retryMaxAttempts: 1}
	if _, err := c.GetCollectionInfo(context.Background(), "c"); err == nil {
		t.Fatalf("expected error")
	}
}

func TestDoJSON_HTTPErrorWithoutMappedSentinel(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTeapot)
		_, _ = w.Write([]byte("teapot"))
	}))
	defer srv.Close()

	c := &restClient{baseURL: srv.URL, httpClient: srv.Client(), retryMaxAttempts: 1}
	err := c.doJSON(context.Background(), http.MethodGet, "/teapot", nil, nil)
	if err == nil {
		t.Fatalf("expected error")
	}
	if errors.Is(err, ErrBadRequest) || errors.Is(err, ErrServer) {
		t.Fatalf("did not expect mapped sentinel error: %v", err)
	}
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) || httpErr.StatusCode != http.StatusTeapot {
		t.Fatalf("expected raw HTTPError with 418")
	}
}

func TestDoJSON_RetryableNetError_ContextCanceledBeforeSleep(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	rt := roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		cancel()
		return nil, tempNetErr{timeout: true}
	})

	c := &restClient{
		baseURL:          "http://example.com",
		httpClient:       &http.Client{Transport: rt},
		retryMaxAttempts: 2,
		retryBaseDelay:   time.Second,
		retryMaxDelay:    time.Second,
	}
	err := c.doJSON(ctx, http.MethodGet, "/x", nil, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context canceled, got: %v", err)
	}
}

func TestDoJSON_ReadBodyErrorThenSuccess(t *testing.T) {
	var calls int32
	rt := roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		c := atomic.AddInt32(&calls, 1)
		if c == 1 {
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(&errReader{}),
				Header:     make(http.Header),
			}, nil
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(`{"ok":true}`)),
			Header:     make(http.Header),
		}, nil
	})

	c := &restClient{baseURL: "http://example.com", httpClient: &http.Client{Transport: rt}, retryMaxAttempts: 2, retryBaseDelay: 0, retryMaxDelay: 0}
	var out map[string]any
	if err := c.doJSON(context.Background(), http.MethodGet, "/x", nil, &out); err != nil {
		t.Fatalf("expected success after retry, got: %v", err)
	}
	if atomic.LoadInt32(&calls) != 2 {
		t.Fatalf("expected 2 calls, got %d", atomic.LoadInt32(&calls))
	}
}

func TestDoJSON_StatusRetry_ContextCanceledBeforeSleep(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("server error"))
	}))
	defer srv.Close()

	time.Sleep(2 * time.Millisecond)
	c := &restClient{baseURL: srv.URL, httpClient: srv.Client(), retryMaxAttempts: 2, retryBaseDelay: time.Second, retryMaxDelay: time.Second}
	err := c.doJSON(ctx, http.MethodGet, "/x", nil, nil)
	if err == nil {
		t.Fatalf("expected context cancellation error")
	}
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context cancellation error, got: %v", err)
	}
}

func TestIsRetryableNetErr_NonRetryableNetError(t *testing.T) {
	var ne net.Error = tempNetErr{timeout: false, temporary: false}
	if isRetryableNetErr(ne) {
		t.Fatalf("expected non-retryable for net error without timeout/temporary")
	}
}

func TestHandleRequestError_ContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	c := &restClient{retryMaxAttempts: 2}
	retry, err := c.handleRequestError(ctx, 1, tempNetErr{timeout: true})

	if retry {
		t.Fatal("did not expect retry after context cancellation")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context canceled, got %v", err)
	}
}

func TestHandleRequestError_RequestCanceled(t *testing.T) {
	c := &restClient{retryMaxAttempts: 2}
	retry, err := c.handleRequestError(context.Background(), 1, context.Canceled)

	if retry {
		t.Fatal("did not expect retry for canceled request")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context canceled, got %v", err)
	}
}

func TestHandleRequestError_ContextCanceledDuringSleep(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(5 * time.Millisecond)
		cancel()
	}()

	c := &restClient{
		retryMaxAttempts: 2,
		retryBaseDelay:   time.Second,
		retryMaxDelay:    time.Second,
	}
	retry, err := c.handleRequestError(ctx, 1, tempNetErr{timeout: true})

	if retry {
		t.Fatal("did not expect retry after context cancellation")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context canceled, got %v", err)
	}
}

func TestIsRetryableNetErr_StringSignals(t *testing.T) {
	tests := []string{
		"broken pipe",
		"request timeout",
		"tls handshake timeout",
		"connection refused",
	}
	for _, msg := range tests {
		if !isRetryableNetErr(errors.New(msg)) {
			t.Fatalf("expected retryable for %q", msg)
		}
	}
}

func TestSearch_ReturnsDoJSONError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`boom`))
	}))
	defer srv.Close()

	c := &restClient{baseURL: srv.URL, httpClient: srv.Client(), retryMaxAttempts: 1}
	_, err := c.Search(context.Background(), SearchRequest{
		Collection: "test",
		Vector:     []float32{1},
		Limit:      1,
	})
	if err == nil {
		t.Fatalf("expected search error")
	}
}

func TestDoJSON_ReadBodyError_ContextCanceledBeforeSleep(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	rt := roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		go func() {
			time.Sleep(5 * time.Millisecond)
			cancel()
		}()
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(&errReader{}),
			Header:     make(http.Header),
		}, nil
	})

	c := &restClient{
		baseURL:          "http://example.com",
		httpClient:       &http.Client{Transport: rt},
		retryMaxAttempts: 2,
		retryBaseDelay:   time.Second,
		retryMaxDelay:    time.Second,
	}
	err := c.doJSON(ctx, http.MethodGet, "/x", nil, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context canceled, got: %v", err)
	}
}

func TestDoJSON_CloseBodyErrorThenSuccess(t *testing.T) {
	var calls int32
	rt := roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		c := atomic.AddInt32(&calls, 1)
		if c == 1 {
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       qdrantCloseErrorBody{Reader: strings.NewReader(`{"ok":true}`)},
				Header:     make(http.Header),
			}, nil
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(`{"ok":true}`)),
			Header:     make(http.Header),
		}, nil
	})

	c := &restClient{baseURL: "http://example.com", httpClient: &http.Client{Transport: rt}, retryMaxAttempts: 2, retryBaseDelay: 0, retryMaxDelay: 0}
	var out map[string]any
	if err := c.doJSON(context.Background(), http.MethodGet, "/x", nil, &out); err != nil {
		t.Fatalf("expected success after close retry, got: %v", err)
	}
	if atomic.LoadInt32(&calls) != 2 {
		t.Fatalf("expected 2 calls, got %d", atomic.LoadInt32(&calls))
	}
}

func TestDoJSON_CloseBodyErrorFinalAttempt(t *testing.T) {
	rt := roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       qdrantCloseErrorBody{Reader: strings.NewReader(`{"ok":true}`)},
			Header:     make(http.Header),
		}, nil
	})

	c := &restClient{baseURL: "http://example.com", httpClient: &http.Client{Transport: rt}, retryMaxAttempts: 1}
	err := c.doJSON(context.Background(), http.MethodGet, "/x", nil, nil)
	if err == nil || !strings.Contains(err.Error(), "close failed") {
		t.Fatalf("expected close error, got: %v", err)
	}
}

func TestDoJSON_CloseBodyError_ContextCanceledBeforeSleep(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	rt := roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		cancel()
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       qdrantCloseErrorBody{Reader: strings.NewReader(`{"ok":true}`)},
			Header:     make(http.Header),
		}, nil
	})

	c := &restClient{baseURL: "http://example.com", httpClient: &http.Client{Transport: rt}, retryMaxAttempts: 2, retryBaseDelay: time.Second, retryMaxDelay: time.Second}
	err := c.doJSON(ctx, http.MethodGet, "/x", nil, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context canceled, got: %v", err)
	}
}

func TestDoJSON_StatusRetry_ContextCanceledDuringSleep(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		go func() {
			time.Sleep(5 * time.Millisecond)
			cancel()
		}()
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("server error"))
	}))
	defer srv.Close()

	c := &restClient{baseURL: srv.URL, httpClient: srv.Client(), retryMaxAttempts: 2, retryBaseDelay: time.Second, retryMaxDelay: time.Second}
	err := c.doJSON(ctx, http.MethodGet, "/x", nil, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context canceled, got: %v", err)
	}
}

func TestBackoff_NegativeJitterClampedToZero(t *testing.T) {
	c := &restClient{
		retryBaseDelay: 10 * time.Millisecond,
		retryMaxDelay:  10 * time.Millisecond,
		retryJitter:    1e6,
	}
	for i := 0; i < 10000; i++ {
		if c.backoff(1) == 0 {
			return
		}
	}
	t.Fatalf("expected at least one negative jitter clamp to zero")
}
