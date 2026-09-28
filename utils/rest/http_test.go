package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/cshekharsharma/photon/utils/filesys"
	"github.com/cshekharsharma/photon/utils/rest/httpstub"
	"github.com/stretchr/testify/assert"
)

// Mock HttpClient for testing
type mockHttpClient struct {
	resp *http.Response
	err  error
}

func (m *mockHttpClient) Do(req *http.Request) (*http.Response, error) {
	return m.resp, m.err
}

type errReadCloser struct {
	err error
}

func (e errReadCloser) Read(p []byte) (int, error) {
	return 0, e.err
}

func (e errReadCloser) Close() error {
	return nil
}

type closeErrReadCloser struct {
	*strings.Reader
}

func (e closeErrReadCloser) Close() error {
	return errors.New("close failed")
}

// Utility to create a JSON response
func jsonResponse(body interface{}, statusCode int) *http.Response {
	data, _ := json.Marshal(body)
	return &http.Response{
		StatusCode: statusCode,
		Body:       io.NopCloser(strings.NewReader(string(data))),
		Status:     http.StatusText(statusCode),
		Header:     make(http.Header),
	}
}

// Utility to create a non-JSON response
func nonJsonResponse(body string, statusCode int) *http.Response {
	return &http.Response{
		StatusCode: statusCode,
		Body:       io.NopCloser(strings.NewReader(body)),
		Status:     http.StatusText(statusCode),
		Header:     make(http.Header),
	}
}

func TestMakeHTTPRequest(t *testing.T) {
	tests := []struct {
		name        string
		request     RequestEntity
		stubEnabled bool
		setupStubs  func(*testing.T)
		clientResp  *http.Response
		clientErr   error
		wantErr     bool
		errContains string
		wantCheck   func(map[string]interface{}, *testing.T)
	}{
		{
			name: "Invalid URL",
			request: RequestEntity{
				Url:    "http://127.0.0.1:80/%zz",
				Method: http.MethodGet,
			},
			stubEnabled: false,
			wantErr:     true,
			errContains: "invalid URL escape",
		},
		{
			name: "GET request with query params - success",
			request: RequestEntity{
				Url:    "http://example.com",
				Method: http.MethodGet,
				QueryParams: map[string][]string{
					"foo": {"bar", "baz"},
				},
			},
			stubEnabled: false,
			clientResp:  jsonResponse(map[string]interface{}{"ok": true}, http.StatusOK),
			wantCheck: func(resp map[string]interface{}, tt *testing.T) {
				if resp["ok"] != true {
					tt.Error("expected ok=true in response")
				}
			},
		},
		{
			name: "Stub enabled, stub returns error",
			request: RequestEntity{
				Url:    "http://example.com",
				Method: http.MethodGet,
				StubID: "stub-error",
			},
			stubEnabled: true,
			setupStubs: func(*testing.T) {
				// Add a stub that will return error only if we want by adjusting CallStub logic,
				// but we can't mock CallStub. Instead, we won't add a stub with ID "stub-error"
				// so CallStub returns nil. Wait, we need error from stub?
				// Actually, we can't force CallStub to return error without mocking.
				// Let's add a stub that always returns a negative response with no probability of positive:
				// If we want a stub error scenario, we rely on a scenario that triggers error from stub:
				// In the current code, CallStub won't return an error itself unless latency or max hits...
				// Without mocking CallStub, we can't force it to return error. Let's choose another scenario:
				// We'll test that if stub returns a non-2xx response, it errors out. That's different from a direct stubErr.
				// Let's rename this test scenario for a non-2xx stub response scenario:
			},
			// We'll repurpose this test to "Stub enabled, stub returns non-2xx status"
			clientResp:  nil, // won't be reached
			wantErr:     true,
			errContains: "not found",
		},
		{
			name: "Stub enabled, stub returns valid response",
			request: RequestEntity{
				Url:    "http://example.com",
				Method: http.MethodGet,
				StubID: "stub-success",
			},
			stubEnabled: true,
			setupStubs: func(t *testing.T) {
				// Add a stub that returns a positive 200 response
				httpstub.ClearAllStubs()
				entry := &httpstub.StubEntry{
					Endpoint:         "/ignored",
					PositiveResponse: `{"stub":"ok"}`,
					ResponseCode:     http.StatusOK,
					Probability:      1.0,
				}
				_, err := httpstub.AddStub(entry)
				assert.NoError(t, err)
				// The stub ID from AddStub is generated; we can't control easily.
				// Wait, we must use the ID user gave. Let's set entry.ID = "stub-success"
				entry.ID = "stub-success"
				_, err = httpstub.AddStub(entry)
				assert.NoError(t, err)
			},
			wantCheck: func(resp map[string]interface{}, tt *testing.T) {
				if resp["stub"] != "ok" {
					tt.Error("expected stub=ok in response")
				}
			},
		},
		{
			name: "No stub, client.Do returns error",
			request: RequestEntity{
				Url:    "http://example.com",
				Method: http.MethodPost,
			},
			stubEnabled: false,
			clientErr:   errors.New("network error"),
			wantErr:     true,
			errContains: "network error",
		},
		{
			name: "No stub, client.Do returns nil response",
			request: RequestEntity{
				Url:    "http://example.com",
				Method: http.MethodPost,
			},
			stubEnabled: false,
			clientResp:  nil,
			wantErr:     true,
			errContains: "returned empty response",
		},
		{
			name: "No stub, non-2xx response",
			request: RequestEntity{
				Url:    "http://example.com",
				Method: http.MethodDelete,
			},
			stubEnabled: false,
			clientResp:  nonJsonResponse("Not Found", http.StatusNotFound),
			wantErr:     true,
			errContains: "Not Found",
		},
		{
			name: "No stub, invalid JSON response",
			request: RequestEntity{
				Url:    "http://example.com",
				Method: http.MethodPut,
			},
			stubEnabled: false,
			clientResp:  nonJsonResponse("not json", http.StatusOK),
			wantErr:     true,
			errContains: "invalid character",
		},
		{
			name: "Timeout > 0, simulate timeout",
			request: RequestEntity{
				Url:     "http://example.com",
				Method:  http.MethodGet,
				Timeout: 50 * time.Millisecond,
			},
			stubEnabled: false,
			// simulate that client.Do returns context.DeadlineExceeded when ctx times out
			clientErr:   context.DeadlineExceeded,
			wantErr:     true,
			errContains: "context deadline exceeded",
		},
		{
			name: "Timeout = 0 means no special timeout, success",
			request: RequestEntity{
				Url:    "http://example.com",
				Method: http.MethodGet,
				// no Timeout
			},
			stubEnabled: false,
			clientResp:  jsonResponse(map[string]interface{}{"noTimeout": true}, http.StatusOK),
			wantCheck: func(resp map[string]interface{}, tt *testing.T) {
				if resp["noTimeout"] != true {
					tt.Error("expected noTimeout=true in response")
				}
			},
		},
		{
			name: "Stub enabled, stub not found",
			request: RequestEntity{
				Url:    "http://example.com",
				Method: http.MethodGet,
				StubID: "",
			},
			stubEnabled: true,
			// no stub added with ID "no-such-stub"
			clientResp: jsonResponse(map[string]interface{}{"real": "call"}, http.StatusOK),
			wantCheck: func(resp map[string]interface{}, tt *testing.T) {
				if resp["real"] != "call" {
					tt.Error("expected real=call in response")
				}
			},
		},
		{
			name: "Stub enabled, stub returns non-2xx status",
			request: RequestEntity{
				Url:    "http://example.com",
				Method: http.MethodGet,
				StubID: "stub-non2xx",
			},
			stubEnabled: true,
			setupStubs: func(t *testing.T) {
				httpstub.ClearAllStubs()
				entry := &httpstub.StubEntry{
					ID:               "stub-non2xx",
					Endpoint:         "/ignored",
					ResponseCode:     http.StatusBadRequest,
					PositiveResponse: "{}",
					Probability:      1.0,
				}

				_, err := httpstub.AddStub(entry)
				assert.NoError(t, err)
			},
			wantErr:     true,
			errContains: "Bad Request",
		},
		{
			name: "Stub enabled, stub not found returns error",
			request: RequestEntity{
				Url:    "http://example.com",
				Method: http.MethodGet,
				StubID: "missing-stub",
			},
			stubEnabled: true,
			wantErr:     true,
			errContains: "stub with ID missing-stub not found",
		},
		{
			name: "Stub enabled, stub returns invalid JSON",
			request: RequestEntity{
				Url:    "http://example.com",
				Method: http.MethodGet,
				StubID: "stub-bad-json",
			},
			stubEnabled: true,
			setupStubs: func(t *testing.T) {
				httpstub.ClearAllStubs()
				entry := &httpstub.StubEntry{
					ID:               "stub-bad-json",
					Endpoint:         "/ignored",
					ResponseCode:     http.StatusOK,
					PositiveResponse: "not-json",
					Probability:      1.0,
				}
				_, err := httpstub.AddStub(entry)
				assert.NoError(t, err)
			},
			wantErr:     true,
			errContains: "invalid character",
		},
		{
			name: "No stub, response body read error",
			request: RequestEntity{
				Url:    "http://example.com",
				Method: http.MethodGet,
			},
			stubEnabled: false,
			clientResp: &http.Response{
				StatusCode: http.StatusOK,
				Status:     http.StatusText(http.StatusOK),
				Body:       errReadCloser{err: errors.New("read failed")},
				Header:     make(http.Header),
			},
			wantErr:     true,
			errContains: "read failed",
		},
		{
			name: "Invalid HTTP method",
			request: RequestEntity{
				Url:    "http://example.com",
				Method: "BAD METHOD",
			},
			stubEnabled: false,
			wantErr:     true,
			errContains: "invalid method",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			httpstub.InitStubConfig(tt.stubEnabled)
			httpstub.ClearAllStubs()

			if tt.setupStubs != nil {
				tt.setupStubs(t)
			}

			mockClient := &mockHttpClient{
				resp: tt.clientResp,
				err:  tt.clientErr,
			}

			resp, err := MakeHTTPRequest(context.Background(), mockClient, tt.request)
			if tt.wantErr && err == nil {
				t.Errorf("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			if tt.errContains != "" && err != nil {
				if !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("expected error to contain %q, got %v", tt.errContains, err)
				}
			}

			if !tt.wantErr && tt.wantCheck != nil {
				tt.wantCheck(resp, t)
			}
		})
	}
}

// Additional test for a basic success scenario without stubs or timeout
func TestMakeHTTPRequest_BasicSuccess(t *testing.T) {
	httpstub.InitStubConfig(false)
	httpstub.ClearAllStubs()

	mockClient := &mockHttpClient{
		resp: jsonResponse(map[string]interface{}{"hello": "world"}, http.StatusOK),
	}

	resp, err := MakeHTTPRequest(context.Background(), mockClient, RequestEntity{
		Url:    "http://example.com",
		Method: http.MethodGet,
	})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if resp["hello"] != "world" {
		t.Error("expected hello=world in response")
	}
}

type captureHeaderClient struct {
	headerKey string
	headerVal string
}

func (c *captureHeaderClient) Do(req *http.Request) (*http.Response, error) {
	if req.Header.Get(c.headerKey) != c.headerVal {
		return nil, fmt.Errorf("header %s not set", c.headerKey)
	}
	return jsonResponse(map[string]interface{}{"ok": true}, http.StatusOK), nil
}

type deadlineCaptureClient struct {
	hasDeadline bool
	deadline    time.Time
}

func (c *deadlineCaptureClient) Do(req *http.Request) (*http.Response, error) {
	c.deadline, c.hasDeadline = req.Context().Deadline()
	return jsonResponse(map[string]interface{}{"ok": true}, http.StatusOK), nil
}

func TestMakeHTTPRequest_HeadersSet(t *testing.T) {
	httpstub.InitStubConfig(false)
	httpstub.ClearAllStubs()

	client := &captureHeaderClient{
		headerKey: "X-Test",
		headerVal: "value",
	}

	resp, err := MakeHTTPRequest(context.Background(), client, RequestEntity{
		Url:    "http://example.com",
		Method: http.MethodGet,
		Headers: map[string]string{
			"X-Test": "value",
		},
	})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if resp["ok"] != true {
		t.Error("expected ok=true in response")
	}
}

func TestMakeHTTPRequest_DefaultDeadline(t *testing.T) {
	SetDefaultHTTPRequestTimeout(25 * time.Millisecond)
	t.Cleanup(func() { SetDefaultHTTPRequestTimeout(DefaultHTTPRequestTimeout) })

	httpstub.InitStubConfig(false)
	httpstub.ClearAllStubs()

	client := &deadlineCaptureClient{}
	_, err := MakeHTTPRequest(context.Background(), client, RequestEntity{
		Url:    "http://example.com",
		Method: http.MethodGet,
	})

	assert.NoError(t, err)
	assert.True(t, client.hasDeadline)
	assert.LessOrEqual(t, time.Until(client.deadline), 25*time.Millisecond)
	assert.Equal(t, 25*time.Millisecond, GetDefaultHTTPRequestTimeout())
}

func TestSetDefaultHTTPRequestTimeout_InvalidResetsDefault(t *testing.T) {
	SetDefaultHTTPRequestTimeout(-time.Second)
	t.Cleanup(func() { SetDefaultHTTPRequestTimeout(DefaultHTTPRequestTimeout) })

	assert.Equal(t, DefaultHTTPRequestTimeout, GetDefaultHTTPRequestTimeout())
}

func TestHTTPRequestContext_PreservesExistingDeadline(t *testing.T) {
	parent, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	parentDeadline, ok := parent.Deadline()
	assert.True(t, ok)

	ctx, cancelRequest := httpRequestContext(parent, time.Millisecond)
	defer cancelRequest()
	gotDeadline, ok := ctx.Deadline()

	assert.True(t, ok)
	assert.Equal(t, parentDeadline, gotDeadline)
}

func TestMakeHTTPRequest_StubReadError(t *testing.T) {
	httpstub.InitStubConfig(true)
	httpstub.ClearAllStubs()

	entry := &httpstub.StubEntry{
		ID:               "stub-read-error",
		Endpoint:         "/ignored",
		ResponseCode:     http.StatusOK,
		PositiveResponse: `{"ok":true}`,
		Probability:      1.0,
	}
	_, addErr := httpstub.AddStub(entry)
	assert.NoError(t, addErr)

	oldReadAll := readAllFn
	readAllFn = func(r io.Reader) ([]byte, error) {
		return nil, errors.New("stub read error")
	}
	defer func() {
		readAllFn = oldReadAll
	}()

	_, err := MakeHTTPRequest(context.Background(), &mockHttpClient{}, RequestEntity{
		Url:    "http://example.com",
		Method: http.MethodGet,
		StubID: "stub-read-error",
	})
	if err == nil || !strings.Contains(err.Error(), "stub read error") {
		t.Fatalf("expected stub read error, got %v", err)
	}
}

func TestMakeHTTPRequest_StubCloseError(t *testing.T) {
	httpstub.InitStubConfig(true)
	httpstub.ClearAllStubs()

	oldCallStub := callStubFn
	callStubFn = func(context.Context, string) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Status:     http.StatusText(http.StatusOK),
			Body:       closeErrReadCloser{Reader: strings.NewReader(`{"ok":true}`)},
			Header:     make(http.Header),
		}, nil
	}
	defer func() { callStubFn = oldCallStub }()

	_, err := MakeHTTPRequest(context.Background(), &mockHttpClient{}, RequestEntity{
		Url:    "http://example.com",
		Method: http.MethodGet,
		StubID: "stub-close-error",
	})
	if err == nil || !strings.Contains(err.Error(), "close failed") {
		t.Fatalf("expected stub close error, got %v", err)
	}
}

func TestMakeHTTPRequest_ResponseCloseError(t *testing.T) {
	httpstub.InitStubConfig(false)
	httpstub.ClearAllStubs()

	_, err := MakeHTTPRequest(context.Background(), &mockHttpClient{
		resp: &http.Response{
			StatusCode: http.StatusOK,
			Status:     http.StatusText(http.StatusOK),
			Body:       closeErrReadCloser{Reader: strings.NewReader(`{"ok":true}`)},
			Header:     make(http.Header),
		},
	}, RequestEntity{
		Url:    "http://example.com",
		Method: http.MethodGet,
	})
	if err == nil || !strings.Contains(err.Error(), "close failed") {
		t.Fatalf("expected response close error, got %v", err)
	}
}

func TestIsHttpRequest(t *testing.T) {
	req := new(http.Request)

	req.Method = http.MethodGet
	assert.True(t, IsHttpRequest(req))

	req.Method = "Dummy"
	assert.False(t, IsHttpRequest(req))
}

func TestValidateAndFetchGetParams(t *testing.T) {
	keys := map[string]reflect.Kind{
		"age": reflect.Int,
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		params, err := ValidateAndFetchGetParams(r, keys)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if _, err := fmt.Fprintf(w, "Parsed Params: %+v", params); err != nil {
			t.Fatalf("failed to write response: %v", err)
		}
	}))
	defer server.Close()

	// Test with valid int parameter
	resp, err := http.Get(fmt.Sprintf("%s?age=25", server.URL))
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Test with invalid int parameter
	resp, err = http.Get(fmt.Sprintf("%s?age=abc", server.URL))
	assert.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

	// Test with missing parameter
	resp, err = http.Get(server.URL)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestValidateAndFetchGetParams_String(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/?name=alice", nil)
	params, err := ValidateAndFetchGetParams(req, map[string]reflect.Kind{
		"name": reflect.String,
	})
	assert.NoError(t, err)
	assert.Equal(t, "alice", params["name"])
}

func TestValidateAndFetchPostParams(t *testing.T) {
	keys := map[string]reflect.Kind{
		"age": reflect.Int,
	}

	form := url.Values{}
	form.Add("age", "30")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		params, err := ValidateAndFetchPostParams(r, keys)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if _, err := fmt.Fprintf(w, "Parsed Params: %+v", params); err != nil {
			t.Fatalf("failed to write response: %v", err)
		}
	}))
	defer server.Close()

	// Test with valid data
	resp, err := http.PostForm(server.URL, form)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Test with invalid data
	form.Set("age", "abc")
	resp, err = http.PostForm(server.URL, form)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

	// Test with missing data
	form.Del("age")
	resp, err = http.PostForm(server.URL, form)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestValidateAndFetchPostParams_String(t *testing.T) {
	form := url.Values{}
	form.Add("name", "alice")

	req, err := http.NewRequest(http.MethodPost, "/", strings.NewReader(form.Encode()))
	assert.NoError(t, err)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	params, err := ValidateAndFetchPostParams(req, map[string]reflect.Kind{
		"name": reflect.String,
	})
	assert.NoError(t, err)
	assert.Equal(t, "alice", params["name"])
}

func TestReadJsonPayload(t *testing.T) {
	validJson := `{"name":"John"}`
	invalidJson := `{"name":}`

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, err := ReadJsonPayload(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if _, err := fmt.Fprintf(w, "Parsed Data: %+v", data); err != nil {
			t.Fatalf("failed to write response: %v", err)
		}
	}))
	defer server.Close()

	// Test with valid JSON
	resp, err := http.Post(server.URL, ContentTypeJSON, bytes.NewBufferString(validJson))
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Test with invalid JSON
	resp, err = http.Post(server.URL, ContentTypeJSON, bytes.NewBufferString(invalidJson))
	assert.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestReadJsonPayload_ReadError(t *testing.T) {
	req, err := http.NewRequest(http.MethodPost, "/", errReadCloser{err: errors.New("read error")})
	assert.NoError(t, err)

	_, err = ReadJsonPayload(req)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "error reading request body")
}

func TestReadJsonPayload_CloseError(t *testing.T) {
	req, err := http.NewRequest(http.MethodPost, "/", closeErrReadCloser{Reader: strings.NewReader(`{"ok":true}`)})
	assert.NoError(t, err)

	_, err = ReadJsonPayload(req)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "close failed")
}

func TestValidateAndFetchPostParams_ParseFormError(t *testing.T) {
	req := &http.Request{
		Method: http.MethodPost,
		Header: http.Header{"Content-Type": []string{"application/x-www-form-urlencoded"}},
		Body:   errReadCloser{err: errors.New("parse error")},
	}
	_, err := ValidateAndFetchPostParams(req, map[string]reflect.Kind{"age": reflect.Int})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse form data")
}

func TestDownloadBytes_All(t *testing.T) {
	t.Run("SniffedContentType_and_Body", func(t *testing.T) {
		data := []byte("hello, world\n")
		req := httptest.NewRequest(http.MethodGet, "/dl", nil)
		rr := httptest.NewRecorder()

		DownloadBytes(rr, req, "greeting.txt", "", data)

		res := rr.Result()
		defer func() {
			if err := res.Body.Close(); err != nil {
				t.Fatalf("failed to close response body: %v", err)
			}
		}()

		// Status & body
		if res.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want %d", res.StatusCode, http.StatusOK)
		}
		body := rr.Body.Bytes()
		if !bytes.Equal(body, data) {
			t.Fatalf("body mismatch: got %q want %q", string(body), string(data))
		}

		// Content-Type sniffed by http.DetectContentType
		gotCT := res.Header.Get(HeaderContentType)
		if !strings.HasPrefix(gotCT, "text/plain") {
			t.Fatalf("Content-Type = %q, want text/plain; charset=utf-8", gotCT)
		}

		// Security header
		if res.Header.Get(HeaderXContentTypeOptions) != "nosniff" {
			t.Fatalf("nosniff header missing or wrong")
		}

		// CORS expose header
		if res.Header.Get(HeaderAccessControlExposeHeaders) != "Content-Disposition" {
			t.Fatalf("Access-Control-Expose-Headers mismatch: %q", res.Header.Get("Access-Control-Expose-Headers"))
		}

		// Cache header
		if res.Header.Get(HeaderCacheControl) != "private, max-age=0, must-revalidate" {
			t.Fatalf("Cache-Control mismatch: %q", res.Header.Get("Cache-Control"))
		}

		// Content-Disposition: attachment; filename="..."; filename*=UTF-8''...
		cd := res.Header.Get(HeaderContentDisposition)
		if !strings.HasPrefix(cd, `attachment;`) {
			t.Fatalf("Content-Disposition missing attachment: %q", cd)
		}
		if !strings.Contains(cd, `filename="greeting.txt"`) {
			t.Fatalf("Content-Disposition missing filename: %q", cd)
		}
		wantEncoded := url.PathEscape("greeting.txt")
		if !strings.Contains(cd, `filename*=UTF-8''`+wantEncoded) {
			t.Fatalf("Content-Disposition missing filename* encoded: %q", cd)
		}
	})

	t.Run("EmptyData_DefaultOctetStream", func(t *testing.T) {
		data := []byte{}
		req := httptest.NewRequest(http.MethodGet, "/dl", nil)
		rr := httptest.NewRecorder()

		DownloadBytes(rr, req, "empty.bin", "", data)

		res := rr.Result()
		defer func() {
			if err := res.Body.Close(); err != nil {
				t.Fatalf("failed to close response body: %v", err)
			}
		}()

		if res.Header.Get(HeaderContentType) != "application/octet-stream" {
			t.Fatalf("Content-Type = %q, want application/octet-stream", res.Header.Get("Content-Type"))
		}
	})

	t.Run("ProvidedContentType_TakesPrecedence", func(t *testing.T) {
		data := []byte(`{"ok":true}`)
		req := httptest.NewRequest(http.MethodGet, "/dl", nil)
		rr := httptest.NewRecorder()

		DownloadBytes(rr, req, "data.json", "application/json", data)

		res := rr.Result()
		defer func() {
			if err := res.Body.Close(); err != nil {
				t.Fatalf("failed to close response body: %v", err)
			}
		}()

		if res.Header.Get(HeaderContentType) != "application/json" {
			t.Fatalf("Content-Type = %q, want application/json", res.Header.Get("Content-Type"))
		}
	})

	t.Run("UTF8_and_UnsafeFilename_Sanitised_and_Encoded", func(t *testing.T) {
		raw := "招待状 \"weird\\name\"\r\n2025.pdf"
		safe, _ := filesys.SanitizeFilename(raw, "download")

		if safe != "招待状 _weird_name___2025.pdf" {
			t.Fatalf("SanitiseFilename(safe) = %q, want %q", safe, "招待状 _weird_name___2025.pdf")
		}

		data := []byte("x")
		req := httptest.NewRequest(http.MethodGet, "/dl", nil)
		rr := httptest.NewRecorder()

		DownloadBytes(rr, req, raw, "", data)

		res := rr.Result()
		defer func() {
			if err := res.Body.Close(); err != nil {
				t.Fatalf("failed to close response body: %v", err)
			}
		}()

		cd := res.Header.Get(HeaderContentDisposition)
		if !strings.Contains(cd, `filename="`+safe+`"`) {
			t.Fatalf("Content-Disposition filename missing or wrong: %q", cd)
		}

		if !strings.Contains(cd, `filename*=UTF-8''`+url.PathEscape(safe)) {
			t.Fatalf("Content-Disposition filename* missing or wrong: %q", cd)
		}
	})

	t.Run("RangeRequest_206_and_PartialBody", func(t *testing.T) {
		data := []byte("abcdef")
		req := httptest.NewRequest(http.MethodGet, "/dl", nil)
		req.Header.Set("Range", "bytes=2-4") // expect "cde"
		rr := httptest.NewRecorder()

		DownloadBytes(rr, req, "abc.txt", "", data)

		res := rr.Result()
		defer func() {
			if err := res.Body.Close(); err != nil {
				t.Fatalf("failed to close response body: %v", err)
			}
		}()

		if res.StatusCode != http.StatusPartialContent {
			t.Fatalf("status = %d, want %d (206)", res.StatusCode, http.StatusPartialContent)
		}

		cr := res.Header.Get("Content-Range")
		if !strings.HasPrefix(cr, "bytes 2-4/6") {
			t.Fatalf("Content-Range = %q, want prefix %q", cr, "bytes 2-4/6")
		}

		body := rr.Body.String()
		if body != "cde" {
			t.Fatalf("partial body = %q, want %q", body, "cde")
		}

		if ar := res.Header.Get("Accept-Ranges"); ar != "bytes" {
			t.Fatalf("Accept-Ranges = %q, want %q", ar, "bytes")
		}
	})
}
