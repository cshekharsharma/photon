package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestSecurityHeadersOptions(t *testing.T) {
	handled := false
	nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handled = true
		w.WriteHeader(http.StatusOK)
	})

	req := httptest.NewRequest("GET", "http://example.com/foo", nil)
	rec := httptest.NewRecorder()

	handlerToTest := SecurityHeaders(SecurityHeadersOptions{})(nextHandler)

	handlerToTest.ServeHTTP(rec, req)

	tests := []struct {
		header string
		want   string
	}{
		{"X-Frame-Options", "DENY"},
		{"X-XSS-Protection", "0"},
		{"X-Content-Type-Options", "nosniff"},
		{"Referrer-Policy", "no-referrer"},
		{"Permissions-Policy", "camera=(), microphone=(), geolocation=()"},
		{"Cross-Origin-Opener-Policy", "same-origin"},
	}

	for _, tt := range tests {
		t.Run(tt.header, func(t *testing.T) {
			if got := rec.Header().Get(tt.header); got != tt.want {
				t.Errorf("Header %s = %v, want %v", tt.header, got, tt.want)
			}
		})
	}

	if status := rec.Code; status != http.StatusOK {
		t.Errorf("Handler returned wrong status code: got %v want %v",
			status, http.StatusOK)
	}
	if !handled {
		t.Fatal("next handler was not called")
	}
	if got := rec.Header().Get("Content-Security-Policy"); got != "" {
		t.Fatalf("Content-Security-Policy = %q, want empty by default", got)
	}
}

func TestSecurityHeaders(t *testing.T) {
	nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte("ok"))
	})

	req := httptest.NewRequest("GET", "http://example.com/foo", nil)
	rec := httptest.NewRecorder()

	handlerToTest := SecurityHeaders(SecurityHeadersOptions{
		XFrameOptions:           "SAMEORIGIN",
		XContentTypeOptions:     "-",
		ReferrerPolicy:          "strict-origin",
		PermissionsPolicy:       "fullscreen=(self)",
		CrossOriginOpenerPolicy: "unsafe-none",
		XSSProtection:           "-",
		ContentSecurityPolicy:   "default-src 'self'",
	})(nextHandler)

	handlerToTest.ServeHTTP(rec, req)

	assertions := map[string]string{
		"X-Frame-Options":            "SAMEORIGIN",
		"Referrer-Policy":            "strict-origin",
		"Permissions-Policy":         "fullscreen=(self)",
		"Cross-Origin-Opener-Policy": "unsafe-none",
		"Content-Security-Policy":    "default-src 'self'",
	}

	for header, want := range assertions {
		t.Run(header, func(t *testing.T) {
			if got := rec.Header().Get(header); got != want {
				t.Fatalf("%s = %q, want %q", header, got, want)
			}
		})
	}

	if got := rec.Header().Get("X-Content-Type-Options"); got != "" {
		t.Fatalf("X-Content-Type-Options = %q, want empty when disabled", got)
	}
	if got := rec.Header().Get("X-XSS-Protection"); got != "" {
		t.Fatalf("X-XSS-Protection = %q, want empty when disabled", got)
	}
	if status := rec.Code; status != http.StatusCreated {
		t.Fatalf("status = %d, want %d", status, http.StatusCreated)
	}
	if body := rec.Body.String(); body != "ok" {
		t.Fatalf("body = %q, want ok", body)
	}
}
