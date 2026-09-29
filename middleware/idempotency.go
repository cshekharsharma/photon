package middleware

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/cshekharsharma/photon/coordination/idempotency"
	"github.com/cshekharsharma/photon/utils/rest/apiresponse"
)

const (
	defaultIdempotencyHeader       = "Idempotency-Key"
	defaultIdempotencyTTL          = 24 * time.Hour
	defaultIdempotencyMaxBytes     = int64(1 << 20)
	defaultIdempotencyErrorMessage = ""
)

// IdempotencyOptions configures idempotency protection for unsafe HTTP methods.
type IdempotencyOptions struct {
	Store            idempotency.Store
	HeaderName       string
	TTL              time.Duration
	Methods          map[string]bool
	ScopeFunc        func(*http.Request) string
	FingerprintFunc  func(*http.Request, []byte) (string, error)
	MaxRequestBytes  int64
	MaxResponseBytes int64
	StoreStatus      func(status int) bool
}

// Idempotency returns middleware that reserves, completes, and replays idempotent HTTP calls.
func Idempotency(opts IdempotencyOptions) func(http.Handler) http.Handler {
	cfg := normalizeIdempotencyOptions(opts)
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !cfg.Methods[r.Method] {
				next.ServeHTTP(w, r)
				return
			}
			if cfg.Store == nil {
				sendIdempotencyError(w, apiresponse.ServiceUnavailable, http.StatusServiceUnavailable)
				return
			}

			key := strings.TrimSpace(r.Header.Get(cfg.HeaderName))
			if key == "" {
				sendIdempotencyError(w, apiresponse.BadRequest, http.StatusBadRequest)
				return
			}

			body, ok := readAndRestoreBody(r, cfg.MaxRequestBytes)
			if !ok {
				sendIdempotencyError(w, apiresponse.BadRequest, http.StatusBadRequest)
				return
			}

			scope := strings.TrimSpace(cfg.ScopeFunc(r))
			if scope == "" {
				sendIdempotencyError(w, apiresponse.BadRequest, http.StatusBadRequest)
				return
			}
			fingerprint, err := idempotencyFingerprint(cfg, r, scope, body)
			if err != nil || strings.TrimSpace(fingerprint) == "" {
				sendIdempotencyError(w, apiresponse.BadRequest, http.StatusBadRequest)
				return
			}

			req := idempotency.Request{
				Key:         key,
				Scope:       scope,
				Fingerprint: fingerprint,
				TTL:         cfg.TTL,
				OwnerToken:  idempotency.NewOwnerToken(),
			}
			decision, err := cfg.Store.Reserve(r.Context(), req)
			if err != nil {
				sendIdempotencyError(w, apiresponse.ServiceUnavailable, http.StatusServiceUnavailable)
				return
			}

			switch decision.Status {
			case idempotency.StatusReserved:
				serveReserved(w, r, next, cfg, req)
			case idempotency.StatusCompleted:
				replayStoredResponse(w, decision.Response)
			case idempotency.StatusInFlight:
				sendIdempotencyError(w, apiresponse.Conflict, http.StatusConflict)
			case idempotency.StatusMismatch:
				sendIdempotencyError(w, apiresponse.UnprocessableEntity, http.StatusUnprocessableEntity)
			default:
				sendIdempotencyError(w, apiresponse.ServiceUnavailable, http.StatusServiceUnavailable)
			}
		})
	}
}

func normalizeIdempotencyOptions(opts IdempotencyOptions) IdempotencyOptions {
	if opts.HeaderName == "" {
		opts.HeaderName = defaultIdempotencyHeader
	}
	if opts.TTL <= 0 {
		opts.TTL = defaultIdempotencyTTL
	}
	if opts.Methods == nil {
		opts.Methods = map[string]bool{
			http.MethodPost:  true,
			http.MethodPut:   true,
			http.MethodPatch: true,
		}
	}
	if opts.ScopeFunc == nil {
		opts.ScopeFunc = defaultIdempotencyScope
	}
	if opts.MaxRequestBytes <= 0 {
		opts.MaxRequestBytes = defaultIdempotencyMaxBytes
	}
	if opts.MaxResponseBytes <= 0 {
		opts.MaxResponseBytes = defaultIdempotencyMaxBytes
	}
	if opts.StoreStatus == nil {
		opts.StoreStatus = func(status int) bool {
			return status >= http.StatusOK && status < http.StatusBadRequest
		}
	}
	return opts
}

func idempotencyFingerprint(cfg IdempotencyOptions, r *http.Request, scope string, body []byte) (string, error) {
	if cfg.FingerprintFunc != nil {
		return cfg.FingerprintFunc(r, body)
	}
	return fingerprintWithScope(r, scope, body), nil
}

func readAndRestoreBody(r *http.Request, limit int64) ([]byte, bool) {
	if r.Body == nil {
		return nil, true
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, limit+1))
	_ = r.Body.Close()
	if err != nil || int64(len(body)) > limit {
		r.Body = io.NopCloser(bytes.NewReader(nil))
		return nil, false
	}
	r.Body = io.NopCloser(bytes.NewReader(body))
	return body, true
}

func defaultIdempotencyScope(r *http.Request) string {
	return r.Method + " " + requestRoutePath(r)
}

func fingerprintWithScope(r *http.Request, scope string, body []byte) string {
	bodyHash := sha256.Sum256(body)
	raw := fmt.Sprintf("%s\n%s\n%s\n%s", r.Method, requestRoutePath(r), scope, hex.EncodeToString(bodyHash[:]))
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}

func requestRoutePath(r *http.Request) string {
	if r.URL == nil || r.URL.EscapedPath() == "" {
		return "/"
	}
	return r.URL.EscapedPath()
}

func serveReserved(
	w http.ResponseWriter,
	r *http.Request,
	next http.Handler,
	cfg IdempotencyOptions,
	req idempotency.Request,
) {
	recorder := newBoundedResponseWriter(w, cfg.MaxResponseBytes)
	defer func() {
		if recovered := recover(); recovered != nil {
			_ = cfg.Store.Release(context.WithoutCancel(r.Context()), idempotency.ReleaseRequest{
				Key:         req.Key,
				Scope:       req.Scope,
				Fingerprint: req.Fingerprint,
				OwnerToken:  req.OwnerToken,
			})
			panic(recovered)
		}
	}()

	next.ServeHTTP(recorder, r)
	status := recorder.statusCode()
	if !cfg.StoreStatus(status) {
		_ = cfg.Store.Release(context.WithoutCancel(r.Context()), idempotency.ReleaseRequest{
			Key:         req.Key,
			Scope:       req.Scope,
			Fingerprint: req.Fingerprint,
			OwnerToken:  req.OwnerToken,
		})
		_ = recorder.send()
		return
	}

	completeErr := cfg.Store.Complete(context.WithoutCancel(r.Context()), idempotency.CompleteRequest{
		Key:         req.Key,
		Scope:       req.Scope,
		Fingerprint: req.Fingerprint,
		TTL:         req.TTL,
		OwnerToken:  req.OwnerToken,
		Response: idempotency.StoredResponse{
			StatusCode: status,
			Header:     cloneHeader(recorder.Header()),
			Body:       recorder.body(),
			Replayable: recorder.replayable(),
		},
	})
	if completeErr != nil {
		if !recorder.sent() {
			sendIdempotencyError(w, apiresponse.ServiceUnavailable, http.StatusServiceUnavailable)
		}
		return
	}
	_ = recorder.send()
}

func replayStoredResponse(w http.ResponseWriter, response *idempotency.StoredResponse) {
	if response == nil || !response.Replayable {
		sendIdempotencyError(w, apiresponse.Conflict, http.StatusConflict)
		return
	}
	for key, values := range response.Header {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}
	status := response.StatusCode
	if status == 0 {
		status = http.StatusOK
	}
	w.WriteHeader(status)
	_, _ = w.Write(response.Body)
}

func sendIdempotencyError(w http.ResponseWriter, code apiresponse.InternalResponseCode, status int) {
	apiresponse.New(false, code, nil, defaultIdempotencyErrorMessage).Send(w, status)
}

func cloneHeader(header http.Header) map[string][]string {
	cloned := make(map[string][]string, len(header))
	for key, values := range header {
		cloned[key] = append([]string(nil), values...)
	}
	return cloned
}
