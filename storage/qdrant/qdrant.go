package qdrant

import (
	"bytes"
	"context"
	cryptorand "crypto/rand"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

var cryptoRandRead = cryptorand.Read

type restClient struct {
	baseURL    string
	apiKey     string
	httpClient *http.Client

	retryMaxAttempts int
	retryBaseDelay   time.Duration
	retryMaxDelay    time.Duration
	retryJitter      float64
}

func NewRESTClient(cfg *ConnectionConfig) (*restClient, error) {
	if cfg == nil {
		return nil, errors.New("qdrant: nil config")
	}
	base := strings.TrimSpace(cfg.BaseURL)
	if base == "" {
		return nil, errors.New("qdrant: BaseURL is required")
	}
	baseURL, err := validateBaseURL(base)
	if err != nil {
		return nil, err
	}

	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	dial := cfg.DialTimeout
	if dial <= 0 {
		dial = 5 * time.Second
	}
	idle := cfg.IdleConnTimeout
	if idle <= 0 {
		idle = 90 * time.Second
	}

	maxAttempts := cfg.RetryMaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 3
	}
	baseDelay := cfg.RetryBaseDelay
	if baseDelay <= 0 {
		baseDelay = 150 * time.Millisecond
	}
	maxDelay := cfg.RetryMaxDelay
	if maxDelay <= 0 {
		maxDelay = 2 * time.Second
	}
	jitter := cfg.RetryJitter
	if jitter < 0 || jitter > 1 {
		jitter = 0.2
	}

	tr := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   dial,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          200,
		MaxIdleConnsPerHost:   50,
		IdleConnTimeout:       idle,
		TLSHandshakeTimeout:   5 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		TLSClientConfig:       &tls.Config{MinVersion: tls.VersionTLS12},
	}

	return &restClient{
		baseURL: baseURL,
		apiKey:  cfg.APIKey,
		httpClient: &http.Client{
			Transport: tr,
			Timeout:   timeout,
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				if len(via) == 0 || sameURLAuthority(req.URL, via[0].URL) {
					return nil
				}
				return http.ErrUseLastResponse
			},
		},
		retryMaxAttempts: maxAttempts,
		retryBaseDelay:   baseDelay,
		retryMaxDelay:    maxDelay,
		retryJitter:      jitter,
	}, nil
}

func (c *restClient) Health(ctx context.Context) error {
	// Qdrant commonly exposes /healthz
	var out any
	return c.doJSON(ctx, http.MethodGet, "/healthz", nil, &out)
}

func (c *restClient) UpsertPoints(ctx context.Context, req UpsertPointsRequest) error {
	if req.Collection == "" || len(req.Points) == 0 {
		return errors.New("qdrant: UpsertPoints invalid request")
	}
	body := map[string]any{"points": req.Points}
	path := "/collections/" + url.PathEscape(req.Collection) + "/points"
	if req.Wait {
		path += "?wait=true"
	}
	var out any
	return c.doJSON(ctx, http.MethodPut, path, body, &out)
}

func (c *restClient) DeletePoints(ctx context.Context, req DeletePointsRequest) error {
	if req.Collection == "" || len(req.IDs) == 0 {
		return errors.New("qdrant: DeletePoints invalid request")
	}
	body := map[string]any{"points": map[string]any{"ids": req.IDs}}
	path := "/collections/" + url.PathEscape(req.Collection) + "/points/delete"
	if req.Wait {
		path += "?wait=true"
	}
	var out any
	return c.doJSON(ctx, http.MethodPost, path, body, &out)
}

func (c *restClient) Search(ctx context.Context, req SearchRequest) ([]ScoredPoint, error) {
	if req.Collection == "" || len(req.Vector) == 0 || req.Limit <= 0 {
		return nil, errors.New("qdrant: Search invalid request")
	}

	body := map[string]any{
		"vector":       req.Vector,
		"limit":        req.Limit,
		"offset":       req.Offset,
		"with_payload": req.WithPayload,
		"with_vector":  req.WithVector,
	}
	if req.ScoreThreshold != nil {
		body["score_threshold"] = *req.ScoreThreshold
	}
	if len(req.Filter) > 0 {
		var f any
		if err := json.Unmarshal(req.Filter, &f); err != nil {
			return nil, errors.New("qdrant: invalid filter JSON")
		}
		body["filter"] = f
	}

	var resp struct {
		Result []ScoredPoint `json:"result"`
		Status string        `json:"status"`
	}
	path := "/collections/" + url.PathEscape(req.Collection) + "/points/search"
	if err := c.doJSON(ctx, http.MethodPost, path, body, &resp); err != nil {
		return nil, err
	}
	return resp.Result, nil
}

func (c *restClient) CreateCollection(ctx context.Context, req CreateCollectionRequest) error {
	if strings.TrimSpace(req.Collection) == "" {
		return errors.New("qdrant: CreateCollection invalid collection")
	}
	body, err := req.Config.ToJSON()
	if err != nil {
		return err
	}

	var out any
	path := "/collections/" + url.PathEscape(req.Collection)
	return c.doJSON(ctx, http.MethodPut, path, body, &out)
}

func (c *restClient) DeleteCollection(ctx context.Context, collection string) error {
	if strings.TrimSpace(collection) == "" {
		return errors.New("qdrant: DeleteCollection invalid collection")
	}
	var out any
	path := "/collections/" + url.PathEscape(collection)
	return c.doJSON(ctx, http.MethodDelete, path, nil, &out)
}

func (c *restClient) GetCollectionInfo(ctx context.Context, collection string) (CollectionInfo, error) {
	if strings.TrimSpace(collection) == "" {
		return CollectionInfo{}, errors.New("qdrant: GetCollectionInfo invalid collection")
	}
	var resp struct {
		Result struct {
			Status string `json:"status"`
		} `json:"result"`
		Status string `json:"status"`
	}
	path := "/collections/" + url.PathEscape(collection)
	if err := c.doJSON(ctx, http.MethodGet, path, nil, &resp); err != nil {
		return CollectionInfo{}, err
	}
	return CollectionInfo{Status: resp.Result.Status}, nil
}

// ---------------- internal: hardened request executor ----------------

func (c *restClient) doJSON(ctx context.Context, method, path string, in any, out any) error {
	payload, err := jsonPayload(in)
	if err != nil {
		return err
	}

	var lastErr error
	for attempt := 1; attempt <= c.retryMaxAttempts; attempt++ {
		req, err := c.newRequest(ctx, method, path, payload, in != nil)
		if err != nil {
			return err
		}

		statusCode, body, err := c.doRequest(req)
		if err != nil && statusCode != 0 {
			retry, retryErr := c.retryAfter(ctx, attempt, err)
			lastErr = retryErr
			if !retry {
				break
			}
			continue
		}

		retry, err := c.handleRequestError(ctx, attempt, err)
		if err != nil {
			lastErr = err
			if !retry {
				break
			}
			continue
		}

		retry, err = c.handleResponse(ctx, attempt, statusCode, body)
		if err != nil {
			lastErr = err
			if !retry {
				break
			}
			continue
		}

		if err := decodeJSONBody(body, out); err != nil {
			return err
		}
		return nil
	}

	if lastErr != nil {
		return lastErr
	}
	return errors.New("qdrant: request failed")
}

func jsonPayload(in any) ([]byte, error) {
	if in == nil {
		return nil, nil
	}
	return json.Marshal(in)
}

func validateBaseURL(raw string) (string, error) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || !u.IsAbs() || u.Host == "" {
		return "", errors.New("qdrant: invalid BaseURL")
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return "", errors.New("qdrant: BaseURL must use http or https")
	}
	if u.User != nil {
		return "", errors.New("qdrant: BaseURL must not include credentials")
	}
	if u.RawQuery != "" || u.Fragment != "" {
		return "", errors.New("qdrant: BaseURL must not include query or fragment")
	}
	u.Path = strings.TrimRight(u.Path, "/")
	u.RawPath = ""
	return u.String(), nil
}

func (c *restClient) newRequest(ctx context.Context, method, path string, payload []byte, hasPayload bool) (*http.Request, error) {
	target, err := c.requestURL(path)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, method, target, bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	if hasPayload {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.apiKey != "" {
		req.Header.Set("api-key", c.apiKey)
	}
	return req, nil
}

func (c *restClient) requestURL(path string) (string, error) {
	if !strings.HasPrefix(path, "/") {
		return "", errors.New("qdrant: request path must be absolute")
	}
	base, err := url.Parse(c.baseURL)
	if err != nil {
		return "", errors.New("qdrant: invalid BaseURL")
	}
	pathPart, query, _ := strings.Cut(path, "?")
	u := *base
	u.Path = strings.TrimRight(base.Path, "/") + pathPart
	u.RawPath = ""
	u.RawQuery = query
	u.Fragment = ""
	return u.String(), nil
}

func sameURLAuthority(a, b *url.URL) bool {
	return a != nil && b != nil && a.Scheme == b.Scheme && strings.EqualFold(a.Host, b.Host)
}

func (c *restClient) doRequest(req *http.Request) (int, []byte, error) {
	resp, err := c.httpClient.Do(req) // #nosec G704 -- BaseURL is structurally validated and redirects are confined to the same authority.
	if err != nil {
		return 0, nil, err
	}

	body, readErr := io.ReadAll(io.LimitReader(resp.Body, 2<<20)) // 2MB cap
	closeErr := resp.Body.Close()
	if readErr != nil {
		return resp.StatusCode, nil, readErr
	}
	if closeErr != nil {
		return resp.StatusCode, nil, closeErr
	}
	return resp.StatusCode, body, nil
}

func (c *restClient) handleRequestError(ctx context.Context, attempt int, requestErr error) (bool, error) {
	if requestErr == nil {
		return false, nil
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return false, ctxErr
	}
	if errors.Is(requestErr, context.Canceled) {
		return false, requestErr
	}
	if !isRetryableNetErr(requestErr) || attempt == c.retryMaxAttempts {
		return false, requestErr
	}
	if err := sleepCtx(ctx, c.backoff(attempt)); err != nil {
		return false, err
	}
	return true, requestErr
}

func (c *restClient) handleResponse(ctx context.Context, attempt int, statusCode int, body []byte) (bool, error) {
	if err := qdrantStatusError(statusCode, body); err != nil {
		if isRetryableStatus(statusCode) {
			retry, retryErr := c.retryAfter(ctx, attempt, err)
			return retry, retryErr
		}
		return false, err
	}

	return false, nil
}

func qdrantStatusError(statusCode int, body []byte) error {
	if statusCode >= http.StatusOK && statusCode < http.StatusMultipleChoices {
		return nil
	}

	httpErr := &HTTPError{StatusCode: statusCode, Body: string(body)}
	if mapped := mapHTTPStatus(statusCode); mapped != nil {
		return errors.Join(mapped, httpErr)
	}
	return httpErr
}

func isRetryableStatus(statusCode int) bool {
	return statusCode == http.StatusTooManyRequests || statusCode >= http.StatusInternalServerError
}

func (c *restClient) retryAfter(ctx context.Context, attempt int, originalErr error) (bool, error) {
	if attempt == c.retryMaxAttempts {
		return false, originalErr
	}
	if err := sleepCtx(ctx, c.backoff(attempt)); err != nil {
		return false, err
	}
	return true, originalErr
}

func decodeJSONBody(body []byte, out any) error {
	if out == nil || len(bytes.TrimSpace(body)) == 0 {
		return nil
	}
	return json.Unmarshal(body, out)
}

func (c *restClient) backoff(attempt int) time.Duration {
	// attempt: 1 => after first failure
	d := c.retryBaseDelay * (1 << (attempt - 1))
	if d > c.retryMaxDelay {
		d = c.retryMaxDelay
	}
	if c.retryJitter > 0 {
		j := (cryptoFloat64()*2 - 1) * c.retryJitter
		d = time.Duration(float64(d) * (1 + j))
		if d < 0 {
			d = 0
		}
	}
	return d
}

func cryptoFloat64() float64 {
	var buf [8]byte
	if _, err := cryptoRandRead(buf[:]); err != nil {
		return 1
	}
	return float64(binary.BigEndian.Uint64(buf[:])) / float64(^uint64(0))
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func isRetryableNetErr(err error) bool {
	var ne net.Error
	if errors.Is(err, context.Canceled) {
		return false
	}

	if errors.As(err, &ne) {
		return ne.Timeout() || isTemporaryNetErr(ne)
	}

	s := strings.ToLower(err.Error())
	return strings.Contains(strings.ToLower(s), "connection reset") ||
		strings.Contains(strings.ToLower(s), "broken pipe") ||
		strings.Contains(strings.ToLower(s), "timeout") ||
		strings.Contains(strings.ToLower(s), "tls handshake") ||
		strings.Contains(strings.ToLower(s), "connection refused")
}

type temporaryNetError interface {
	Temporary() bool
}

func isTemporaryNetErr(err error) bool {
	temporary, ok := err.(temporaryNetError)
	return ok && temporary.Temporary()
}
