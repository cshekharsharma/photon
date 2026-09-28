// Package rest provides the HTTP and REST related utilities for the application.
package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cshekharsharma/photon/utils/filesys"
	"github.com/cshekharsharma/photon/utils/rest/httpstub"
	"github.com/cshekharsharma/photon/utils/types"
)

type HttpClient interface {
	Do(req *http.Request) (*http.Response, error)
}

const DefaultHTTPRequestTimeout = 10 * time.Second

var (
	defaultHTTPRequestTimeoutNanos atomic.Int64

	readAllFn   = io.ReadAll
	callStubFn  = httpstub.CallStub
	closeBodyFn = func(c io.Closer) error {
		return c.Close()
	}
)

func init() {
	SetDefaultHTTPRequestTimeout(DefaultHTTPRequestTimeout)
}

// SetDefaultHTTPRequestTimeout configures the package default used when a
// RequestEntity has no timeout and the caller context has no deadline.
func SetDefaultHTTPRequestTimeout(timeout time.Duration) {
	if timeout <= 0 {
		timeout = DefaultHTTPRequestTimeout
	}
	defaultHTTPRequestTimeoutNanos.Store(int64(timeout))
}

// GetDefaultHTTPRequestTimeout returns the current package default request timeout.
func GetDefaultHTTPRequestTimeout() time.Duration {
	return time.Duration(defaultHTTPRequestTimeoutNanos.Load())
}

// RequestEntity is an input model for sending http requests to remote URL.
// This struct contains all required properties that are required
// to send a full-fledged HTTP request using CURL implementation.
type RequestEntity struct {
	Url         string            // Remote http(s) URL
	Method      string            // HTTP method
	Headers     map[string]string // HTTP headers as a kv pair
	QueryParams url.Values        // HTTP query params
	Body        io.Reader         // HTTP request payload/body
	Timeout     time.Duration     // Timeout for the request
	StubID      string            // ID of the stub to use for the request
}

// Determines and returns the flag if the current workflow is a HTTP request
// or not. This method is not full proof but will give you right results
// in all usual workflows.
func IsHttpRequest(r *http.Request) bool {
	httpMethod := r.Method

	webRequestMethods := []string{
		http.MethodGet,
		http.MethodPost,
		http.MethodDelete,
		http.MethodPost,
		http.MethodPatch,
		http.MethodOptions,
		http.MethodHead,
		http.MethodTrace,
		http.MethodConnect,
	}

	for _, method := range webRequestMethods {
		if method == httpMethod {
			return true
		}
	}
	return false
}

// MakeHTTPRequest sends an HTTP request to a remote URL using the provided configuration and returns
// the parsed JSON response as a map. Before performing the real network call, it optionally attempts
// to return a stubbed response if a StubID is specified and stubbing is enabled.
//
// The function supports GET requests with query parameters, arbitrary HTTP headers, request bodies,
// and graceful handling of various response scenarios.
//
// Parameters:
//   - client: An HttpClient interface (generally *http.Client) used to execute the request.
//   - request: A RequestEntity defining the URL, HTTP method, headers, optional query parameters,
//     request body, and an optional StubID.
//
// Behavior:
//  1. If request.StubID is non-empty, MakeHTTPRequest calls CallStub with the given ID. If
//     stubbing is enabled and a matching stub is found, it immediately returns the stubbed response
//     without making a real network call.
//  2. If no stub response is returned (either because stubs are disabled, the StubID doesn't match
//     any stub, or StubID is empty), MakeHTTPRequest proceeds to make a real HTTP call via client.Do(...)
//     using the provided URL, method, headers, and body.
//  3. On success, it attempts to parse the response body as JSON into a map[string]interface{}. The
//     response is considered successful if it returns HTTP 200 (OK) or 201 (Created).
//  4. If the response status code is not 200 or 201, MakeHTTPRequest returns an error with the
//     response's status text.
//  5. If parsing the JSON fails, it returns a JSON unmarshal error.
//  6. If the HTTP call returns an error or a nil response, that error is returned.
//
// Typical Usage:
//
//	request := RequestEntity{
//	    Url:    "https://api.example.com/data",
//	    Method: http.MethodGet,
//	    QueryParams: map[string][]string{
//	        "filter": {"active"},
//	    },
//	    Headers: map[string]string{
//	        "Authorization": "Bearer token_here",
//	    },
//	    StubID: "stub-1234", // Optional: if set and stubs match, will return stubbed response
//	}
//
//	result, err := MakeHTTPRequest(ctx, httpClient, request)
//	if err != nil {
//	    log.Fatalf("Error making HTTP request: %v", err)
//	}
//	fmt.Printf("Received response: %v\n", result)
//
// In testing or development:
//   - Set StubID and configure stubs via AddStub/CallStub to return predictable responses.
//   - Disable stubs or omit StubID to get real remote responses.
//
// MakeHTTPRequest returns:
//   - (map[string]interface{}, error): On success, the parsed JSON response and no error;
//     otherwise, an empty map and an error describing what went wrong.
func MakeHTTPRequest(ctx context.Context, client HttpClient, request RequestEntity) (responseObject map[string]interface{}, err error) {
	ctx, cancel := httpRequestContext(ctx, request.Timeout)
	defer cancel()

	u, err := url.Parse(request.Url)
	if err != nil {
		return responseObject, err
	}

	if request.Method == http.MethodGet {
		q := u.Query()
		for k, v := range request.QueryParams {
			q.Set(k, strings.Join(v, ","))
		}
		u.RawQuery = q.Encode()
	}

	// If a StubID is provided, try to call the stub first.
	// If stubs are disabled or stub not found, CallStub returns nil response and we proceed to real call.
	// If a stub returns a response or error, we handle that directly.
	if request.StubID != "" && httpstub.IsStubbingEnabled() {
		stubResp, stubErr := callStubFn(ctx, request.StubID)
		if stubErr != nil {
			return responseObject, stubErr
		}

		if stubResp != nil {
			defer func() {
				if closeErr := stubResp.Body.Close(); err == nil && closeErr != nil {
					err = closeErr
				}
			}()

			responseData, readErr := readAllFn(stubResp.Body)
			if readErr != nil {
				return responseObject, readErr
			}

			jsonErr := json.Unmarshal(responseData, &responseObject)

			validStatus := []int{
				http.StatusOK,
				http.StatusCreated,
			}
			if isValid, _ := types.ExistsInList(stubResp.StatusCode, validStatus); !isValid {
				return responseObject, errors.New(stubResp.Status)
			}

			if jsonErr != nil {
				return responseObject, jsonErr
			}

			return responseObject, nil
		}
	}

	// If we reach here, it means either no StubID was given
	// or CallStub returned nil (stubs disabled or no stub matched).
	httpreq, err := http.NewRequestWithContext(ctx, request.Method, u.String(), request.Body)
	if err != nil {
		return responseObject, err
	}

	for k, v := range request.Headers {
		httpreq.Header.Set(k, v)
	}

	res, err := client.Do(httpreq)
	if err != nil {
		return responseObject, err
	}

	if res == nil {
		return responseObject, fmt.Errorf("error: calling %s returned empty response", u.String())
	}

	defer func() {
		if closeErr := res.Body.Close(); err == nil && closeErr != nil {
			err = closeErr
		}
	}()

	responseData, readErr := readAllFn(res.Body)
	if readErr != nil {
		return responseObject, readErr
	}
	jsonErr := json.Unmarshal(responseData, &responseObject)

	validStatus := []int{
		http.StatusOK,
		http.StatusCreated,
	}
	if isValid, _ := types.ExistsInList(res.StatusCode, validStatus); !isValid {
		return responseObject, errors.New(res.Status)
	}

	if jsonErr != nil {
		return responseObject, jsonErr
	}

	return responseObject, nil
}

func httpRequestContext(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if _, hasDeadline := ctx.Deadline(); hasDeadline {
		return ctx, func() {}
	}
	if timeout <= 0 {
		timeout = GetDefaultHTTPRequestTimeout()
	}
	return context.WithTimeout(ctx, timeout)
}

// ValidateAndFetchGetParams extracts and validates GET parameters from the request based on the provided keys map.
// It ensures that each key exists and converts it to the specified data type in the map.
//
// Parameters:
//   - r: Pointer to the http.Request from which GET parameters are to be extracted.
//   - keys: Map specifying each key and its expected data type using reflect.Kind.
//
// Returns:
//   - A map containing the validated parameter values.
//   - An error if a parameter is missing or cannot be converted to the expected type.
func ValidateAndFetchGetParams(r *http.Request, keys map[string]reflect.Kind) (map[string]interface{}, error) {
	values := make(map[string]interface{}, len(keys))

	for key, dt := range keys {
		if r.URL.Query().Has(key) {
			val := r.URL.Query().Get(key)

			if dt == reflect.Int {
				intval, err := types.ToInt(val)
				if err != nil {
					return nil, fmt.Errorf("invalid value for key: `%s`", key)
				}

				values[key] = intval
			} else {
				values[key] = val
			}
		} else {
			return nil, fmt.Errorf("param `%s` is required", key)
		}
	}

	return values, nil
}

// ValidateAndFetchPostParams extracts and validates POST parameters from the request based on the provided keys map.
// Similar to ValidateAndFetchGetParams, it parses form data and ensures each key is present and correctly typed.
//
// Parameters:
//   - r: Pointer to the http.Request from which POST parameters are to be extracted.
//   - keys: Map specifying each key and its expected data type using reflect.Kind.
//
// Returns:
//   - A map containing the validated parameter values.
//   - An error if form data cannot be parsed, a parameter is missing, or conversion fails.
func ValidateAndFetchPostParams(r *http.Request, keys map[string]reflect.Kind) (map[string]interface{}, error) {
	values := make(map[string]interface{}, len(keys))

	if err := r.ParseForm(); err != nil {
		return nil, fmt.Errorf("failed to parse form data")
	}

	for key, dt := range keys {
		if _, exists := r.Form[key]; exists {
			val := r.FormValue(key)

			if dt == reflect.Int {
				intval, err := types.ToInt(val)
				if err != nil {
					return nil, fmt.Errorf("invalid value for key: `%s`", key)
				}

				values[key] = intval
			} else {
				values[key] = val
			}
		} else {
			return nil, fmt.Errorf("param `%s` is required", key)
		}
	}

	return values, nil
}

// ReadJsonPayload reads and parses the JSON-encoded body of an http.Request into a map.
// This function is used to extract data from requests with JSON payloads.
//
// Parameters:
//   - r: Pointer to the http.Request that contains the JSON body to be read.
//
// Returns:
//   - A map of string to interface{} that holds the JSON data.
//   - An error if the body cannot be read or the JSON data is malformed.
func ReadJsonPayload(r *http.Request) (map[string]interface{}, error) {
	body, readErr := io.ReadAll(r.Body)
	closeErr := closeBodyFn(r.Body)
	if readErr != nil {
		return nil, errors.New("error reading request body")
	}
	if closeErr != nil {
		return nil, closeErr
	}

	var data map[string]interface{}
	err := json.Unmarshal(body, &data)
	if err != nil {
		return nil, errors.New("error parsing JSON data")
	}

	return data, nil
}

// DownloadBytes sends a byte slice as a downloadable file.
// - Keeps UTF-8 filenames, sanitizes only unsafe chars via SanitiseFilename
// - Sniffs Content-Type if not provided
// - Streams with range support using http.ServeContent
// - Exposes Content-Disposition header for JS to read (if needed)
//
// Parameters:
//   - w: http.ResponseWriter to write the response to.
//   - r: Pointer to the http.Request that initiated the download.
//   - filename: The desired filename for the downloaded file.
//   - contentType: The MIME type of the file. If empty, it will be auto-detected.
//   - data: The byte slice containing the file data to be sent.
func DownloadBytes(w http.ResponseWriter, r *http.Request, filename, contentType string, data []byte) {
	w.Header().Set(HeaderXContentTypeOptions, "nosniff")

	// Content-Type (sniff if not provided)
	if contentType == "" {
		if len(data) > 0 {
			n := 512
			if len(data) < n {
				n = len(data)
			}
			w.Header().Set(HeaderContentType, http.DetectContentType(data[:n]))
		} else {
			w.Header().Set(HeaderContentType, ContentTypeOctetStream)
		}
	} else {
		w.Header().Set(HeaderContentType, contentType)
	}

	// UTF-8 filename sanitization + RFC5987 percent-encoded variant
	safe, encoded := filesys.SanitizeFilename(filename, "download")
	w.Header().Set(HeaderContentDisposition,
		fmt.Sprintf(`attachment; filename="%s"; filename*=UTF-8''%s`, safe, encoded),
	)

	w.Header().Set(HeaderAccessControlExposeHeaders, "Content-Disposition")
	w.Header().Set(HeaderCacheControl, "private, max-age=0, must-revalidate")

	reader := bytes.NewReader(data)
	http.ServeContent(w, r, safe, time.Time{}, reader)
}
