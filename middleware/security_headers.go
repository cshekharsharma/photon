package middleware

import (
	"net/http"
)

const (
	defaultXFrameOptions         = "DENY"
	defaultContentTypeOptions    = "nosniff"
	defaultReferrerPolicy        = "no-referrer"
	defaultPermissionsPolicy     = "camera=(), microphone=(), geolocation=()"
	defaultCrossOriginOpener     = "same-origin"
	defaultXSSProtectionDisabled = "0"
)

type SecurityHeadersOptions struct {
	XFrameOptions           string
	XContentTypeOptions     string
	ReferrerPolicy          string
	PermissionsPolicy       string
	CrossOriginOpenerPolicy string
	XSSProtection           string
	ContentSecurityPolicy   string
}

// SecurityHeaders returns middleware that adds security HTTP headers.
func SecurityHeaders(options SecurityHeadersOptions) func(http.Handler) http.Handler {
	normalized := normalizeSecurityHeadersOptions(options)
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			setHeaderIfConfigured(w, "X-Frame-Options", normalized.XFrameOptions)
			setHeaderIfConfigured(w, "X-XSS-Protection", normalized.XSSProtection)
			setHeaderIfConfigured(w, "X-Content-Type-Options", normalized.XContentTypeOptions)
			setHeaderIfConfigured(w, "Referrer-Policy", normalized.ReferrerPolicy)
			setHeaderIfConfigured(w, "Permissions-Policy", normalized.PermissionsPolicy)
			setHeaderIfConfigured(w, "Cross-Origin-Opener-Policy", normalized.CrossOriginOpenerPolicy)
			setHeaderIfConfigured(w, "Content-Security-Policy", normalized.ContentSecurityPolicy)

			next.ServeHTTP(w, r)
		})
	}
}

func normalizeSecurityHeadersOptions(options SecurityHeadersOptions) SecurityHeadersOptions {
	if options.XFrameOptions == "" {
		options.XFrameOptions = defaultXFrameOptions
	}
	if options.XContentTypeOptions == "" {
		options.XContentTypeOptions = defaultContentTypeOptions
	}
	if options.ReferrerPolicy == "" {
		options.ReferrerPolicy = defaultReferrerPolicy
	}
	if options.PermissionsPolicy == "" {
		options.PermissionsPolicy = defaultPermissionsPolicy
	}
	if options.CrossOriginOpenerPolicy == "" {
		options.CrossOriginOpenerPolicy = defaultCrossOriginOpener
	}
	if options.XSSProtection == "" {
		options.XSSProtection = defaultXSSProtectionDisabled
	}
	return options
}

func setHeaderIfConfigured(w http.ResponseWriter, key string, value string) {
	if value == "-" {
		return
	}
	if value != "" {
		w.Header().Set(key, value)
	}
}
