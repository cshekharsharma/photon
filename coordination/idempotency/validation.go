package idempotency

import (
	"context"
	"fmt"
	"strings"
)

func validateReserve(ctx context.Context, req Request) error {
	if ctx == nil {
		return ErrNilContext
	}
	if err := validateParts(req.Key, req.Scope, req.Fingerprint, req.OwnerToken); err != nil {
		return err
	}
	if req.TTL <= 0 {
		return fmt.Errorf("%w: ttl must be positive", ErrInvalidRequest)
	}
	return nil
}

func validateComplete(ctx context.Context, req CompleteRequest) error {
	if ctx == nil {
		return ErrNilContext
	}
	if err := validateParts(req.Key, req.Scope, req.Fingerprint, req.OwnerToken); err != nil {
		return err
	}
	if req.TTL <= 0 {
		return fmt.Errorf("%w: ttl must be positive", ErrInvalidRequest)
	}
	return nil
}

func validateRelease(ctx context.Context, req ReleaseRequest) error {
	if ctx == nil {
		return ErrNilContext
	}
	return validateParts(req.Key, req.Scope, req.Fingerprint, req.OwnerToken)
}

func validateParts(key, scope, fingerprint, ownerToken string) error {
	switch {
	case strings.TrimSpace(key) == "":
		return fmt.Errorf("%w: key cannot be empty", ErrInvalidRequest)
	case strings.TrimSpace(scope) == "":
		return fmt.Errorf("%w: scope cannot be empty", ErrInvalidRequest)
	case strings.TrimSpace(fingerprint) == "":
		return fmt.Errorf("%w: fingerprint cannot be empty", ErrInvalidRequest)
	case strings.TrimSpace(ownerToken) == "":
		return fmt.Errorf("%w: owner token cannot be empty", ErrInvalidRequest)
	default:
		return nil
	}
}
