package idempotency

import "errors"

var (
	ErrNilContext        = errors.New("idempotency: context cannot be nil")
	ErrInvalidRequest    = errors.New("idempotency: invalid request")
	ErrStoreUnavailable  = errors.New("idempotency: store unavailable")
	ErrMismatch          = errors.New("idempotency: key reused with different fingerprint")
	ErrInFlight          = errors.New("idempotency: request already in flight")
	ErrReplayUnavailable = errors.New("idempotency: replay unavailable")
	ErrNotReserved       = errors.New("idempotency: reservation not owned by caller")
)
