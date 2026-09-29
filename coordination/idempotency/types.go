package idempotency

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// Store owns atomic idempotency state for a scoped key.
type Store interface {
	Reserve(ctx context.Context, req Request) (Decision, error)
	Complete(ctx context.Context, req CompleteRequest) error
	Release(ctx context.Context, req ReleaseRequest) error
}

// Request identifies a single idempotent operation attempt.
type Request struct {
	Key         string
	Scope       string
	Fingerprint string
	TTL         time.Duration
	OwnerToken  string
}

// CompleteRequest stores the final result for the owner that reserved the key.
type CompleteRequest struct {
	Key         string
	Scope       string
	Fingerprint string
	TTL         time.Duration
	OwnerToken  string
	Response    StoredResponse
}

// ReleaseRequest releases an unfinished processing record owned by OwnerToken.
type ReleaseRequest struct {
	Key         string
	Scope       string
	Fingerprint string
	OwnerToken  string
}

// Decision describes the current state for a scoped idempotency key.
type Decision struct {
	Status   Status
	Response *StoredResponse
}

// Status is the store decision for a reserve attempt.
type Status string

const (
	StatusReserved  Status = "reserved"
	StatusInFlight  Status = "in_flight"
	StatusCompleted Status = "completed"
	StatusMismatch  Status = "mismatch"
)

// StoredResponse is transport-neutral response data safe to replay.
type StoredResponse struct {
	StatusCode int                 `json:"status_code"`
	Header     map[string][]string `json:"header,omitempty"`
	Body       []byte              `json:"body,omitempty"`
	Replayable bool                `json:"replayable"`
}

// NewOwnerToken creates an opaque fencing token for one reserve/complete cycle.
func NewOwnerToken() string {
	return uuid.NewString()
}
