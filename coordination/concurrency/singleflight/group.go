// Package singleflight provides per-process duplicate suppression for
// concurrently requested work with the same key.
package singleflight

import (
	"context"
	"errors"
	"fmt"
	"strings"

	xsingleflight "golang.org/x/sync/singleflight"
)

var (
	ErrNilGroup     = errors.New("singleflight group cannot be nil")
	ErrNilContext   = errors.New("singleflight context cannot be nil")
	ErrEmptyKey     = errors.New("singleflight key cannot be empty")
	ErrNilFunction  = errors.New("singleflight function cannot be nil")
	ErrTypeMismatch = errors.New("singleflight result type mismatch")
)

// Group suppresses duplicate in-flight work for matching keys within one process.
type Group struct {
	group xsingleflight.Group
}

type typedResult[T any] struct {
	value T
}

type typedResultMarker interface {
	singleflightTypedResult()
}

func (typedResult[T]) singleflightTypedResult() {}

// NewGroup creates a singleflight group.
func NewGroup() *Group {
	return &Group{}
}

// Do runs fn once for concurrent callers using the same key. Duplicate callers
// wait for the in-flight result unless their own context is canceled first.
func (g *Group) Do(
	ctx context.Context,
	key string,
	fn func(context.Context) (any, error),
) (value any, shared bool, err error) {
	value, shared, err = g.do(ctx, key, fn)
	if err != nil {
		return nil, shared, err
	}
	if _, ok := value.(typedResultMarker); ok {
		return nil, shared, fmt.Errorf("%w for key %q", ErrTypeMismatch, key)
	}
	return value, shared, nil
}

// DoTyped runs fn once for concurrent callers using the same key and returns a
// typed value without requiring callers to perform assertions.
func DoTyped[T any](
	ctx context.Context,
	group *Group,
	key string,
	fn func(context.Context) (T, error),
) (value T, shared bool, err error) {
	if fn == nil {
		return value, false, ErrNilFunction
	}

	result, shared, err := group.do(ctx, key, func(ctx context.Context) (any, error) {
		typedValue, fnErr := fn(ctx)
		return typedResult[T]{value: typedValue}, fnErr
	})
	if err != nil {
		return value, shared, err
	}

	typed, ok := result.(typedResult[T])
	if !ok {
		return value, shared, fmt.Errorf("%w for key %q", ErrTypeMismatch, key)
	}
	return typed.value, shared, nil
}

// Forget removes key from the in-flight set when present. It is intentionally
// best-effort so cleanup paths can call it without extra branching.
func (g *Group) Forget(key string) {
	if g == nil || strings.TrimSpace(key) == "" {
		return
	}
	g.group.Forget(key)
}

func (g *Group) do(
	ctx context.Context,
	key string,
	fn func(context.Context) (any, error),
) (value any, shared bool, err error) {
	if g == nil {
		return nil, false, ErrNilGroup
	}
	if ctx == nil {
		return nil, false, ErrNilContext
	}
	if strings.TrimSpace(key) == "" {
		return nil, false, ErrEmptyKey
	}
	if fn == nil {
		return nil, false, ErrNilFunction
	}

	resultCh := g.group.DoChan(key, func() (any, error) {
		return fn(ctx)
	})

	select {
	case <-ctx.Done():
		return nil, false, ctx.Err()
	case result := <-resultCh:
		return result.Val, result.Shared, result.Err
	}
}
