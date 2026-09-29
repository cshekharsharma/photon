package idempotency

import "fmt"

// StoreStrategy builds a concrete Store implementation.
type StoreStrategy interface {
	NewStore() (Store, error)
}

// RedisStrategy builds the production Redis-backed idempotency store.
type RedisStrategy struct {
	Options RedisStoreOptions
}

// NewStore creates a RedisStore from the strategy options.
func (s RedisStrategy) NewStore() (Store, error) {
	return NewRedisStore(s.Options)
}

// NewStore creates a Store from the provided strategy.
func NewStore(strategy StoreStrategy) (Store, error) {
	if strategy == nil {
		return nil, fmt.Errorf("%w: store strategy is required", ErrInvalidRequest)
	}
	store, err := strategy.NewStore()
	if err != nil {
		return nil, err
	}
	if store == nil {
		return nil, fmt.Errorf("%w: store strategy returned nil store", ErrInvalidRequest)
	}
	return store, nil
}
