package concurrency

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/cshekharsharma/photon/storage/redis"
)

var (
	defaultLockRetryTimeout  = 10 * time.Second       // default retry timeout for lock acquisition
	defaultLockRetryInterval = 100 * time.Millisecond // default wait between acquisition attempts
)

// DistributedLocker executes work while holding a distributed lock.
type DistributedLocker interface {
	// Lock acquires a distributed lock, passes the fencing token to fn, renews
	// the lock while fn runs, and releases the lock before returning.
	Lock(ctx context.Context, key string, expiry time.Duration, retryInterval time.Duration, fn func(context.Context, uint64) error) error
}

// GetDistributedLocker creates a new instance of DistributedLocker based on the provided options.
// It supports different locker providers, such as Redis.
func GetDistributedLocker(opts *LockOptions) (DistributedLocker, error) {
	if opts == nil {
		return nil, errors.New("options cannot be nil")
	}

	if opts.DefaultLockRetryTimeout == 0 {
		opts.DefaultLockRetryTimeout = defaultLockRetryTimeout
	}

	switch opts.LockerProvider {
	case RedisLockProvider:
		storageclient, ok := opts.StorageClient.(redis.RedisInterface)
		if !ok {
			return nil, errors.New("invalid storage client type for Redis locker")
		}
		return &RedisLocker{
			storageclient:           storageclient,
			defaultLockRetryTimeout: opts.DefaultLockRetryTimeout,
		}, nil

	default:
		return nil, fmt.Errorf("unsupported locker provider: %s", opts.LockerProvider)
	}
}
