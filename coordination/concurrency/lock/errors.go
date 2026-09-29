package concurrency

import "errors"

var (
	// ErrLockNotAcquired is returned when a distributed lock could not be acquired within
	// the expected time window. It typically occurs if another process or node is already
	// holding the lock.
	ErrLockNotAcquired = errors.New("could not acquire lock") //

	// ErrLockNotHeld is returned when attempting to unlock or extend a lock that the caller
	// does not own. It ensures that only the holder of the lock can modify or release it.
	ErrLockNotHeld = errors.New("lock not held")

	ErrNilLockContext  = errors.New("context cannot be nil")
	ErrNilLockFunction = errors.New("distributed lock function cannot be nil")
)
