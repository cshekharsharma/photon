// Package concurrency provides primitives to manage distributed concurrency patterns
// such as distributed locks.
package concurrency

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cshekharsharma/photon/storage/redis"
	"github.com/google/uuid"
)

// RedisLocker manages distributed locks using a Redis backend.
//
// It safely handles lock acquisition, renewal (extend), and release (unlock)
// operations by using Lua scripts to ensure atomicity and consistency across
// multiple nodes.
//
// It internally maintains an in-memory map to track currently held locks locally,
// making unlock and extend operations more efficient.
type RedisLocker struct {
	storageclient           redis.RedisInterface
	defaultLockRetryTimeout time.Duration
	lockStore               sync.Map // Map of [Key -> redisLockState] for local tracking
}

type redisLockState struct {
	value string
	token uint64
}

// Lock runs fn while holding a distributed lock on the specified key.
//
// Parameters:
//   - ctx: Context with cancellation or deadline for retries.
//   - key: Unique key representing the lock.
//   - expiry: How long the lock should live.
//   - retryInterval: Time to wait before retrying if the lock is already held.
//   - fn: Work to run while holding the lock. It receives a fencing token.
//
// Internally renews the lock while fn runs and releases it before returning.
func (d *RedisLocker) Lock(
	ctx context.Context,
	key string,
	expiry time.Duration,
	retryInterval time.Duration,
	fn func(context.Context, uint64) error,
) error {
	if ctx == nil {
		return ErrNilLockContext
	}
	if fn == nil {
		return ErrNilLockFunction
	}
	if expiry <= 0 {
		return errors.New("lock expiry must be positive")
	}

	token, err := d.acquire(ctx, key, expiry, retryInterval)
	if err != nil {
		return err
	}

	workCtx, cancelWork := context.WithCancelCause(ctx)
	defer cancelWork(nil)

	stopRenewal := make(chan struct{})
	renewalDone := make(chan struct{})
	go d.renew(workCtx, cancelWork, key, expiry, stopRenewal, renewalDone)

	fnErr := fn(workCtx, token)
	close(stopRenewal)
	<-renewalDone

	if cause := context.Cause(workCtx); cause != nil && errors.Is(fnErr, context.Canceled) && !errors.Is(cause, context.Canceled) {
		fnErr = cause
	}

	unlockCtx, cancelUnlock := context.WithTimeout(context.WithoutCancel(ctx), expiry)
	defer cancelUnlock()

	unlockErr := d.unlock(unlockCtx, key)
	if fnErr != nil && unlockErr != nil {
		return errors.Join(fnErr, unlockErr)
	}
	if fnErr != nil {
		return fnErr
	}
	return unlockErr
}

func (d *RedisLocker) acquire(
	ctx context.Context,
	key string,
	expiry time.Duration,
	retryInterval time.Duration,
) (uint64, error) {
	if ctx == nil {
		return 0, ErrNilLockContext
	}
	if err := validateLockKey(key); err != nil {
		return 0, err
	}
	if expiry <= 0 {
		return 0, errors.New("lock expiry must be positive")
	}
	if retryInterval <= 0 {
		retryInterval = defaultLockRetryInterval
	}
	owner := uuid.NewString()
	expiryMillis := durationMilliseconds(expiry)

	// Ensure context has a deadline to avoid infinite looping.
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		timeout := d.defaultLockRetryTimeout
		if timeout <= 0 {
			timeout = defaultLockRetryTimeout
		}
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	for {
		token, err := d.tryAcquire(ctx, key, owner, expiryMillis)
		if err != nil {
			return 0, fmt.Errorf("distributed lock acquisition failed for key '%s': %w", key, err)
		}
		if token > 0 {
			d.lockStore.Store(key, redisLockState{
				value: lockValue(token, owner),
				token: token,
			})
			return token, nil
		}

		timer := time.NewTimer(jitterLockRetryInterval(retryInterval))
		select {
		case <-ctx.Done():
			timer.Stop()
			return 0, ErrLockNotAcquired
		case <-timer.C:
		}
	}
}

func (d *RedisLocker) fencingToken(key string) (uint64, bool) {
	state, ok := d.lockState(key)
	if !ok || state.token == 0 {
		return 0, false
	}
	return state.token, true
}

func (d *RedisLocker) unlock(ctx context.Context, key string) error {
	if err := validateLockKey(key); err != nil {
		return err
	}

	valueRaw, ok := d.lockStore.Load(key)
	if !ok {
		return ErrLockNotHeld
	}
	state, ok := redisLockStateFromValue(valueRaw)
	if !ok {
		return ErrLockNotHeld
	}

	const luaScript = `
		if redis.call("GET", KEYS[1]) == ARGV[1] then
			return redis.call("DEL", KEYS[1])
		else
			return 0
		end
	`

	res, err := d.storageclient.GetClient().Eval(ctx, luaScript, []string{redisLockKey(key)}, state.value).Result()
	if err != nil {
		return err
	}

	intRes, ok := res.(int64)
	if !ok {
		return errors.New("unexpected result type from Eval")
	}
	if intRes == 0 {
		d.lockStore.Delete(key)
		return ErrLockNotHeld
	}

	d.lockStore.Delete(key)
	return nil
}

func (d *RedisLocker) extend(ctx context.Context, key string, extension time.Duration) error {
	if err := validateLockKey(key); err != nil {
		return err
	}
	if extension <= 0 {
		return errors.New("lock extension must be positive")
	}

	valueRaw, ok := d.lockStore.Load(key)
	if !ok {
		return ErrLockNotHeld
	}
	state, ok := redisLockStateFromValue(valueRaw)
	if !ok {
		return ErrLockNotHeld
	}

	const luaScript = `
		if redis.call("GET", KEYS[1]) == ARGV[1] then
			return redis.call("PEXPIRE", KEYS[1], ARGV[2])
		else
			return 0
		end
	`

	millis := durationMilliseconds(extension)
	cmd := d.storageclient.GetClient().Eval(ctx, luaScript, []string{redisLockKey(key)}, state.value, millis)

	if cmd == nil {
		return errors.New("internal error in redis command")
	}

	res, err := cmd.Result()
	if err != nil {
		return err
	}

	intRes, ok := res.(int64)
	if !ok {
		return errors.New("unexpected result type from Eval")
	}

	if intRes == 0 {
		d.lockStore.Delete(key)
		return ErrLockNotHeld
	}

	return nil
}

func (d *RedisLocker) renew(
	ctx context.Context,
	cancel context.CancelCauseFunc,
	key string,
	expiry time.Duration,
	stop <-chan struct{},
	done chan<- struct{},
) {
	defer close(done)

	interval := expiry / 2
	if interval <= 0 {
		interval = expiry
	}

	timer := time.NewTimer(interval)
	defer timer.Stop()

	for {
		select {
		case <-stop:
			return
		case <-ctx.Done():
			return
		case <-timer.C:
			renewCtx, cancelRenew := context.WithTimeout(context.WithoutCancel(ctx), interval)
			err := d.extend(renewCtx, key, expiry)
			cancelRenew()
			if err != nil {
				cancel(fmt.Errorf("distributed lock renewal failed for key %q: %w", key, err))
				return
			}
			timer.Reset(interval)
		}
	}
}

func (d *RedisLocker) tryAcquire(ctx context.Context, key string, owner string, expiryMillis int64) (uint64, error) {
	const luaScript = `
		if redis.call("EXISTS", KEYS[1]) == 1 then
			return 0
		end
		local token = redis.call("INCR", KEYS[2])
		redis.call("PSETEX", KEYS[1], ARGV[1], token .. ":" .. ARGV[2])
		return token
	`

	res, err := d.storageclient.GetClient().
		Eval(ctx, luaScript, []string{redisLockKey(key), fencingCounterKey(key)}, expiryMillis, owner).
		Result()
	if err != nil {
		return 0, err
	}
	return positiveUint64(res)
}

func (d *RedisLocker) lockState(key string) (redisLockState, bool) {
	valueRaw, ok := d.lockStore.Load(key)
	if !ok {
		return redisLockState{}, false
	}
	return redisLockStateFromValue(valueRaw)
}

func redisLockStateFromValue(value any) (redisLockState, bool) {
	switch typed := value.(type) {
	case redisLockState:
		return typed, typed.value != ""
	case string:
		return redisLockState{value: typed}, typed != ""
	default:
		return redisLockState{}, false
	}
}

func positiveUint64(value any) (uint64, error) {
	switch typed := value.(type) {
	case int64:
		if typed < 0 {
			return 0, errors.New("unexpected negative fencing token")
		}
		return strconv.ParseUint(strconv.FormatInt(typed, 10), 10, 64)
	case uint64:
		return typed, nil
	case int:
		if typed < 0 {
			return 0, errors.New("unexpected negative fencing token")
		}
		return strconv.ParseUint(strconv.Itoa(typed), 10, 64)
	default:
		return 0, errors.New("unexpected result type from Eval")
	}
}

func durationMilliseconds(duration time.Duration) int64 {
	millis := duration / time.Millisecond
	if duration%time.Millisecond != 0 {
		millis++
	}
	return int64(millis)
}

func validateLockKey(key string) error {
	if strings.TrimSpace(key) == "" {
		return errors.New("lock key cannot be empty")
	}
	return nil
}

func jitterLockRetryInterval(interval time.Duration) time.Duration {
	spread := interval / 5
	if spread <= 0 {
		return interval
	}
	return interval - spread/2 + time.Duration(time.Now().UnixNano()%int64(spread+1))
}

func lockValue(token uint64, owner string) string {
	return strconv.FormatUint(token, 10) + ":" + owner
}

func redisLockKey(key string) string {
	return redisClusterKey(key, "holder")
}

func fencingCounterKey(key string) string {
	return redisClusterKey(key, "fence")
}

func redisClusterKey(key string, suffix string) string {
	sum := sha256.Sum256([]byte(key))
	return "{photon-lock:" + hex.EncodeToString(sum[:]) + "}:" + suffix
}
