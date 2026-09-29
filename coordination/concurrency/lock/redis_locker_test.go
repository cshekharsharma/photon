package concurrency

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/cshekharsharma/photon/storage/redis"
	"github.com/cshekharsharma/photon/utils/testutil/mocks"
	redisv9 "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type mockRedis struct {
	mock.Mock
	client redis.RedisClientInterface
}

func (m *mockRedis) GetClient() redis.RedisClientInterface {
	args := m.Called()
	return args.Get(0).(redis.RedisClientInterface)
}

func (m *mockRedis) GetRawClient() *redisv9.Client {
	args := m.Called()
	return args.Get(0).(*redisv9.Client)
}

func (m *mockRedis) SetClient(client redis.RedisClientInterface) {
	m.client = client
}

func (m *mockRedis) Close() error {
	args := m.Called()
	return args.Error(0)
}

// ----------------------------- Tests for RedisLocker -----------------------------
func TestRedisLock_Success(t *testing.T) {
	mockClient := &mocks.MockRedisClient{}
	mockRedis := &mockRedis{client: mockClient}
	mockRedis.On("GetClient").Return(mockClient)
	locker, _ := GetDistributedLocker(&LockOptions{LockerProvider: RedisLockProvider, StorageClient: mockRedis})

	mockCmd := redisv9.NewCmdResult(int64(7), nil)
	mockClient.On("Eval", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(mockCmd)

	redisLocker := locker.(*RedisLocker)
	token, err := redisLocker.acquire(context.TODO(), "mylock", time.Second, time.Millisecond*10)
	assert.NoError(t, err)
	assert.Equal(t, uint64(7), token)

	storedToken, ok := redisLocker.fencingToken("mylock")
	assert.True(t, ok)
	assert.Equal(t, uint64(7), storedToken)
}

func TestRedisLock_RunsFunctionAndUnlocks(t *testing.T) {
	mockClient := &mocks.MockRedisClient{}
	mockRedis := &mockRedis{client: mockClient}
	mockRedis.On("GetClient").Return(mockClient)
	locker, _ := GetDistributedLocker(&LockOptions{LockerProvider: RedisLockProvider, StorageClient: mockRedis})

	mockClient.On("Eval", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(redisv9.NewCmdResult(int64(7), nil)).
		Once()
	mockClient.On("Eval", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(redisv9.NewCmdResult(int64(1), nil)).
		Once()

	err := locker.Lock(context.Background(), "mylock", time.Second, time.Millisecond, func(ctx context.Context, token uint64) error {
		assert.NoError(t, ctx.Err())
		assert.Equal(t, uint64(7), token)
		return nil
	})

	assert.NoError(t, err)
	mockClient.AssertExpectations(t)
}

func TestRedisLock_PublicValidation(t *testing.T) {
	locker := &RedisLocker{}
	fn := func(context.Context, uint64) error { return nil }

	var nilContext context.Context
	err := locker.Lock(nilContext, "mylock", time.Second, time.Millisecond, fn)
	assert.Equal(t, ErrNilLockContext, err)

	err = locker.Lock(context.Background(), "mylock", time.Second, time.Millisecond, nil)
	assert.Equal(t, ErrNilLockFunction, err)

	err = locker.Lock(context.Background(), "mylock", 0, time.Millisecond, fn)
	assert.EqualError(t, err, "lock expiry must be positive")

	err = locker.Lock(context.Background(), " \t", time.Second, time.Millisecond, fn)
	assert.EqualError(t, err, "lock key cannot be empty")
}

func TestRedisLock_JoinsFunctionAndUnlockErrors(t *testing.T) {
	mockClient := &mocks.MockRedisClient{}
	mockRedis := &mockRedis{client: mockClient}
	mockRedis.On("GetClient").Return(mockClient)
	locker, _ := GetDistributedLocker(&LockOptions{LockerProvider: RedisLockProvider, StorageClient: mockRedis})

	workErr := errors.New("work failed")
	unlockErr := errors.New("unlock failed")
	mockClient.On("Eval", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(redisv9.NewCmdResult(int64(7), nil)).
		Once()
	mockClient.On("Eval", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(redisv9.NewCmdResult(nil, unlockErr)).
		Once()

	err := locker.Lock(context.Background(), "mylock", time.Second, time.Millisecond, func(context.Context, uint64) error {
		return workErr
	})

	assert.ErrorIs(t, err, workErr)
	assert.ErrorIs(t, err, unlockErr)
}

func TestRedisLock_RenewalFailureCancelsWork(t *testing.T) {
	mockClient := &mocks.MockRedisClient{}
	mockRedis := &mockRedis{client: mockClient}
	mockRedis.On("GetClient").Return(mockClient)
	locker, _ := GetDistributedLocker(&LockOptions{LockerProvider: RedisLockProvider, StorageClient: mockRedis})

	extendErr := errors.New("extend failed")
	mockClient.On("Eval", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(redisv9.NewCmdResult(int64(7), nil)).
		Once()
	mockClient.On("Eval", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(redisv9.NewCmdResult(nil, extendErr)).
		Once()
	mockClient.On("Eval", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(redisv9.NewCmdResult(int64(1), nil)).
		Once()

	err := locker.Lock(context.Background(), "mylock", 2*time.Millisecond, time.Millisecond, func(ctx context.Context, _ uint64) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
			return errors.New("renewal did not cancel work")
		}
	})

	assert.ErrorIs(t, err, extendErr)
}

func TestRedisLock_RenewsWhileRunning(t *testing.T) {
	mockClient := &mocks.MockRedisClient{}
	mockRedis := &mockRedis{client: mockClient}
	mockRedis.On("GetClient").Return(mockClient)
	locker, _ := GetDistributedLocker(&LockOptions{LockerProvider: RedisLockProvider, StorageClient: mockRedis})

	extended := make(chan struct{})
	mockClient.On("Eval", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(redisv9.NewCmdResult(int64(7), nil)).
		Once()
	mockClient.On("Eval", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Run(func(mock.Arguments) { close(extended) }).
		Return(redisv9.NewCmdResult(int64(1), nil)).
		Once()
	mockClient.On("Eval", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(redisv9.NewCmdResult(int64(1), nil)).
		Once()

	err := locker.Lock(context.Background(), "mylock", 2*time.Millisecond, time.Millisecond, func(context.Context, uint64) error {
		select {
		case <-extended:
			return nil
		case <-time.After(100 * time.Millisecond):
			return errors.New("renewal did not run")
		}
	})

	assert.NoError(t, err)
}

func TestRedisAcquire_NilContext(t *testing.T) {
	var nilContext context.Context
	token, err := (&RedisLocker{}).acquire(nilContext, "mylock", time.Second, time.Millisecond)

	assert.Zero(t, token)
	assert.Equal(t, ErrNilLockContext, err)
}

func TestRedisRenew_StopsOnContextDoneAndTinyExpiry(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(context.Canceled)

	locker := &RedisLocker{}
	stop := make(chan struct{})
	done := make(chan struct{})
	locker.renew(ctx, cancel, "resource", time.Second, stop, done)

	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("renewal loop did not stop after context cancellation")
	}

	mockClient := &mocks.MockRedisClient{}
	mockRedis := &mockRedis{client: mockClient}
	mockRedis.On("GetClient").Return(mockClient)
	locker = &RedisLocker{storageclient: mockRedis}
	locker.lockStore.Store("resource", redisLockState{value: "1:owner", token: 1})

	extendErr := errors.New("tiny expiry failed")
	mockClient.On("Eval", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(redisv9.NewCmdResult(nil, extendErr)).
		Once()

	ctx, cancel = context.WithCancelCause(context.Background())
	done = make(chan struct{})
	locker.renew(ctx, cancel, "resource", time.Nanosecond, stop, done)

	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("renewal loop did not stop after tiny-expiry failure")
	}
	assert.ErrorIs(t, context.Cause(ctx), extendErr)
}

func TestRedisLock_ReturnsFencingToken(t *testing.T) {
	mockClient := &mocks.MockRedisClient{}
	mockRedis := &mockRedis{client: mockClient}
	mockRedis.On("GetClient").Return(mockClient)
	locker, _ := GetDistributedLocker(&LockOptions{LockerProvider: RedisLockProvider, StorageClient: mockRedis})

	mockCmd := redisv9.NewCmdResult(int64(11), nil)
	mockClient.On("Eval", mock.Anything, mock.Anything, []string{redisLockKey("mylock"), fencingCounterKey("mylock")}, mock.Anything).Return(mockCmd)

	token, err := locker.(*RedisLocker).acquire(context.Background(), "mylock", time.Second+time.Nanosecond, time.Millisecond*10)
	assert.NoError(t, err)
	assert.Equal(t, uint64(11), token)
}

func TestRedisLock_ReacquireUpdatesFencingToken(t *testing.T) {
	mockClient := &mocks.MockRedisClient{}
	mockRedis := &mockRedis{client: mockClient}
	mockRedis.On("GetClient").Return(mockClient)
	locker, _ := GetDistributedLocker(&LockOptions{LockerProvider: RedisLockProvider, StorageClient: mockRedis})

	mockClient.On("Eval", mock.Anything, mock.Anything, []string{redisLockKey("resource"), fencingCounterKey("resource")}, mock.Anything).
		Return(redisv9.NewCmdResult(int64(1), nil)).
		Once()
	mockClient.On("Eval", mock.Anything, mock.Anything, []string{redisLockKey("resource"), fencingCounterKey("resource")}, mock.Anything).
		Return(redisv9.NewCmdResult(int64(2), nil)).
		Once()

	redisLocker := locker.(*RedisLocker)
	token, err := redisLocker.acquire(context.Background(), "resource", time.Second, time.Millisecond)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), token)

	token, err = redisLocker.acquire(context.Background(), "resource", time.Second, time.Millisecond)
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), token)

	storedToken, ok := redisLocker.fencingToken("resource")
	assert.True(t, ok)
	assert.Equal(t, uint64(2), storedToken)
}

func TestRedisLock_WithStorageFailure(t *testing.T) {
	mockClient := &mocks.MockRedisClient{}
	mockRedis := &mockRedis{client: mockClient}
	mockRedis.On("GetClient").Return(mockClient)
	locker, _ := GetDistributedLocker(&LockOptions{LockerProvider: RedisLockProvider, StorageClient: mockRedis})

	cmd := redisv9.NewCmdResult(nil, errors.New("some error"))
	mockClient.On("Eval", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(cmd)

	ctx := context.Background()
	token, err := locker.(*RedisLocker).acquire(ctx, "mylock", time.Second, time.Millisecond*10)
	assert.Zero(t, token)
	assert.Error(t, err)
}

func TestRedisLock_ContextTimeout(t *testing.T) {
	mockClient := &mocks.MockRedisClient{}
	mockRedis := &mockRedis{client: mockClient}
	mockRedis.On("GetClient").Return(mockClient) // Fix: Add this
	locker, _ := GetDistributedLocker(&LockOptions{LockerProvider: RedisLockProvider, StorageClient: mockRedis})

	mockCmd := redisv9.NewCmdResult(int64(0), nil)
	mockClient.On("Eval", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(mockCmd)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	token, err := locker.(*RedisLocker).acquire(ctx, "lockfail", time.Second, 10*time.Millisecond)
	assert.Zero(t, token)
	assert.Equal(t, ErrLockNotAcquired, err)
}

func TestRedisLock_InvalidExpiry(t *testing.T) {
	locker := &RedisLocker{}

	token, err := locker.acquire(context.Background(), "badlock", 0, time.Millisecond)

	assert.Zero(t, token)
	assert.EqualError(t, err, "lock expiry must be positive")
}

func TestRedisLock_InvalidKey(t *testing.T) {
	locker := &RedisLocker{}

	token, err := locker.acquire(context.Background(), " \t\n", time.Second, time.Millisecond)

	assert.Zero(t, token)
	assert.EqualError(t, err, "lock key cannot be empty")
}

func TestRedisLock_UsesDefaultRetryIntervalAndConfiguredTimeout(t *testing.T) {
	mockClient := &mocks.MockRedisClient{}
	mockRedis := &mockRedis{client: mockClient}
	mockRedis.On("GetClient").Return(mockClient)
	locker, _ := GetDistributedLocker(&LockOptions{
		LockerProvider:          RedisLockProvider,
		StorageClient:           mockRedis,
		DefaultLockRetryTimeout: 20 * time.Millisecond,
	})

	mockCmd := redisv9.NewCmdResult(int64(0), nil)
	mockClient.On("Eval", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(mockCmd)

	token, err := locker.(*RedisLocker).acquire(context.Background(), "lockfail", time.Second, 0)

	assert.Zero(t, token)
	assert.Equal(t, ErrLockNotAcquired, err)
}

func TestRedisLock_UsesPackageDefaultTimeout(t *testing.T) {
	origTimeout := defaultLockRetryTimeout
	defaultLockRetryTimeout = 20 * time.Millisecond
	defer func() { defaultLockRetryTimeout = origTimeout }()

	mockClient := &mocks.MockRedisClient{}
	mockRedis := &mockRedis{client: mockClient}
	mockRedis.On("GetClient").Return(mockClient)
	locker := &RedisLocker{storageclient: mockRedis}

	mockCmd := redisv9.NewCmdResult(int64(0), nil)
	mockClient.On("Eval", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(mockCmd)

	token, err := locker.acquire(context.Background(), "lockfail", time.Second, time.Millisecond)

	assert.Zero(t, token)
	assert.Equal(t, ErrLockNotAcquired, err)
}

func TestRedisLock_UnexpectedTokenType(t *testing.T) {
	mockClient := &mocks.MockRedisClient{}
	mockRedis := &mockRedis{client: mockClient}
	mockRedis.On("GetClient").Return(mockClient)
	locker, _ := GetDistributedLocker(&LockOptions{LockerProvider: RedisLockProvider, StorageClient: mockRedis})

	mockClient.On("Eval", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(redisv9.NewCmdResult("bad-token", nil))

	token, err := locker.(*RedisLocker).acquire(context.Background(), "mylock", time.Second, time.Millisecond)

	assert.Zero(t, token)
	assert.EqualError(t, err, "distributed lock acquisition failed for key 'mylock': unexpected result type from Eval")
}

func TestRedisLock_NegativeToken(t *testing.T) {
	mockClient := &mocks.MockRedisClient{}
	mockRedis := &mockRedis{client: mockClient}
	mockRedis.On("GetClient").Return(mockClient)
	locker, _ := GetDistributedLocker(&LockOptions{LockerProvider: RedisLockProvider, StorageClient: mockRedis})

	mockClient.On("Eval", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(redisv9.NewCmdResult(int64(-1), nil))

	token, err := locker.(*RedisLocker).acquire(context.Background(), "mylock", time.Second, time.Millisecond)

	assert.Zero(t, token)
	assert.EqualError(t, err, "distributed lock acquisition failed for key 'mylock': unexpected negative fencing token")
}

func TestRedisFencingToken_NotHeldOrLegacyValue(t *testing.T) {
	locker := &RedisLocker{}

	token, ok := locker.fencingToken("missing")
	assert.False(t, ok)
	assert.Zero(t, token)

	locker.lockStore.Store("legacy", "lockuuid")
	token, ok = locker.fencingToken("legacy")
	assert.False(t, ok)
	assert.Zero(t, token)
}

func TestPositiveUint64NumericTypes(t *testing.T) {
	token, err := positiveUint64(uint64(9))
	assert.NoError(t, err)
	assert.Equal(t, uint64(9), token)

	token, err = positiveUint64(3)
	assert.NoError(t, err)
	assert.Equal(t, uint64(3), token)

	token, err = positiveUint64(-1)
	assert.Zero(t, token)
	assert.EqualError(t, err, "unexpected negative fencing token")
}

func TestRedisUnlock_InvalidLocalState(t *testing.T) {
	locker := &RedisLocker{}
	locker.lockStore.Store("unlockkey", 123)

	err := locker.unlock(context.Background(), "unlockkey")

	assert.Equal(t, ErrLockNotHeld, err)
}

func TestRedisUnlock_InvalidKey(t *testing.T) {
	locker := &RedisLocker{}

	err := locker.unlock(context.Background(), "")

	assert.EqualError(t, err, "lock key cannot be empty")
}

func TestRedisUnlock_Success(t *testing.T) {
	mockClient := &mocks.MockRedisClient{}
	mockRedis := &mockRedis{client: mockClient}
	mockRedis.On("GetClient").Return(mockClient)

	locker, _ := GetDistributedLocker(&LockOptions{LockerProvider: RedisLockProvider, StorageClient: mockRedis})
	locker.((*RedisLocker)).lockStore.Store("unlockkey", "lockuuid")

	cmd := redisv9.NewCmd(context.TODO())
	cmd.SetVal(int64(1))
	mockClient.On("Eval", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(cmd, nil)

	ctx := context.Background()
	err := locker.(*RedisLocker).unlock(ctx, "unlockkey")
	assert.NoError(t, err)
}

func TestRedisUnlock_LockNotHeld(t *testing.T) {
	mockClient := &mocks.MockRedisClient{}
	mockRedis := &mockRedis{client: mockClient}
	locker, _ := GetDistributedLocker(&LockOptions{LockerProvider: RedisLockProvider, StorageClient: mockRedis})

	ctx := context.Background()
	err := locker.(*RedisLocker).unlock(ctx, "nonexistent")
	assert.Equal(t, ErrLockNotHeld, err)
}

func TestRedisUnlock_EvalFails(t *testing.T) {
	mockClient := &mocks.MockRedisClient{}
	mockRedis := &mockRedis{client: mockClient}
	mockRedis.On("GetClient").Return(mockClient)
	locker, _ := GetDistributedLocker(&LockOptions{LockerProvider: RedisLockProvider, StorageClient: mockRedis})

	locker.(*RedisLocker).lockStore.Store("unlockkey", "lockuuid")

	mockClient.On("Eval", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(redisv9.NewCmdResult(nil, errors.New("eval error")))

	ctx := context.Background()
	err := locker.(*RedisLocker).unlock(ctx, "unlockkey")
	assert.EqualError(t, err, "eval error")
}

func TestRedisUnlock_EvalReturnsZero(t *testing.T) {
	mockClient := &mocks.MockRedisClient{}
	mockRedis := &mockRedis{client: mockClient}
	mockRedis.On("GetClient").Return(mockClient)
	locker, _ := GetDistributedLocker(&LockOptions{LockerProvider: RedisLockProvider, StorageClient: mockRedis})

	locker.(*RedisLocker).lockStore.Store("unlockkey", "lockuuid")

	cmd := redisv9.NewCmd(context.Background())
	cmd.SetVal(int64(0))
	mockClient.On("Eval", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(cmd)

	err := locker.(*RedisLocker).unlock(context.Background(), "unlockkey")
	assert.Equal(t, ErrLockNotHeld, err)
	_, ok := locker.(*RedisLocker).lockStore.Load("unlockkey")
	assert.False(t, ok)
}

func TestRedisUnlock_StaleOwnerDeletesLocalState(t *testing.T) {
	mockClient := &mocks.MockRedisClient{}
	mockRedis := &mockRedis{client: mockClient}
	mockRedis.On("GetClient").Return(mockClient)
	locker, _ := GetDistributedLocker(&LockOptions{LockerProvider: RedisLockProvider, StorageClient: mockRedis})

	locker.(*RedisLocker).lockStore.Store("unlockkey", redisLockState{value: "1:old-owner", token: 1})

	cmd := redisv9.NewCmd(context.Background())
	cmd.SetVal(int64(0))
	mockClient.On("Eval", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(cmd)

	err := locker.(*RedisLocker).unlock(context.Background(), "unlockkey")

	assert.Equal(t, ErrLockNotHeld, err)
	_, ok := locker.(*RedisLocker).lockStore.Load("unlockkey")
	assert.False(t, ok)
}

func TestRedisUnlock_UnexpectedEvalType(t *testing.T) {
	mockClient := &mocks.MockRedisClient{}
	mockRedis := &mockRedis{client: mockClient}
	mockRedis.On("GetClient").Return(mockClient)
	locker, _ := GetDistributedLocker(&LockOptions{LockerProvider: RedisLockProvider, StorageClient: mockRedis})

	locker.(*RedisLocker).lockStore.Store("unlockkey", "lockuuid")

	cmd := redisv9.NewCmd(context.Background())
	cmd.SetVal("not-int64")
	mockClient.On("Eval", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(cmd)

	err := locker.(*RedisLocker).unlock(context.Background(), "unlockkey")
	assert.EqualError(t, err, "unexpected result type from Eval")
}

func TestRedisExtend_InvalidExtension(t *testing.T) {
	locker := &RedisLocker{}

	err := locker.extend(context.Background(), "extendkey", 0)

	assert.EqualError(t, err, "lock extension must be positive")
}

func TestRedisExtend_InvalidKey(t *testing.T) {
	locker := &RedisLocker{}

	err := locker.extend(context.Background(), " ", time.Second)

	assert.EqualError(t, err, "lock key cannot be empty")
}

func TestRedisExtend_InvalidLocalState(t *testing.T) {
	locker := &RedisLocker{}
	locker.lockStore.Store("extendkey", 123)

	err := locker.extend(context.Background(), "extendkey", time.Second)

	assert.Equal(t, ErrLockNotHeld, err)
}

func TestRedisExtend_Success(t *testing.T) {
	mockClient := &mocks.MockRedisClient{}
	mockRedis := &mockRedis{client: mockClient}
	mockRedis.On("GetClient").Return(mockClient)
	locker, _ := GetDistributedLocker(&LockOptions{LockerProvider: RedisLockProvider, StorageClient: mockRedis})
	locker.(*RedisLocker).lockStore.Store("extendkey", "lockuuid")

	mockCmd := redisv9.NewCmdResult(int64(1), nil)
	mockClient.On("Eval", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(mockCmd)

	ctx := context.Background()
	err := locker.(*RedisLocker).extend(ctx, "extendkey", 10*time.Second)
	assert.NoError(t, err)
}

func TestRedisExtend_LockNotHeld(t *testing.T) {
	mockClient := &mocks.MockRedisClient{}
	mockRedis := &mockRedis{client: mockClient}
	locker, _ := GetDistributedLocker(&LockOptions{LockerProvider: RedisLockProvider, StorageClient: mockRedis})

	ctx := context.Background()
	err := locker.(*RedisLocker).extend(ctx, "unknown", 10*time.Second)
	assert.Equal(t, ErrLockNotHeld, err)
}

func TestRedisExtend_EvalFails(t *testing.T) {
	mockClient := &mocks.MockRedisClient{}
	mockRedis := &mockRedis{client: mockClient}
	mockRedis.On("GetClient").Return(mockClient)

	locker, _ := GetDistributedLocker(&LockOptions{LockerProvider: RedisLockProvider, StorageClient: mockRedis})
	locker.((*RedisLocker)).lockStore.Store("extendkey", "lockuuid")

	mockClient.On("Eval", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil, errors.New("eval error"))

	ctx := context.Background()
	err := locker.(*RedisLocker).extend(ctx, "extendkey", 10*time.Second)

	assert.EqualError(t, err, "internal error in redis command")
}

func TestRedisExtend_Errors(t *testing.T) {
	mockClient := &mocks.MockRedisClient{}
	mockRedis := &mockRedis{client: mockClient}
	mockRedis.On("GetClient").Return(mockClient)

	locker, _ := GetDistributedLocker(&LockOptions{LockerProvider: RedisLockProvider, StorageClient: mockRedis})
	locker.(*RedisLocker).lockStore.Store("extendkey", "lockuuid")

	t.Run("ResultError", func(t *testing.T) {
		cmd := redisv9.NewCmd(context.Background())
		cmd.SetErr(errors.New("result failed"))

		mockClient.On("Eval", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(cmd).Once()

		err := locker.(*RedisLocker).extend(context.Background(), "extendkey", 10*time.Second)
		assert.EqualError(t, err, "result failed")
	})

	t.Run("WrongType", func(t *testing.T) {
		cmd := redisv9.NewCmd(context.Background())
		cmd.SetVal("not-int64")

		mockClient.On("Eval", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(cmd).Once()

		err := locker.(*RedisLocker).extend(context.Background(), "extendkey", 10*time.Second)
		assert.EqualError(t, err, "unexpected result type from Eval")
	})

	t.Run("ZeroReturnValue", func(t *testing.T) {
		locker.(*RedisLocker).lockStore.Store("extendkey", "lockuuid")
		cmd := redisv9.NewCmd(context.Background())
		cmd.SetVal(int64(0))

		mockClient.On("Eval", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(cmd).Once()

		err := locker.(*RedisLocker).extend(context.Background(), "extendkey", 10*time.Second)
		assert.Equal(t, ErrLockNotHeld, err)
		_, ok := locker.(*RedisLocker).lockStore.Load("extendkey")
		assert.False(t, ok)
	})
}

func TestRedisExtend_StaleOwnerDeletesLocalState(t *testing.T) {
	mockClient := &mocks.MockRedisClient{}
	mockRedis := &mockRedis{client: mockClient}
	mockRedis.On("GetClient").Return(mockClient)
	locker, _ := GetDistributedLocker(&LockOptions{LockerProvider: RedisLockProvider, StorageClient: mockRedis})

	locker.(*RedisLocker).lockStore.Store("extendkey", redisLockState{value: "1:old-owner", token: 1})

	cmd := redisv9.NewCmd(context.Background())
	cmd.SetVal(int64(0))
	mockClient.On("Eval", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(cmd)

	err := locker.(*RedisLocker).extend(context.Background(), "extendkey", time.Second)

	assert.Equal(t, ErrLockNotHeld, err)
	_, ok := locker.(*RedisLocker).lockStore.Load("extendkey")
	assert.False(t, ok)
}

func TestRedisLockKeyHelpers(t *testing.T) {
	holderKey := redisLockKey("resource")
	fenceKey := fencingCounterKey("resource")

	assert.Contains(t, holderKey, "{photon-lock:")
	assert.Contains(t, holderKey, "}:holder")
	assert.Contains(t, fenceKey, "{photon-lock:")
	assert.Contains(t, fenceKey, "}:fence")
	assert.NotEqual(t, holderKey, fenceKey)
	assert.Equal(t, holderKey[:78], fenceKey[:78])
	assert.NotContains(t, holderKey, "resource")
}

func TestJitterLockRetryInterval(t *testing.T) {
	assert.Equal(t, time.Nanosecond, jitterLockRetryInterval(time.Nanosecond))

	delay := jitterLockRetryInterval(100 * time.Millisecond)
	assert.GreaterOrEqual(t, delay, 90*time.Millisecond)
	assert.LessOrEqual(t, delay, 110*time.Millisecond)
}
