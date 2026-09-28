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

	token, err := locker.Lock(context.TODO(), "mylock", time.Second, time.Millisecond*10)
	assert.NoError(t, err)
	assert.Equal(t, uint64(7), token)

	storedToken, ok := locker.FencingToken("mylock")
	assert.True(t, ok)
	assert.Equal(t, uint64(7), storedToken)
}

func TestRedisLock_ReturnsFencingToken(t *testing.T) {
	mockClient := &mocks.MockRedisClient{}
	mockRedis := &mockRedis{client: mockClient}
	mockRedis.On("GetClient").Return(mockClient)
	locker, _ := GetDistributedLocker(&LockOptions{LockerProvider: RedisLockProvider, StorageClient: mockRedis})

	mockCmd := redisv9.NewCmdResult(int64(11), nil)
	mockClient.On("Eval", mock.Anything, mock.Anything, []string{"mylock", "mylock:fence"}, mock.Anything).Return(mockCmd)

	token, err := locker.Lock(context.Background(), "mylock", time.Second+time.Nanosecond, time.Millisecond*10)
	assert.NoError(t, err)
	assert.Equal(t, uint64(11), token)
}

func TestRedisLock_ReacquireUpdatesFencingToken(t *testing.T) {
	mockClient := &mocks.MockRedisClient{}
	mockRedis := &mockRedis{client: mockClient}
	mockRedis.On("GetClient").Return(mockClient)
	locker, _ := GetDistributedLocker(&LockOptions{LockerProvider: RedisLockProvider, StorageClient: mockRedis})

	mockClient.On("Eval", mock.Anything, mock.Anything, []string{"resource", "resource:fence"}, mock.Anything).
		Return(redisv9.NewCmdResult(int64(1), nil)).
		Once()
	mockClient.On("Eval", mock.Anything, mock.Anything, []string{"resource", "resource:fence"}, mock.Anything).
		Return(redisv9.NewCmdResult(int64(2), nil)).
		Once()

	token, err := locker.Lock(context.Background(), "resource", time.Second, time.Millisecond)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), token)

	token, err = locker.Lock(context.Background(), "resource", time.Second, time.Millisecond)
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), token)

	storedToken, ok := locker.FencingToken("resource")
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
	token, err := locker.Lock(ctx, "mylock", time.Second, time.Millisecond*10)
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

	token, err := locker.Lock(ctx, "lockfail", time.Second, 10*time.Millisecond)
	assert.Zero(t, token)
	assert.Equal(t, ErrLockNotAcquired, err)
}

func TestRedisLock_InvalidExpiry(t *testing.T) {
	locker := &RedisLocker{}

	token, err := locker.Lock(context.Background(), "badlock", 0, time.Millisecond)

	assert.Zero(t, token)
	assert.EqualError(t, err, "lock expiry must be positive")
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

	token, err := locker.Lock(context.Background(), "lockfail", time.Second, 0)

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

	token, err := locker.Lock(context.Background(), "lockfail", time.Second, time.Millisecond)

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

	token, err := locker.Lock(context.Background(), "mylock", time.Second, time.Millisecond)

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

	token, err := locker.Lock(context.Background(), "mylock", time.Second, time.Millisecond)

	assert.Zero(t, token)
	assert.EqualError(t, err, "distributed lock acquisition failed for key 'mylock': unexpected negative fencing token")
}

func TestRedisFencingToken_NotHeldOrLegacyValue(t *testing.T) {
	locker := &RedisLocker{}

	token, ok := locker.FencingToken("missing")
	assert.False(t, ok)
	assert.Zero(t, token)

	locker.lockStore.Store("legacy", "lockuuid")
	token, ok = locker.FencingToken("legacy")
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

	err := locker.Unlock(context.Background(), "unlockkey")

	assert.Equal(t, ErrLockNotHeld, err)
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
	err := locker.Unlock(ctx, "unlockkey")
	assert.NoError(t, err)
}

func TestRedisUnlock_LockNotHeld(t *testing.T) {
	mockClient := &mocks.MockRedisClient{}
	mockRedis := &mockRedis{client: mockClient}
	locker, _ := GetDistributedLocker(&LockOptions{LockerProvider: RedisLockProvider, StorageClient: mockRedis})

	ctx := context.Background()
	err := locker.Unlock(ctx, "nonexistent")
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
	err := locker.Unlock(ctx, "unlockkey")
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

	err := locker.Unlock(context.Background(), "unlockkey")
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

	err := locker.Unlock(context.Background(), "unlockkey")

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

	err := locker.Unlock(context.Background(), "unlockkey")
	assert.EqualError(t, err, "unexpected result type from Eval")
}

func TestRedisExtend_InvalidExtension(t *testing.T) {
	locker := &RedisLocker{}

	err := locker.Extend(context.Background(), "extendkey", 0)

	assert.EqualError(t, err, "lock extension must be positive")
}

func TestRedisExtend_InvalidLocalState(t *testing.T) {
	locker := &RedisLocker{}
	locker.lockStore.Store("extendkey", 123)

	err := locker.Extend(context.Background(), "extendkey", time.Second)

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
	err := locker.Extend(ctx, "extendkey", 10*time.Second)
	assert.NoError(t, err)
}

func TestRedisExtend_LockNotHeld(t *testing.T) {
	mockClient := &mocks.MockRedisClient{}
	mockRedis := &mockRedis{client: mockClient}
	locker, _ := GetDistributedLocker(&LockOptions{LockerProvider: RedisLockProvider, StorageClient: mockRedis})

	ctx := context.Background()
	err := locker.Extend(ctx, "unknown", 10*time.Second)
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
	err := locker.Extend(ctx, "extendkey", 10*time.Second)

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

		err := locker.Extend(context.Background(), "extendkey", 10*time.Second)
		assert.EqualError(t, err, "result failed")
	})

	t.Run("WrongType", func(t *testing.T) {
		cmd := redisv9.NewCmd(context.Background())
		cmd.SetVal("not-int64")

		mockClient.On("Eval", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(cmd).Once()

		err := locker.Extend(context.Background(), "extendkey", 10*time.Second)
		assert.EqualError(t, err, "unexpected result type from Eval")
	})

	t.Run("ZeroReturnValue", func(t *testing.T) {
		locker.(*RedisLocker).lockStore.Store("extendkey", "lockuuid")
		cmd := redisv9.NewCmd(context.Background())
		cmd.SetVal(int64(0))

		mockClient.On("Eval", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(cmd).Once()

		err := locker.Extend(context.Background(), "extendkey", 10*time.Second)
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

	err := locker.Extend(context.Background(), "extendkey", time.Second)

	assert.Equal(t, ErrLockNotHeld, err)
	_, ok := locker.(*RedisLocker).lockStore.Load("extendkey")
	assert.False(t, ok)
}
