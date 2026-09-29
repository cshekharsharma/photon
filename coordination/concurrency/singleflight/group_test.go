package singleflight

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDoConcurrentSameKeyRunsOnce(t *testing.T) {
	group := NewGroup()
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	var closeStarted sync.Once

	const waiters = 8
	var wg sync.WaitGroup
	values := make([]any, waiters)
	shared := make([]bool, waiters)
	errs := make([]error, waiters)
	begin := make(chan struct{})

	for i := range waiters {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			<-begin
			values[index], shared[index], errs[index] = group.Do(context.Background(), "key", func(context.Context) (any, error) {
				calls.Add(1)
				closeStarted.Do(func() { close(started) })
				<-release
				return "value", nil
			})
		}(i)
	}

	close(begin)
	<-started
	time.Sleep(20 * time.Millisecond)
	close(release)
	wg.Wait()

	assert.Equal(t, int32(1), calls.Load())
	for i := range waiters {
		assert.NoError(t, errs[i])
		assert.Equal(t, "value", values[i])
		assert.True(t, shared[i])
	}
}

func TestDoDifferentKeysRunIndependently(t *testing.T) {
	group := NewGroup()
	var calls atomic.Int32

	for _, key := range []string{"a", "b", "c"} {
		value, shared, err := group.Do(context.Background(), key, func(context.Context) (any, error) {
			calls.Add(1)
			return key, nil
		})

		assert.NoError(t, err)
		assert.False(t, shared)
		assert.Equal(t, key, value)
	}
	assert.Equal(t, int32(3), calls.Load())
}

func TestDoSharesErrors(t *testing.T) {
	group := NewGroup()
	started := make(chan struct{})
	release := make(chan struct{})
	boom := errors.New("boom")
	var calls atomic.Int32
	var closeStarted sync.Once

	var wg sync.WaitGroup
	errs := make([]error, 2)
	shared := make([]bool, 2)

	for i := range 2 {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			_, shared[index], errs[index] = group.Do(context.Background(), "key", func(context.Context) (any, error) {
				calls.Add(1)
				closeStarted.Do(func() { close(started) })
				<-release
				return nil, boom
			})
		}(i)
	}

	<-started
	time.Sleep(20 * time.Millisecond)
	close(release)
	wg.Wait()

	assert.Equal(t, int32(1), calls.Load())
	for i := range 2 {
		assert.ErrorIs(t, errs[i], boom)
		assert.True(t, shared[i])
	}
}

func TestDoWaiterContextCancellationReturnsEarly(t *testing.T) {
	group := NewGroup()
	started := make(chan struct{})
	release := make(chan struct{})
	leaderDone := make(chan error, 1)
	var calls atomic.Int32

	go func() {
		value, _, err := group.Do(context.Background(), "key", func(context.Context) (any, error) {
			calls.Add(1)
			close(started)
			<-release
			return "leader", nil
		})
		if value != "leader" {
			leaderDone <- errors.New("unexpected leader value")
			return
		}
		leaderDone <- err
	}()

	<-started
	waiterCtx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	value, shared, err := group.Do(waiterCtx, "key", func(context.Context) (any, error) {
		calls.Add(1)
		return "waiter", nil
	})

	assert.Nil(t, value)
	assert.False(t, shared)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	close(release)
	assert.NoError(t, <-leaderDone)
	assert.Equal(t, int32(1), calls.Load())
}

func TestDoLeaderContextCancellationPropagates(t *testing.T) {
	group := NewGroup()
	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	started := make(chan struct{})
	leaderErr := make(chan error, 1)
	waiterErr := make(chan error, 1)

	go func() {
		_, _, err := group.Do(leaderCtx, "key", func(ctx context.Context) (any, error) {
			close(started)
			<-ctx.Done()
			return nil, ctx.Err()
		})
		leaderErr <- err
	}()

	<-started
	go func() {
		_, _, err := group.Do(context.Background(), "key", func(context.Context) (any, error) {
			return "waiter", nil
		})
		waiterErr <- err
	}()

	time.Sleep(20 * time.Millisecond)
	cancelLeader()

	assert.ErrorIs(t, <-leaderErr, context.Canceled)
	assert.ErrorIs(t, <-waiterErr, context.Canceled)
}

func TestForgetAllowsFreshCall(t *testing.T) {
	group := NewGroup()
	release := make(chan struct{})
	started := make(chan struct{}, 2)
	var calls atomic.Int32

	call := func(ctx context.Context) (any, error) {
		callNumber := calls.Add(1)
		started <- struct{}{}
		<-release
		return callNumber, nil
	}

	first := make(chan any, 1)
	go func() {
		value, _, _ := group.Do(context.Background(), "key", call)
		first <- value
	}()
	<-started

	group.Forget("key")

	second := make(chan any, 1)
	go func() {
		value, _, _ := group.Do(context.Background(), "key", call)
		second <- value
	}()
	<-started

	close(release)
	assert.ElementsMatch(t, []any{int32(1), int32(2)}, []any{<-first, <-second})
}

func TestValidationErrors(t *testing.T) {
	group := NewGroup()
	var nilGroup *Group

	_, _, err := nilGroup.Do(context.Background(), "key", func(context.Context) (any, error) {
		return "value", nil
	})
	assert.ErrorIs(t, err, ErrNilGroup)

	var nilCtx context.Context
	_, _, err = group.Do(nilCtx, "key", func(context.Context) (any, error) {
		return "value", nil
	})
	assert.ErrorIs(t, err, ErrNilContext)

	_, _, err = group.Do(context.Background(), " \t\n", func(context.Context) (any, error) {
		return "value", nil
	})
	assert.ErrorIs(t, err, ErrEmptyKey)

	_, _, err = group.Do(context.Background(), "key", nil)
	assert.ErrorIs(t, err, ErrNilFunction)

	_, _, err = DoTyped(context.Background(), nilGroup, "key", func(context.Context) (string, error) {
		return "value", nil
	})
	assert.ErrorIs(t, err, ErrNilGroup)

	_, _, err = DoTyped[string](context.Background(), group, "key", nil)
	assert.ErrorIs(t, err, ErrNilFunction)

	group.Forget("")
	nilGroup.Forget("key")
}

func TestDoTypedReturnsTypedValue(t *testing.T) {
	group := NewGroup()

	value, shared, err := DoTyped(context.Background(), group, "key", func(context.Context) (int, error) {
		return 42, nil
	})

	assert.NoError(t, err)
	assert.False(t, shared)
	assert.Equal(t, 42, value)
}

func TestDoTypedReturnsNilPointer(t *testing.T) {
	type record struct {
		ID int
	}

	group := NewGroup()

	value, shared, err := DoTyped(context.Background(), group, "key", func(context.Context) (*record, error) {
		return nil, nil
	})

	assert.NoError(t, err)
	assert.False(t, shared)
	assert.Nil(t, value)
}

func TestDoTypedRejectsRawInFlightResult(t *testing.T) {
	group := NewGroup()
	started := make(chan struct{})
	release := make(chan struct{})
	type typedOutcome struct {
		value  int
		shared bool
		err    error
	}
	outcome := make(chan typedOutcome, 1)

	go func() {
		_, _, _ = group.Do(context.Background(), "key", func(context.Context) (any, error) {
			close(started)
			<-release
			return "raw", nil
		})
	}()

	<-started
	go func() {
		value, shared, err := DoTyped(context.Background(), group, "key", func(context.Context) (int, error) {
			return 1, nil
		})
		outcome <- typedOutcome{value: value, shared: shared, err: err}
	}()
	time.Sleep(20 * time.Millisecond)
	close(release)
	got := <-outcome

	assert.Zero(t, got.value)
	assert.True(t, got.shared)
	assert.ErrorIs(t, got.err, ErrTypeMismatch)
}

func TestDoRejectsTypedInFlightResult(t *testing.T) {
	group := NewGroup()
	started := make(chan struct{})
	release := make(chan struct{})
	type rawOutcome struct {
		value  any
		shared bool
		err    error
	}
	outcome := make(chan rawOutcome, 1)

	go func() {
		_, _, _ = DoTyped(context.Background(), group, "key", func(context.Context) (int, error) {
			close(started)
			<-release
			return 1, nil
		})
	}()

	<-started
	go func() {
		value, shared, err := group.Do(context.Background(), "key", func(context.Context) (any, error) {
			return "raw", nil
		})
		outcome <- rawOutcome{value: value, shared: shared, err: err}
	}()
	time.Sleep(20 * time.Millisecond)
	close(release)
	got := <-outcome

	assert.Nil(t, got.value)
	assert.True(t, got.shared)
	assert.ErrorIs(t, got.err, ErrTypeMismatch)
}

func TestDoTypedRejectsDifferentTypedInFlightResult(t *testing.T) {
	group := NewGroup()
	started := make(chan struct{})
	release := make(chan struct{})
	type typedOutcome struct {
		value  string
		shared bool
		err    error
	}
	outcome := make(chan typedOutcome, 1)

	go func() {
		_, _, _ = DoTyped(context.Background(), group, "key", func(context.Context) (int, error) {
			close(started)
			<-release
			return 1, nil
		})
	}()

	<-started
	go func() {
		value, shared, err := DoTyped(context.Background(), group, "key", func(context.Context) (string, error) {
			return "value", nil
		})
		outcome <- typedOutcome{value: value, shared: shared, err: err}
	}()
	time.Sleep(20 * time.Millisecond)
	close(release)
	got := <-outcome

	assert.Empty(t, got.value)
	assert.True(t, got.shared)
	assert.ErrorIs(t, got.err, ErrTypeMismatch)
}

func TestDoTypedSharesDuplicateTypedCall(t *testing.T) {
	group := NewGroup()
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	var closeStarted sync.Once

	var wg sync.WaitGroup
	values := make([]int, 2)
	shared := make([]bool, 2)
	errs := make([]error, 2)

	for i := range 2 {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			values[index], shared[index], errs[index] = DoTyped(context.Background(), group, "key", func(context.Context) (int, error) {
				calls.Add(1)
				closeStarted.Do(func() { close(started) })
				<-release
				return 99, nil
			})
		}(i)
	}

	<-started
	time.Sleep(20 * time.Millisecond)
	close(release)
	wg.Wait()

	assert.Equal(t, int32(1), calls.Load())
	for i := range 2 {
		require.NoError(t, errs[i])
		assert.Equal(t, 99, values[i])
		assert.True(t, shared[i])
	}
}
