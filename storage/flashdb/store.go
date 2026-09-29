package flashdb

// A high-performance, concurrency-safe, in-process key-value store for Photon.
//
// This rewrite enforces **byte-based capacity** using your reflection-based
// size estimator and **TinyLFU** admission, with **LRU** eviction order.
//
// Design
//  - Sharded hashmap (per-shard RWMutex) for low contention
//  - TinyLFU (global) for admission on insert when budget is tight
//  - LRU (per shard) for eviction order
//  - Strict budget by bytes: key + record overhead + deep value size
//  - TTL: lazy expiration on access + periodic janitor sweep
//  - No I/O under locks; eviction callbacks should be non-blocking upstream
//
// Notes
//  - We compute cost **once** per Set/Update and store it; never recompute on Get
//  - We wrap the deep-size function with panic protection and sane fallbacks

import (
	"errors"
	"hash/fnv"
	"math"
	"sync"
	"time"

	"github.com/cshekharsharma/photon/utils/stdlib"
)

// Store satisfies caching.Cache (see compile-time check below).
//
// We shard the map; each shard maintains its own LRU and cost accounting.
// TinyLFU is shared to keep a global popularity signal (simple and effective).
type Store struct {
	shards []*shard
	opts   Options
	lfu    *stdlib.TinyLFU[string]
	stopCh chan struct{}
	once   sync.Once
}

var (
	existsBeforeWriteLockHook = func() {}
	ttlRemainingSecondsHook   = func(expiry time.Time) int64 {
		return int64(time.Until(expiry).Seconds())
	}
	maxDeletePerShardHook = func() uint16 { return maxDeletePerShard }
)

// New constructs a Store with sane defaults. MaxCostBytes must be > 0.
func New(opts Options) (*Store, error) {
	if opts.Shards <= 0 {
		opts.Shards = ShardCount
	}

	if opts.JanitorEvery == 0 {
		opts.JanitorEvery = time.Minute
	}

	if opts.LFUDepth == 0 {
		opts.LFUDepth = 4
	}

	if opts.LFUWidth == 0 {
		opts.LFUWidth = 1 << 16
	}

	if opts.LFUAgingEvery == 0 {
		opts.LFUAgingEvery = 200_000
	}

	lfu, err := stdlib.NewTinyLFU(stdlib.TinyLFUOptions[string]{
		Depth:        opts.LFUDepth,
		Width:        opts.LFUWidth,
		AgingEvery:   opts.LFUAgingEvery,
		AdmitOnEqual: true,
		Hash:         stdlib.Hash64String,
	})

	if err != nil {
		return nil, err
	}

	if opts.MaxStorageSize == 0 {
		return nil, errors.New("flashdb: MaxStorageSize must be > 0")
	}

	perShard := opts.MaxStorageSize / uint64(opts.Shards)

	shards := make([]*shard, opts.Shards)
	for i := range shards {
		shards[i] = &shard{
			data:     make(map[string]*record, 256),
			lru:      stdlib.NewLRU[string, struct{}](0, nil),
			capacity: perShard,
		}
	}

	st := &Store{
		shards: shards,
		opts:   opts,
		lfu:    lfu,
		stopCh: make(chan struct{}),
	}

	if opts.JanitorEvery > 0 {
		go st.janitor()
	}

	return st, nil
}

// Close stops the janitor.
func (s *Store) Close() {
	s.once.Do(func() {
		close(s.stopCh)
	})
}

// shardFor returns the shard for a given key.
func (s *Store) shardFor(key string) *shard {
	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(key))

	shardIndex := hasher.Sum32() % uint32(len(s.shards)) // #nosec G115 -- shard count is validated positive and bounded by memory allocation.
	return s.shards[shardIndex]
}

// Exists checks if a key exists and is not expired. Does not update recency or frequency.
// Returns false for missing, expired, or invalid keys.
func (s *Store) Exists(key string) (bool, error) {
	if key == "" {
		return false, ErrInvalidKey
	}

	shard := s.shardFor(key)
	now := time.Now()

	shard.mu.RLock()

	record, ok := shard.data[key]
	expired := ok && record.isExpired(now)

	shard.mu.RUnlock()

	if !ok {
		return false, nil
	}

	if !expired {
		return true, nil
	}

	existsBeforeWriteLockHook()
	shard.mu.Lock()
	if record2, ok2 := shard.data[key]; ok2 && record2.isExpired(now) {
		shard.removeLocked(key)
		shard.mu.Unlock()
		return false, nil
	}

	shard.mu.Unlock()
	return true, nil // someone refreshed TTL between RUnlock and Lock
}

// Get fetches a value by its (already-composed) key.
// Lazy-expires, bumps LRU recency, and records TinyLFU access.
// Returns ErrNotFound for missing or expired keys.
func (s *Store) Get(key string) (any, error) {
	if key == "" {
		return nil, ErrInvalidKey
	}

	shard := s.shardFor(key)
	now := time.Now()

	shard.mu.Lock()

	record, ok := shard.data[key]
	if !ok || record.isExpired(now) {
		if ok {
			shard.removeLocked(key)
		}

		shard.mu.Unlock()
		return nil, ErrNotFound
	}

	// Bump recency and record frequency.
	shard.lru.Get(key)
	if s.lfu != nil { // safe if LFU can be disabled in some builds
		s.lfu.Record(key)
	}

	val := clone(record.value)
	shard.mu.Unlock()
	return val, nil
}

// Set stores/updates a value under key with an optional TTL.
// Returns true if stored, false if rejected by TinyLFU admission.
// Enforces per-shard byte budget using key+record size.
//
// Notes:
//   - ttl==0 uses s.opts.DefaultTTL (if > 0). Adjust unit mapping as per your option.
//   - Uses your deep-size estimator; we add a small record overhead to approximate metadata.
//   - All mutations happen under the shard write lock.
func (s *Store) Set(key string, value map[string]any, ttl time.Duration) (bool, error) {
	if key == "" {
		return false, ErrInvalidKey
	}

	shard := s.shardFor(key)
	now := time.Now()

	if ttl == 0 && s.opts.DefaultTTL > 0 {
		ttl = s.opts.DefaultTTL
	}

	var exp time.Time
	if ttl > 0 {
		exp = now.Add(ttl)
	}

	newSize := computeSize(key, value)

	shard.mu.Lock()
	defer shard.mu.Unlock()

	// TinyLFU: record the access (miss/write)
	if s.lfu != nil {
		s.lfu.Record(key)
	}

	// Update existing
	if record, ok := shard.data[key]; ok {
		delta := uint64(0)
		if newSize >= 0 {
			old := record.size

			if newSize >= old {
				delta = nonNegativeInt64ToUint64(newSize - old)
				shard.currentSize += delta
			} else {
				shard.currentSize -= nonNegativeInt64ToUint64(old - newSize)
			}
		}

		record.value = clone(value)
		record.size = newSize
		record.expiry = exp
		shard.lru.Get(key)

		// If an external cap shrink or large growth pushed us over budget, trim.
		for shard.isFull() {
			vk, ok := shard.tailKeyLocked()
			if !ok {
				break
			}

			shard.removeLocked(vk) // For updates we trim unconditionally (we're already over budget).
		}

		return true, nil
	}

	newCost := nonNegativeInt64ToUint64(newSize)

	// Reject items that can never fit this shard alone.
	if newCost > shard.capacity {
		return false, nil
	}

	// While we don't have room, compare admission vs. LRU victim and evict if admitted.
	for shard.isFullAfter(newCost) {
		vk, ok := shard.tailKeyLocked()

		if !ok { // No victim to evict; bail
			return false, nil
		}

		if s.lfu != nil && !s.lfu.ShouldAdmit(key, vk) {
			// Incoming is colder than victim → reject admission
			return false, nil
		}

		shard.removeLocked(vk)
	}

	// Insert
	shard.data[key] = &record{value: clone(value), size: newSize, expiry: exp}
	shard.currentSize += newCost
	shard.lru.Set(key, struct{}{})

	return true, nil
}

// Delete removes a key if it exists. Returns true if the key existed and was removed.
// Returns false if the key was missing or invalid. Does not error if the key is missing.
func (s *Store) Delete(key string) (bool, error) {
	if key == "" {
		return false, ErrInvalidKey
	}

	sh := s.shardFor(key)

	sh.mu.Lock()
	defer sh.mu.Unlock()

	if _, ok := sh.data[key]; !ok {
		return false, nil
	}

	sh.removeLocked(key)
	return true, nil
}

// ----------------------------- Batch ops -----------------------------

// MultiGet returns a map of key -> value for all found (and not-expired) keys.
// Missing or expired keys are silently omitted.
func (s *Store) MultiGet(keys []string) (map[string]any, error) {
	out := make(map[string]any, len(keys))
	if len(keys) == 0 {
		return out, nil
	}

	for _, k := range keys {
		if k == "" {
			continue
		}

		if v, err := s.Get(k); err == nil {
			out[k] = v
		}
	}

	return out, nil
}

// MultiSet sets many keys with the same TTL. Returns per-key success.
// A key is true if it was stored (admitted and within budget); false if
// rejected (e.g., TinyLFU admission fail) or the key was invalid.
func (s *Store) MultiSet(items map[string]map[string]any, ttl time.Duration) (map[string]bool, error) {
	res := make(map[string]bool, len(items))

	if len(items) == 0 {
		return res, nil
	}

	for key, value := range items {
		if key == "" {
			res[key] = false
			continue
		}

		ok, err := s.Set(key, value, ttl)
		// Treat errors (e.g., invalid key) as false for this key; keep going.
		_ = err
		res[key] = ok
	}

	return res, nil
}

// MultiDelete deletes all provided keys. Returns per-key deletion success.
// A key is true if it existed and was removed; false if it was missing or invalid.
func (s *Store) MultiDelete(keys []string) (map[string]bool, error) {
	res := make(map[string]bool, len(keys))

	if len(keys) == 0 {
		return res, nil
	}

	for _, key := range keys {
		if key == "" {
			res[key] = false
			continue
		}

		ok, _ := s.Delete(key) // Delete returns (bool, error); we ignore error to keep batch robust
		res[key] = ok
	}

	return res, nil
}

// ----------------------------- Mutations -----------------------------

// Increment increases numeric fields in the record by given offsets.
// Supported types: int, int32, int64, float64.
// Returns ErrNotFound if key missing/expired, ErrIllegalField if field absent,
// ErrBadType if value type is not numeric.
func (s *Store) Increment(key string, offsets map[string]int64) error {
	if key == "" {
		return ErrInvalidKey
	}

	shard := s.shardFor(key)
	now := time.Now()

	shard.mu.Lock()
	defer shard.mu.Unlock()

	record, ok := shard.data[key]
	if !ok || record.isExpired(now) {
		if ok {
			shard.removeLocked(key)
		}

		return ErrNotFound
	}

	for field, delta := range offsets {
		cur, ok := record.value[field]
		if !ok {
			return ErrIllegalField
		}

		next, ok := addInt(cur, delta)

		if !ok {
			return ErrBadType
		}

		record.value[field] = next
	}

	shard.lru.Get(key)
	if s.lfu != nil {
		s.lfu.Record(key)
	}

	return nil
}

// Decrement decreases numeric fields in the record by given offsets.
// Internally calls Increment with negative values.
func (s *Store) Decrement(key string, offsets map[string]int64) error {
	negativeOffsets := make(map[string]int64, len(offsets))

	for field, delta := range offsets {
		negativeOffsets[field] = -delta
	}

	return s.Increment(key, negativeOffsets)
}

// Append appends strings to existing string fields in the record.
// Returns ErrNotFound if key missing/expired, ErrIllegalField if field absent,
// ErrBadType if value is not a string.
func (s *Store) Append(key string, appends map[string]string) error {
	if key == "" {
		return ErrInvalidKey
	}

	shard := s.shardFor(key)
	now := time.Now()

	shard.mu.Lock()
	record, ok := shard.data[key]

	if !ok || record.isExpired(now) {
		if ok {
			shard.removeLocked(key)
		}

		shard.mu.Unlock()
		return ErrNotFound
	}

	// mutate in-place
	for field, suffix := range appends {
		cur, ok := record.value[field]

		if !ok {
			shard.mu.Unlock()
			return ErrIllegalField
		}

		base, ok := cur.(string)
		if !ok {
			shard.mu.Unlock()
			return ErrBadType
		}

		record.value[field] = base + suffix
	}

	// recompute size delta
	old := record.size
	newSize := computeSize(key, record.value)
	if newSize >= old {
		shard.currentSize += nonNegativeInt64ToUint64(newSize - old)
	} else {
		shard.currentSize -= nonNegativeInt64ToUint64(old - newSize)
	}
	record.size = newSize

	// bump recency/freq
	shard.lru.Get(key)
	if s.lfu != nil {
		s.lfu.Record(key)
	}

	// enforce budget
	for shard.isFull() {
		vk, ok := shard.tailKeyLocked()
		if !ok {
			break
		}
		shard.removeLocked(vk)
	}

	shard.mu.Unlock()
	return nil
}

func nonNegativeInt64ToUint64(v int64) uint64 {
	if v <= 0 {
		return 0
	}
	return uint64(v) // #nosec G115 -- negative values are clamped above.
}

func int64ToInt32(v int64) (int32, bool) {
	if v < math.MinInt32 || v > math.MaxInt32 {
		return 0, false
	}
	return int32(v), true // #nosec G115 -- range checked above.
}

// GetTTL returns the remaining TTL (in seconds) for key.
// Returns -1 if the record has no TTL, and ErrNotFound if the key is missing or expired.
func (s *Store) GetTTL(key string) (int64, error) {
	if key == "" {
		return 0, ErrInvalidKey
	}

	shard := s.shardFor(key)
	now := time.Now()

	// Write lock so we can lazily remove expired records.
	shard.mu.Lock()
	defer shard.mu.Unlock()

	rec, ok := shard.data[key]
	if !ok || rec.isExpired(now) {
		if ok {
			shard.removeLocked(key)
		}

		return 0, ErrNotFound
	}

	if rec.expiry.IsZero() {
		return -1, nil
	}

	secs := ttlRemainingSecondsHook(rec.expiry)
	if secs < 0 {
		// Edge case: just crossed expiry boundary.
		shard.removeLocked(key)
		return 0, ErrNotFound
	}

	return secs, nil
}

// SetTTL updates the TTL for key. ttl<=0 clears TTL.
// Returns ErrNotFound if the key is missing or expired.
func (s *Store) SetTTL(key string, ttl time.Duration) error {
	if key == "" {
		return ErrInvalidKey
	}

	shard := s.shardFor(key)
	now := time.Now()

	shard.mu.Lock()
	defer shard.mu.Unlock()

	rec, ok := shard.data[key]
	if !ok || rec.isExpired(now) {
		if ok {
			shard.removeLocked(key)
		}

		return ErrNotFound
	}

	if ttl <= 0 {
		rec.expiry = time.Time{}
	} else {
		rec.expiry = now.Add(ttl)
	}

	// Treat as a write: bump recency and record frequency.
	shard.lru.Get(key)
	if s.lfu != nil {
		s.lfu.Record(key)
	}

	return nil
}

// janitor periodically scans all shards and removes expired records.
// It runs until the store is closed. The frequency is controlled by
// the JanitorEvery option; if zero, janitor is disabled.
func (s *Store) janitor() {
	ticker := time.NewTicker(s.opts.JanitorEvery)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopCh:
			return

		case now := <-ticker.C:
			for i := range s.shards {
				shard := s.shards[i]
				shard.mu.Lock()
				deleted := 0

				for k, rec := range shard.data {
					if !rec.expiry.IsZero() && now.After(rec.expiry) {
						shard.removeLocked(k)
						deleted++

						if deleted >= int(maxDeletePerShardHook()) {
							break
						}
					}
				}

				shard.mu.Unlock()
			}
		}
	}
}
