package stdlib

// Hasher converts a key to a 64‑bit hash suitable for the sketch.
// Callers can provide Hash64String/Hash64Bytes or their own function.
type Hasher[K any] func(K) uint64

// TinyLFUOptions configures a TinyLFU instance.
//
// Recommended defaults:
//
//	Depth:       4
//	Width:       1<<16 (per row)
//	AgingEvery:  50_000 .. 500_000 (workload dependent)
//	AdmitOnEqual: true (admit newcomer when frequencies tie)
type TinyLFUOptions[K any] struct {
	Depth        int
	Width        uint64
	AgingEvery   uint64
	AdmitOnEqual bool
	Hash         Hasher[K]
}

// TinyLFU implements the admission test using a Count‑Min Sketch. It is NOT
// internally synchronized; coordinate access externally as needed (e.g., with
// shard‑level locks in your cache engine).
type TinyLFU[K any] struct {
	cms       *CountMinSketch
	hash      Hasher[K]
	admitOnEq bool
}

// NewTinyLFU constructs a TinyLFU using TinyLFUOptions. Hash is required; panics if nil.
func NewTinyLFU[K any](opt TinyLFUOptions[K]) (*TinyLFU[K], error) {
	if opt.Hash == nil {
		panic("tinylfu: nil hash")
	}

	cms, err := NewCountMinSketch(opt.Depth, opt.Width, opt.AgingEvery)
	if err != nil {
		return nil, err
	}

	lfu := &TinyLFU[K]{
		cms:       cms,
		hash:      opt.Hash,
		admitOnEq: opt.AdmitOnEqual,
	}

	// If Depth/Width/Aging defaults were zero, NewCountMinSketch would have erred;
	// no implicit defaults here.
	return lfu, nil
}

// Record registers an access for key (hit or miss). Call this on every request,
// whether the key is currently in the cache or not. This is how TinyLFU learns
// popularity and protects the cache from one‑hit wonders.
func (t *TinyLFU[K]) Record(key K) {
	t.cms.Increment(t.hash(key))
}

// RecordN registers n accesses for key. Useful when you want to batch multiple
// observations at once (e.g., aggregating counters from another system).
func (t *TinyLFU[K]) RecordN(key K, n uint32) {
	if n == 0 {
		return
	}
	h := t.hash(key)
	for i := uint32(0); i < n; i++ {
		t.cms.Increment(h)
	}
}

// Estimate returns the sketch’s current frequency estimate for key.
func (t *TinyLFU[K]) Estimate(key K) uint32 {
	return t.cms.Estimate(t.hash(key))
}

// VictimFreq is a convenience helper that reads the current frequency estimate
// for a would‑be victim key.
func (t *TinyLFU[K]) VictimFreq(victim K) uint32 {
	return t.cms.Estimate(t.hash(victim))
}

// ShouldAdmit compares the estimated frequency of the incoming key against that
// of the victim key. It returns true if the policy decides to admit the
// incoming item (i.e., evict the victim) and false if the newcomer should be
// rejected. When AdmitOnEqual is true, ties admit the newcomer.
func (t *TinyLFU[K]) ShouldAdmit(incoming, victim K) bool {
	fi := t.cms.Estimate(t.hash(incoming))
	fv := t.cms.Estimate(t.hash(victim))

	if t.admitOnEq {
		return fi >= fv
	}

	return fi > fv
}

// ShouldAdmitAgainst compares the incoming key’s frequency against an explicit
// victim frequency (useful when you already fetched the victim’s frequency or
// when the exact victim key is unknown). Honors AdmitOnEqual.
func (t *TinyLFU[K]) ShouldAdmitAgainst(incoming K, victimFreq uint32) bool {
	fi := t.cms.Estimate(t.hash(incoming))
	if t.admitOnEq {
		return fi >= victimFreq
	}
	return fi > victimFreq
}

// Age halves all counters, biasing the sketch toward recent history. Use when
// you manage aging externally. If you configured AgingEvery>0, aging will also
// occur automatically within Record/RecordN.
func (t *TinyLFU[K]) Age() {
	t.cms.Age()
}

// Reset zeroes all counters and the op counter. Useful for tests or hard resets.
func (t *TinyLFU[K]) Reset() {
	t.cms.Reset()
}
