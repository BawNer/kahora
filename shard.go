package kahora

import (
	"sync"
	"sync/atomic"
	"unsafe"
)

const (
	// cacheLineSize is the x86-64 cache line size.
	// shard is padded to this boundary to avoid false sharing.
	cacheLineSize = 64
)

// shard is a single cache partition.
// Each shard is independently locked — operations on different shards never contend.
//
// Memory layout is explicit: RWMutex first (most frequently accessed),
// then maps, then atomics, then padding.
// Padding ensures no two shards share a CPU cache line.
type shard[K comparable, V any] struct {
	mu   sync.RWMutex
	data map[K]entry[V]

	// dirty tracks keys mutated (set or deleted) while a shrink is in progress.
	// Allocated once at shard init, cleared between shrink cycles via clear().
	// Only populated when shrinking == true.
	//
	// Note: dirty contains keys that were either written OR deleted during
	// phase 2 of shrink. Phase 3 inspects each dirty key against s.data:
	//   - present in data → copy into new map (delta write)
	//   - absent from data → ensure absent from new map (delta delete)
	dirty map[K]struct{}

	// shrinking signals that dirty tracking is active.
	// Read by set/delete on the hot path — kept as atomic to allow lock-free check.
	// Always mutated under write lock to maintain ordering with dirty map writes.
	shrinking atomic.Bool

	// count is the number of live entries in this shard.
	// Approximate — may be slightly stale under concurrent load.
	// Used for maxEntries enforcement and shrink eligibility fast-path.
	count atomic.Int64

	// Padding to prevent false sharing with adjacent shards in the slice.
	// Verified by TestShardCacheLineAlignment.
	_ [shardPadding]byte
}

// shardPadding is tuned so unsafe.Sizeof(shard{}) is a multiple of cacheLineSize.
// Verified by TestShardCacheLineAlignment — adjust this value if the test fails.
const shardPadding = 8

// newShard allocates a shard with a pre-allocated map of the given hint size.
func newShard[K comparable, V any](initialSize int) *shard[K, V] {
	return &shard[K, V]{
		data:  make(map[K]entry[V], initialSize),
		dirty: make(map[K]struct{}),
	}
}

// get looks up key in the shard.
// now must be the result of a single monoNow() call from the caller —
// avoids redundant clock reads when one Get traverses multiple checks.
//
// Returns (value, true) on hit, (zero, false) on miss or expiry.
// Removes expired entries lazily.
func (s *shard[K, V]) get(key K, now int64, metrics MetricsRecorder, shardIdx int) (V, bool) {
	s.mu.RLock()
	e, ok := s.data[key]
	s.mu.RUnlock()

	if !ok {
		metrics.RecordMiss(shardIdx)
		var zero V
		return zero, false
	}

	if e.isExpired(now) {
		// Lazy eviction: upgrade to write lock and delete.
		// Re-check under write lock — another goroutine may have already deleted
		// or replaced the entry between our RUnlock and Lock.
		evicted := false

		s.mu.Lock()
		if e2, still := s.data[key]; still && e2.isExpired(now) {
			delete(s.data, key)
			s.count.Add(-1)
			if s.shrinking.Load() {
				s.dirty[key] = struct{}{}
			}
			evicted = true
		}
		s.mu.Unlock()

		// Metrics outside the lock — never block writers on metric backends.
		if evicted {
			metrics.RecordLazyEviction(shardIdx)
		}
		metrics.RecordMiss(shardIdx)
		var zero V
		return zero, false
	}

	metrics.RecordHit(shardIdx)
	return e.value, true
}

// set writes key/value into the shard.
// expiresAt == 0 means no TTL.
// Returns ErrCapacityExceeded if the shard is at its per-shard entry limit.
func (s *shard[K, V]) set(key K, value V, expiresAt int64, shardLimit int, metrics MetricsRecorder, shardIdx int) error {
	s.mu.Lock()

	_, exists := s.data[key]
	if !exists && shardLimit > 0 && int(s.count.Load()) >= shardLimit {
		s.mu.Unlock()
		metrics.RecordCapacityExceeded(shardIdx)
		return ErrCapacityExceeded
	}

	s.data[key] = entry[V]{
		value:     value,
		expiresAt: expiresAt,
	}

	if s.shrinking.Load() {
		s.dirty[key] = struct{}{}
	}

	if !exists {
		s.count.Add(1)
	}

	s.mu.Unlock()

	metrics.RecordSet(shardIdx)
	return nil
}

// delete removes key from the shard.
// No-op if key does not exist.
func (s *shard[K, V]) delete(key K, metrics MetricsRecorder, shardIdx int) {
	s.mu.Lock()
	_, exists := s.data[key]
	if exists {
		delete(s.data, key)
		s.count.Add(-1)
		if s.shrinking.Load() {
			// Mark key as mutated so phase 3 of shrink sees the deletion.
			// Without this, a key deleted during phase 2 would survive in newData.
			s.dirty[key] = struct{}{}
		}
	}
	s.mu.Unlock()

	if exists {
		metrics.RecordDelete(shardIdx)
	}
}

// sweepExpired removes all expired entries from the shard.
// Counts evictions locally and records metrics after releasing the lock —
// metric backends never block other writers.
func (s *shard[K, V]) sweepExpired(now int64, metrics MetricsRecorder, shardIdx int) {
	evicted := 0

	s.mu.Lock()
	for k, e := range s.data {
		if e.isExpired(now) {
			delete(s.data, k)
			s.count.Add(-1)
			if s.shrinking.Load() {
				s.dirty[k] = struct{}{}
			}
			evicted++
		}
	}
	s.mu.Unlock()

	for range evicted {
		metrics.RecordActiveEviction(shardIdx)
	}
}

// maybeShrink reconstructs the shard's map if eligible.
// Three-phase approach minimises lock hold time:
//
//	Phase 1 (write lock, brief): set shrinking flag, snapshot live keys.
//	Phase 2 (no lock): build a new map from the snapshot — the slow part.
//	Phase 3 (write lock, brief): delta-merge dirty keys, swap, clear flag.
//
// The shrinking flag MUST be set under write lock together with the snapshot —
// any later set/delete will see the flag and record into dirty, guaranteeing
// no mutation is lost during phase 2.
func (s *shard[K, V]) maybeShrink(now int64, minEntries int, metrics MetricsRecorder, shardIdx int) {
	// Fast path: skip if shard is already small enough.
	if minEntries > 0 && int(s.count.Load()) >= minEntries {
		return
	}

	// --- Phase 1: write lock, set flag and snapshot atomically ---
	s.mu.Lock()
	s.shrinking.Store(true)

	before := len(s.data)
	keys := make([]K, 0, before)
	values := make([]entry[V], 0, before)
	for k, e := range s.data {
		if !e.isExpired(now) {
			keys = append(keys, k)
			values = append(values, e)
		}
	}
	s.mu.Unlock()

	// --- Phase 2: build new map without any lock ---
	newData := make(map[K]entry[V], len(keys))
	for i, k := range keys {
		newData[k] = values[i]
	}

	// --- Phase 3: delta merge and swap under write lock ---
	s.mu.Lock()

	// Each dirty key was either written or deleted during phase 2.
	// Presence in s.data is the source of truth at swap time —
	// we don't compare timestamps because s.data already holds the latest version.
	for k := range s.dirty {
		if e, ok := s.data[k]; ok {
			// Key exists in current data — copy the latest version into newData.
			newData[k] = e
		} else {
			// Key was deleted during phase 2 — ensure it is absent from newData.
			delete(newData, k)
		}
	}

	after := len(newData)
	s.data = newData
	s.count.Store(int64(after))

	clear(s.dirty)
	s.shrinking.Store(false)

	s.mu.Unlock()

	metrics.RecordShrink(shardIdx, before, after)
}

// shardSize returns unsafe.Sizeof of the shard struct.
// Used by TestShardCacheLineAlignment to verify padding.
func shardSize[K comparable, V any]() uintptr {
	var s shard[K, V]
	return unsafe.Sizeof(s)
}
