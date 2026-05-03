package kahora

import (
	"sync"
	"sync/atomic"
	"unsafe"
)

const (
	// cacheLineSize is the x86-64 cache line size.
	// shardMetrics and shard are padded to this boundary to avoid false sharing.
	cacheLineSize = 64
)

// shard is a single cache partition.
// Each shard is independently locked — operations on different shards never contend.
//
// Memory layout is explicit: RWMutex first (most frequently accessed),
// then maps, then the atomic flag, then padding.
// Padding ensures no two shards share a CPU cache line.
type shard[K comparable, V any] struct {
	mu   sync.RWMutex
	data map[K]entry[V]

	// dirty tracks keys written or updated while a shrink is in progress.
	// Allocated once at shard init, cleared between shrink cycles via clear().
	// Only populated when shrinking == true.
	dirty map[K]struct{}

	// shrinking is set to true during shrink phase 2 (map reconstruction without lock).
	// Set and Delete check this to decide whether to record into dirty.
	shrinking atomic.Bool

	// count is the number of live entries in this shard.
	// Approximate — may be slightly stale under concurrent load.
	// Used for maxEntries enforcement and shrink eligibility.
	count atomic.Int64

	// Padding to prevent false sharing with adjacent shards in the slice.
	// Verified by TestShardSize in shard_test.go.
	_ [shardPadding]byte
}

// shardPadding is computed so that unsafe.Sizeof(shard{}) is a multiple of cacheLineSize.
// The value here is a placeholder — the real check is in the test.
// If you add fields to shard, run TestShardSize to catch misalignment.
const shardPadding = 8

// newShard allocates and initialises a shard with a pre-allocated map.
// initialSize is a hint — avoids rehashing on warm-up.
func newShard[K comparable, V any](initialSize int) *shard[K, V] {
	return &shard[K, V]{
		data:  make(map[K]entry[V], initialSize),
		dirty: make(map[K]struct{}),
	}
}

// get looks up key in the shard.
// now must be the result of a single monoNow() call, shared across all shards
// in the same Get operation — avoids redundant syscalls on the hot path.
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
		s.mu.Lock()
		// Re-check under write lock — another goroutine may have already deleted it.
		if e2, still := s.data[key]; still && e2.isExpired(now) {
			delete(s.data, key)
			s.count.Add(-1)
		}
		s.mu.Unlock()

		metrics.RecordLazyEviction(shardIdx)
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
	now := monoNow()

	s.mu.Lock()
	defer s.mu.Unlock()

	_, exists := s.data[key]
	if !exists && shardLimit > 0 && int(s.count.Load()) >= shardLimit {
		metrics.RecordCapacityExceeded(shardIdx)
		return ErrCapacityExceeded
	}

	s.data[key] = entry[V]{
		value:     value,
		expiresAt: expiresAt,
		updatedAt: now,
	}

	if s.shrinking.Load() {
		s.dirty[key] = struct{}{}
	}

	if !exists {
		s.count.Add(1)
	}

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
			// Remove from dirty too — no point merging a deleted key.
			delete(s.dirty, key)
		}
	}
	s.mu.Unlock()

	if exists {
		metrics.RecordDelete(shardIdx)
	}
}

// sweepExpired iterates the shard and removes all expired entries.
// Called by the background loop when active expiry is enabled.
// Holds the write lock for the full sweep — keep activeExpiryInterval
// high enough that this does not impact Get latency noticeably.
func (s *shard[K, V]) sweepExpired(now int64, metrics MetricsRecorder, shardIdx int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for k, e := range s.data {
		if e.isExpired(now) {
			delete(s.data, k)
			s.count.Add(-1)
			metrics.RecordActiveEviction(shardIdx)
		}
	}
}

// maybeShrink reconstructs the shard's map if the live entry count
// is below minEntries. Uses a three-phase approach to minimise lock hold time:
//
//	Phase 1 (RLock): snapshot all live keys into a slice.
//	Phase 2 (no lock): build a new map from the snapshot.
//	Phase 3 (Lock): delta-merge dirty keys written during phase 2, then swap.
//
// This ensures the write lock is held only for two brief periods,
// not for the full map copy.
func (s *shard[K, V]) maybeShrink(now int64, minEntries int, metrics MetricsRecorder, shardIdx int) {
	// Fast path: check count without locking.
	if minEntries > 0 && int(s.count.Load()) >= minEntries {
		return
	}

	// --- Phase 1: snapshot live entries under RLock ---
	s.mu.RLock()
	before := len(s.data)
	snapshot := make([]entry[V], 0, before)
	keys := make([]K, 0, before)
	for k, e := range s.data {
		if !e.isExpired(now) {
			keys = append(keys, k)
			snapshot = append(snapshot, e)
		}
	}
	snapshotTime := monoNow()
	s.mu.RUnlock()

	// Signal to Set/Delete that dirty tracking is active.
	s.shrinking.Store(true)

	// --- Phase 2: build new map without any lock ---
	newData := make(map[K]entry[V], len(keys))
	for i, k := range keys {
		newData[k] = snapshot[i]
	}

	// --- Phase 3: delta merge and swap under Lock ---
	s.mu.Lock()

	// Merge keys that were written or updated during phase 2.
	// updatedAt > snapshotTime means the entry changed after our snapshot.
	for k := range s.dirty {
		if e, ok := s.data[k]; ok {
			if e.updatedAt > snapshotTime {
				newData[k] = e
			}
		} else {
			// Key was deleted during phase 2 — ensure it is absent from new map.
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

// size returns the current unsafe.Sizeof the shard struct.
// Used in tests to verify cache line alignment.
func shardSize[K comparable, V any]() uintptr {
	var s shard[K, V]
	return unsafe.Sizeof(s)
}
