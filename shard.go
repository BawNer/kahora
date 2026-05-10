package kahora

import (
	"sync"
	"sync/atomic"
	"unsafe"
)

const (
	// cacheLineSize is the x86-64 cache line size.
	cacheLineSize = 64
)

// shard is a single cache partition.
//
// All operations use a single Mutex (not RWMutex). This is a deliberate choice
// to support EvictionLFU, which mutates state on every Get (counter increment).
// For EvictionReject workloads with high read concurrency, this means slightly
// more contention than a pure RWMutex would give, but keeps the type uniform
// and the codebase simpler.
type shard[K comparable, V any] struct {
	mu   sync.Mutex
	data map[K]entry[V]

	// LFU state — non-nil only when policy == EvictionLFU.
	// freq holds per-key access counters; sampleSize controls eviction sampling.
	freq       map[K]uint32
	sampleSize int

	// dirty tracks keys mutated while a shrink is in progress.
	dirty map[K]struct{}

	shrinking atomic.Bool
	count     atomic.Int64

	// Padding to prevent false sharing with adjacent shards in the slice.
	_ [shardPadding]byte
}

// shardPadding is verified by TestShardCacheLineAlignment.
const shardPadding = 8

// newShard allocates a shard with a pre-allocated map of the given hint size.
// If lfuSampleSize > 0, the shard tracks LFU counters.
func newShard[K comparable, V any](initialSize, lfuSampleSize int) *shard[K, V] {
	s := &shard[K, V]{
		data:  make(map[K]entry[V], initialSize),
		dirty: make(map[K]struct{}),
	}
	if lfuSampleSize > 0 {
		s.freq = make(map[K]uint32, initialSize)
		s.sampleSize = lfuSampleSize
	}
	return s
}

// get looks up key in the shard.
// On hit, if LFU is enabled, the access counter is incremented.
func (s *shard[K, V]) get(key K, now int64, metrics MetricsRecorder, shardIdx int) (V, bool) {
	s.mu.Lock()
	e, ok := s.data[key]
	if !ok {
		s.mu.Unlock()
		metrics.RecordMiss(shardIdx)
		var zero V
		return zero, false
	}

	if e.isExpired(now) {
		delete(s.data, key)
		s.count.Add(-1)
		if s.freq != nil {
			delete(s.freq, key)
		}
		if s.shrinking.Load() {
			s.dirty[key] = struct{}{}
		}
		s.mu.Unlock()

		metrics.RecordLazyEviction(shardIdx)
		metrics.RecordMiss(shardIdx)
		var zero V
		return zero, false
	}

	// LFU touch — increment counter, saturating at MaxUint32.
	if s.freq != nil {
		c := s.freq[key]
		if c < ^uint32(0) {
			s.freq[key] = c + 1
		}
	}

	s.mu.Unlock()
	metrics.RecordHit(shardIdx)
	return e.value, true
}

// set writes key/value into the shard.
// Behaviour when shard is at limit:
//   - shardLimit == 0: no limit, always inserts.
//   - LFU enabled: evicts a victim, then inserts. Returns nil.
//   - LFU disabled (Reject mode): returns ErrCapacityExceeded.
func (s *shard[K, V]) set(key K, value V, expiresAt int64, shardLimit int, metrics MetricsRecorder, shardIdx int) error {
	evicted := false

	s.mu.Lock()

	_, exists := s.data[key]

	if !exists && shardLimit > 0 && int(s.count.Load()) >= shardLimit {
		if s.freq != nil {
			// LFU: evict a victim to make room.
			victim, found := s.sampleVictim()
			if found {
				delete(s.data, victim)
				delete(s.freq, victim)
				s.count.Add(-1)
				if s.shrinking.Load() {
					s.dirty[victim] = struct{}{}
				}
				evicted = true
			}
			// If sampleVictim found nothing (shouldn't happen when count >= shardLimit > 0),
			// fall through and insert anyway — capacity is approximate by design.
		} else {
			// Reject: refuse the write.
			s.mu.Unlock()
			metrics.RecordCapacityExceeded(shardIdx)
			return ErrCapacityExceeded
		}
	}

	s.data[key] = entry[V]{
		value:     value,
		expiresAt: expiresAt,
	}

	if s.freq != nil {
		// New entries start at counter 0 — they must prove their worth.
		// Existing entries keep their counter (don't reset on overwrite).
		if !exists {
			s.freq[key] = 0
		}
	}

	if s.shrinking.Load() {
		s.dirty[key] = struct{}{}
	}

	if !exists {
		s.count.Add(1)
	}

	s.mu.Unlock()

	if evicted {
		metrics.RecordEviction(shardIdx)
	}
	metrics.RecordSet(shardIdx)
	return nil
}

// sampleVictim picks an LFU eviction victim by scanning up to sampleSize
// random entries from the freq map and returning the one with the lowest counter.
// Caller must hold s.mu.
//
// Go map iteration order is randomised by the runtime — no separate RNG needed.
func (s *shard[K, V]) sampleVictim() (K, bool) {
	var (
		minKey  K
		minFreq uint32 = ^uint32(0)
		seen    int
		found   bool
	)

	for k, f := range s.freq {
		if !found || f < minFreq {
			minKey = k
			minFreq = f
			found = true
		}
		seen++
		if seen >= s.sampleSize {
			break
		}
	}

	return minKey, found
}

// delete removes key from the shard.
func (s *shard[K, V]) delete(key K, metrics MetricsRecorder, shardIdx int) {
	s.mu.Lock()
	_, exists := s.data[key]
	if exists {
		delete(s.data, key)
		s.count.Add(-1)
		if s.freq != nil {
			delete(s.freq, key)
		}
		if s.shrinking.Load() {
			s.dirty[key] = struct{}{}
		}
	}
	s.mu.Unlock()

	if exists {
		metrics.RecordDelete(shardIdx)
	}
}

// sweepExpired removes all expired entries from the shard.
func (s *shard[K, V]) sweepExpired(now int64, metrics MetricsRecorder, shardIdx int) {
	evicted := 0

	s.mu.Lock()
	for k, e := range s.data {
		if e.isExpired(now) {
			delete(s.data, k)
			s.count.Add(-1)
			if s.freq != nil {
				delete(s.freq, k)
			}
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

// ageFreq halves all LFU counters. Called periodically to prevent unbounded
// growth and to allow historically hot but currently cold keys to be evicted.
// No-op if LFU is disabled.
func (s *shard[K, V]) ageFreq() {
	if s.freq == nil {
		return
	}
	s.mu.Lock()
	for k, f := range s.freq {
		s.freq[k] = f / 2
	}
	s.mu.Unlock()
}

// maybeShrink reconstructs the shard's map if eligible.
//
// Three-phase approach:
//
//	Phase 1 (locked, brief): set shrinking flag, snapshot live keys.
//	Phase 2 (no lock): build a new map from the snapshot.
//	Phase 3 (locked, brief): delta-merge dirty keys, swap, clear flag.
func (s *shard[K, V]) maybeShrink(now int64, minEntries int, metrics MetricsRecorder, shardIdx int) {
	if minEntries > 0 && int(s.count.Load()) >= minEntries {
		return
	}

	// --- Phase 1 ---
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

	// --- Phase 2 ---
	newData := make(map[K]entry[V], len(keys))
	for i, k := range keys {
		newData[k] = values[i]
	}

	// --- Phase 3 ---
	s.mu.Lock()

	for k := range s.dirty {
		if e, ok := s.data[k]; ok {
			newData[k] = e
		} else {
			delete(newData, k)
		}
	}

	after := len(newData)
	s.data = newData
	s.count.Store(int64(after))

	// Reconstruct freq map to drop entries that no longer exist in data.
	if s.freq != nil {
		newFreq := make(map[K]uint32, after)
		for k := range newData {
			if f, ok := s.freq[k]; ok {
				newFreq[k] = f
			}
		}
		s.freq = newFreq
	}

	clear(s.dirty)
	s.shrinking.Store(false)

	s.mu.Unlock()

	metrics.RecordShrink(shardIdx, before, after)
}

// shardSize returns unsafe.Sizeof of the shard struct.
func shardSize[K comparable, V any]() uintptr {
	var s shard[K, V]
	return unsafe.Sizeof(s)
}
