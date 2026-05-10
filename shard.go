package kahora

import (
	"sync"
	"sync/atomic"
	"unsafe"
)

const cacheLineSize = 64

// shard uses sync.Mutex (not RWMutex) because LFU mutates state on every Get.
type shard[K comparable, V any] struct {
	mu   sync.Mutex
	data map[K]entry[V]

	// nil unless EvictionLFU
	freq       map[K]uint32
	sampleSize int

	dirty     map[K]struct{}
	shrinking atomic.Bool
	count     atomic.Int64

	_ [shardPadding]byte
}

const shardPadding = 8

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

func (s *shard[K, V]) set(key K, value V, expiresAt int64, shardLimit int, metrics MetricsRecorder, shardIdx int) error {
	evicted := false

	s.mu.Lock()

	_, exists := s.data[key]

	if !exists && shardLimit > 0 && int(s.count.Load()) >= shardLimit {
		if s.freq != nil {
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
		} else {
			s.mu.Unlock()
			metrics.RecordCapacityExceeded(shardIdx)
			return ErrCapacityExceeded
		}
	}

	s.data[key] = entry[V]{value: value, expiresAt: expiresAt}

	if s.freq != nil && !exists {
		s.freq[key] = 0
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

// sampleVictim picks the lowest-frequency entry from up to sampleSize random
// candidates. Map iteration order is randomised by the Go runtime.
// Caller must hold s.mu.
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

// maybeShrink rebuilds the shard map in three phases:
//  1. snapshot live keys under lock
//  2. build new map without lock
//  3. delta-merge dirty keys, swap, clear flag
func (s *shard[K, V]) maybeShrink(now int64, minEntries int, metrics MetricsRecorder, shardIdx int) {
	if minEntries > 0 && int(s.count.Load()) >= minEntries {
		return
	}

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

	newData := make(map[K]entry[V], len(keys))
	for i, k := range keys {
		newData[k] = values[i]
	}

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

func shardSize[K comparable, V any]() uintptr {
	var s shard[K, V]
	return unsafe.Sizeof(s)
}
