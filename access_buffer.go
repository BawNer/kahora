package kahora

import (
	"sync"
	"sync/atomic"
)

// accessBuffer is a lossy per-shard ring of recently accessed keys.
// LFU Get records into the ring under a short mutex; a background drainer
// folds the batch into freq. Once the ring fills between drains, extra
// accesses are dropped — accepted imprecision that keeps Get on the RLock
// hot path. See v0.3.0 design notes.
type accessBuffer[K comparable] struct {
	mu   sync.Mutex
	ring []K
	head atomic.Uint64
}

func newAccessBuffer[K comparable](size int) *accessBuffer[K] {
	return &accessBuffer[K]{ring: make([]K, size)}
}

// record buffers key at the next slot. If the ring is full since the last
// drain, the access is dropped. Safe to call under a shard RLock.
func (a *accessBuffer[K]) record(key K) {
	pos := a.head.Add(1) - 1
	if pos < uint64(len(a.ring)) {
		a.mu.Lock()
		a.ring[pos] = key
		a.mu.Unlock()
	}
}

// swap claims all buffered accesses and resets the cursor. Returns the batch
// to fold into freq and the total number of record attempts since the last
// swap (may exceed len(ring); the excess is the drop count).
func (a *accessBuffer[K]) swap() (batch []K, attempted uint64) {
	a.mu.Lock()
	attempted = a.head.Swap(0)
	n := attempted
	if n > uint64(len(a.ring)) {
		n = uint64(len(a.ring))
	}
	if n > 0 {
		batch = make([]K, n)
		copy(batch, a.ring[:n])
	}
	a.mu.Unlock()
	return batch, attempted
}
