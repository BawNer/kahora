package kahora

import (
	"sync/atomic"
)

// MetricsRecorder is the interface for recording cache metrics.
// Implement this to plug in your own metrics backend (Prometheus, StatsD, etc).
//
// All methods are called on the hot path — implementations must be non-blocking
// and must not allocate. If you need to batch or buffer, do it inside your implementation.
//
// Shard index is provided where relevant — allows per-shard observability
// without kahora itself aggregating anything. Aggregation is your job.
type MetricsRecorder interface {
	// RecordHit is called when Get returns a live entry.
	RecordHit(shard int)

	// RecordMiss is called when Get returns nothing —
	// either key not found or entry expired (lazy eviction).
	RecordMiss(shard int)

	// RecordSet is called when Set writes a new or existing entry.
	RecordSet(shard int)

	// RecordDelete is called when Delete explicitly removes an entry.
	RecordDelete(shard int)

	// RecordLazyEviction is called when Get finds an expired entry and removes it.
	// High rate here means active expiry is too infrequent or disabled.
	RecordLazyEviction(shard int)

	// RecordActiveEviction is called when the background sweep removes an expired entry.
	// Requires WithActiveExpiry to be set.
	RecordActiveEviction(shard int)

	// RecordShrink is called when a shard completes map reconstruction.
	// before and after are the live entry counts before and after shrink.
	// Lets you observe how much memory pressure was relieved per cycle.
	RecordShrink(shard, before, after int)

	// RecordCapacityExceeded is called when Set is rejected because
	// the shard has reached its share of maxEntries.
	RecordCapacityExceeded(shard int)
}

// --- nopRecorder ---

// nopRecorder is a no-op implementation of MetricsRecorder.
// Used when no recorder is provided. All calls are inlined away by the compiler.
type nopRecorder struct{}

func (nopRecorder) RecordHit(int)              {}
func (nopRecorder) RecordMiss(int)             {}
func (nopRecorder) RecordSet(int)              {}
func (nopRecorder) RecordDelete(int)           {}
func (nopRecorder) RecordLazyEviction(int)     {}
func (nopRecorder) RecordActiveEviction(int)   {}
func (nopRecorder) RecordShrink(_, _, _ int)   {}
func (nopRecorder) RecordCapacityExceeded(int) {}

// --- DefaultRecorder ---

// shardMetrics holds per-shard atomic counters.
// Padded to 64 bytes to avoid false sharing between shards on different CPU cores.
type shardMetrics struct {
	hits             atomic.Int64
	misses           atomic.Int64
	sets             atomic.Int64
	shrinkCount      atomic.Int64
	lastShrinkBefore atomic.Int64
	lastShrinkAfter  atomic.Int64
	_                [8]byte // padding to 64 bytes
}

// DefaultRecorder is a thread-safe, zero-dependency MetricsRecorder.
// Use NewRecorder to create one, then pass it to New via WithMetricsRecorder.
// Call Snapshot at any time to read current state.
type DefaultRecorder struct {
	shards []shardMetrics

	// Global counters — not per-shard because they either span shards
	// or are infrequent enough that a single atomic is fine.
	hits             atomic.Int64
	misses           atomic.Int64
	sets             atomic.Int64
	deletes          atomic.Int64
	lazyEvictions    atomic.Int64
	activeEvictions  atomic.Int64
	capacityExceeded atomic.Int64
}

// NewRecorder creates a DefaultRecorder sized for the given shard count.
// Pass the same ShardCount you use in New — or use ShardCountM (256) if default.
func NewRecorder(shardCount ShardCount) *DefaultRecorder {
	return &DefaultRecorder{
		shards: make([]shardMetrics, shardCount),
	}
}

func (r *DefaultRecorder) RecordHit(shard int) {
	r.shards[shard].hits.Add(1)
	r.hits.Add(1)
}

func (r *DefaultRecorder) RecordMiss(shard int) {
	r.shards[shard].misses.Add(1)
	r.misses.Add(1)
}

func (r *DefaultRecorder) RecordSet(shard int) {
	r.shards[shard].sets.Add(1)
	r.sets.Add(1)
}

func (r *DefaultRecorder) RecordDelete(_ int) {
	r.deletes.Add(1)
}

func (r *DefaultRecorder) RecordLazyEviction(_ int) {
	r.lazyEvictions.Add(1)
}

func (r *DefaultRecorder) RecordActiveEviction(_ int) {
	r.activeEvictions.Add(1)
}

func (r *DefaultRecorder) RecordShrink(shard, before, after int) {
	r.shards[shard].shrinkCount.Add(1)
	r.shards[shard].lastShrinkBefore.Store(int64(before))
	r.shards[shard].lastShrinkAfter.Store(int64(after))
}

func (r *DefaultRecorder) RecordCapacityExceeded(_ int) {
	r.capacityExceeded.Add(1)
}

// --- Snapshot ---

// ShardSnapshot is a point-in-time snapshot of a single shard's metrics.
type ShardSnapshot struct {
	Index            int
	Hits             int64
	Misses           int64
	Sets             int64
	ShrinkCount      int64
	LastShrinkBefore int64
	LastShrinkAfter  int64
}

// Snapshot is a point-in-time view of all cache metrics.
// Global counters are independent atomics — not derived from shards —
// so they are always consistent with what was actually recorded.
type Snapshot struct {
	// Global
	Hits             int64
	Misses           int64
	Sets             int64
	Deletes          int64
	LazyEvictions    int64
	ActiveEvictions  int64
	CapacityExceeded int64

	// Per-shard — use to observe distribution uniformity and shrink activity.
	Shards []ShardSnapshot
}

// Snapshot returns a point-in-time copy of all metrics.
// Safe to call concurrently. Non-blocking.
func (r *DefaultRecorder) Snapshot() Snapshot {
	shards := make([]ShardSnapshot, len(r.shards))
	for i := range r.shards {
		s := &r.shards[i]
		shards[i] = ShardSnapshot{
			Index:            i,
			Hits:             s.hits.Load(),
			Misses:           s.misses.Load(),
			Sets:             s.sets.Load(),
			ShrinkCount:      s.shrinkCount.Load(),
			LastShrinkBefore: s.lastShrinkBefore.Load(),
			LastShrinkAfter:  s.lastShrinkAfter.Load(),
		}
	}

	return Snapshot{
		Hits:             r.hits.Load(),
		Misses:           r.misses.Load(),
		Sets:             r.sets.Load(),
		Deletes:          r.deletes.Load(),
		LazyEvictions:    r.lazyEvictions.Load(),
		ActiveEvictions:  r.activeEvictions.Load(),
		CapacityExceeded: r.capacityExceeded.Load(),
		Shards:           shards,
	}
}
