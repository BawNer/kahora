package kahora

import "sync/atomic"

// MetricsRecorder receives cache events. Methods are called on the hot
// path — implementations must be non-blocking and not allocate.
type MetricsRecorder interface {
	RecordHit(shard int)
	RecordMiss(shard int)
	RecordSet(shard int)
	RecordDelete(shard int)
	RecordLazyEviction(shard int)
	RecordActiveEviction(shard int)
	RecordEviction(shard int) // policy-driven, not TTL
	RecordShrink(shard, before, after int)
	RecordCapacityExceeded(shard int)
	// RecordAccessesDropped is only called from the LFU drainer, off the Get
	// hot path. dropped is the count of access records that could not be
	// buffered since the previous drain.
	RecordAccessesDropped(shard, dropped int)
}

type nopRecorder struct{}

func (nopRecorder) RecordHit(int)                 {}
func (nopRecorder) RecordMiss(int)                {}
func (nopRecorder) RecordSet(int)                 {}
func (nopRecorder) RecordDelete(int)              {}
func (nopRecorder) RecordLazyEviction(int)        {}
func (nopRecorder) RecordActiveEviction(int)      {}
func (nopRecorder) RecordEviction(int)            {}
func (nopRecorder) RecordShrink(_, _, _ int)      {}
func (nopRecorder) RecordCapacityExceeded(int)    {}
func (nopRecorder) RecordAccessesDropped(_, _ int) {}

type shardMetrics struct {
	hits             atomic.Int64
	misses           atomic.Int64
	sets             atomic.Int64
	evictions        atomic.Int64
	shrinkCount      atomic.Int64
	lastShrinkBefore atomic.Int64
	lastShrinkAfter  atomic.Int64
}

// DefaultRecorder is a thread-safe MetricsRecorder. Pass to WithMetricsRecorder
// and call Snapshot to read.
type DefaultRecorder struct {
	shards []shardMetrics

	hits             atomic.Int64
	misses           atomic.Int64
	sets             atomic.Int64
	deletes          atomic.Int64
	lazyEvictions    atomic.Int64
	activeEvictions  atomic.Int64
	evictions        atomic.Int64
	capacityExceeded atomic.Int64
	accessesDropped  atomic.Int64
}

func NewRecorder(shardCount ShardCount) *DefaultRecorder {
	return &DefaultRecorder{shards: make([]shardMetrics, shardCount)}
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

func (r *DefaultRecorder) RecordDelete(_ int)         { r.deletes.Add(1) }
func (r *DefaultRecorder) RecordLazyEviction(_ int)   { r.lazyEvictions.Add(1) }
func (r *DefaultRecorder) RecordActiveEviction(_ int) { r.activeEvictions.Add(1) }

func (r *DefaultRecorder) RecordEviction(shard int) {
	r.shards[shard].evictions.Add(1)
	r.evictions.Add(1)
}

func (r *DefaultRecorder) RecordShrink(shard, before, after int) {
	r.shards[shard].shrinkCount.Add(1)
	r.shards[shard].lastShrinkBefore.Store(int64(before))
	r.shards[shard].lastShrinkAfter.Store(int64(after))
}

func (r *DefaultRecorder) RecordCapacityExceeded(_ int) { r.capacityExceeded.Add(1) }

func (r *DefaultRecorder) RecordAccessesDropped(_, dropped int) {
	r.accessesDropped.Add(int64(dropped))
}

type ShardSnapshot struct {
	Index            int
	Hits             int64
	Misses           int64
	Sets             int64
	Evictions        int64
	ShrinkCount      int64
	LastShrinkBefore int64
	LastShrinkAfter  int64
}

type Snapshot struct {
	Hits             int64
	Misses           int64
	Sets             int64
	Deletes          int64
	LazyEvictions    int64
	ActiveEvictions  int64
	Evictions        int64
	CapacityExceeded int64
	AccessesDropped  int64
	Shards           []ShardSnapshot
}

func (r *DefaultRecorder) Snapshot() Snapshot {
	shards := make([]ShardSnapshot, len(r.shards))
	for i := range r.shards {
		s := &r.shards[i]
		shards[i] = ShardSnapshot{
			Index:            i,
			Hits:             s.hits.Load(),
			Misses:           s.misses.Load(),
			Sets:             s.sets.Load(),
			Evictions:        s.evictions.Load(),
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
		Evictions:        r.evictions.Load(),
		CapacityExceeded: r.capacityExceeded.Load(),
		AccessesDropped:  r.accessesDropped.Load(),
		Shards:           shards,
	}
}
