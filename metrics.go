package kahora

import "sync/atomic"

// EventType tags a cache event. New event types can be added without breaking
// existing MetricsRecorder implementations — that is the whole point of the
// single-method interface. Ordering is not part of the API contract; treat
// these as opaque tags.
type EventType uint8

const (
	EventHit EventType = iota
	EventMiss
	EventSet
	EventDelete
	EventLazyEviction
	EventActiveEviction
	EventEviction // policy-driven, not TTL
	EventShrink
	EventCapacityExceeded
	EventAccessesDropped
)

func (t EventType) String() string {
	switch t {
	case EventHit:
		return "hit"
	case EventMiss:
		return "miss"
	case EventSet:
		return "set"
	case EventDelete:
		return "delete"
	case EventLazyEviction:
		return "lazy_eviction"
	case EventActiveEviction:
		return "active_eviction"
	case EventEviction:
		return "eviction"
	case EventShrink:
		return "shrink"
	case EventCapacityExceeded:
		return "capacity_exceeded"
	case EventAccessesDropped:
		return "accesses_dropped"
	default:
		return "unknown"
	}
}

// Event is what every cache observation delivers. Passed by value on the hot
// path — keep it small and do not add fields without a benchmark.
//
// Field usage per EventType:
//
//	EventShrink            → Before = pre-shrink size, After = post-shrink size
//	EventAccessesDropped   → Count  = drop count for this drain batch
//	everything else        → Before/After/Count all zero
type Event struct {
	Type   EventType
	Shard  int
	Before int
	After  int
	Count  int
}

// MetricsRecorder receives cache events. Record is called on the hot path —
// implementations must be non-blocking and not allocate.
type MetricsRecorder interface {
	Record(e Event)
}

type nopRecorder struct{}

func (nopRecorder) Record(Event) {}

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

func (r *DefaultRecorder) Record(e Event) {
	switch e.Type {
	case EventHit:
		r.shards[e.Shard].hits.Add(1)
		r.hits.Add(1)
	case EventMiss:
		r.shards[e.Shard].misses.Add(1)
		r.misses.Add(1)
	case EventSet:
		r.shards[e.Shard].sets.Add(1)
		r.sets.Add(1)
	case EventDelete:
		r.deletes.Add(1)
	case EventLazyEviction:
		r.lazyEvictions.Add(1)
	case EventActiveEviction:
		r.activeEvictions.Add(1)
	case EventEviction:
		r.shards[e.Shard].evictions.Add(1)
		r.evictions.Add(1)
	case EventShrink:
		r.shards[e.Shard].shrinkCount.Add(1)
		r.shards[e.Shard].lastShrinkBefore.Store(int64(e.Before))
		r.shards[e.Shard].lastShrinkAfter.Store(int64(e.After))
	case EventCapacityExceeded:
		r.capacityExceeded.Add(1)
	case EventAccessesDropped:
		r.accessesDropped.Add(int64(e.Count))
	}
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
