package kahora

import (
	"hash/maphash"
	"sync/atomic"
	"time"
)

// Cache is a generic, sharded, in-memory cache.
// Always create via New — zero value is not usable.
type Cache[K comparable, V any] struct {
	shards []*shard[K, V]
	seed   maphash.Seed
	opts   options
	closed atomic.Bool
	stop   chan struct{}
}

func New[K comparable, V any](opts ...Option) (*Cache[K, V], error) {
	o := defaultOptions()
	for _, opt := range opts {
		if err := opt(&o); err != nil {
			return nil, err
		}
	}
	if err := o.validate(); err != nil {
		return nil, err
	}

	if o.metricsRecorder == nil {
		o.metricsRecorder = nopRecorder{}
	}

	n := int(o.shardCount)
	initialSize := 0
	if o.maxEntries > 0 {
		initialSize = o.maxEntries / n
	}

	lfuSampleSize := 0
	lfuBufferSize := 0
	if o.evictionPolicy == EvictionLFU {
		lfuSampleSize = o.lfuSampleSize
		lfuBufferSize = o.lfuBufferSize
	}

	shards := make([]*shard[K, V], n)
	for i := range shards {
		shards[i] = newShard[K, V](initialSize, lfuSampleSize, lfuBufferSize)
	}

	c := &Cache[K, V]{
		shards: shards,
		seed:   maphash.MakeSeed(),
		opts:   o,
		stop:   make(chan struct{}),
	}

	go c.background()

	return c, nil
}

func (c *Cache[K, V]) Get(key K) (V, bool) {
	s, idx := c.shardFor(key)
	return s.get(key, monoNow(), c.opts.metricsRecorder, idx)
}

// Set returns ErrCapacityExceeded under EvictionReject when the shard is full.
// Under EvictionLFU it evicts a victim and always succeeds.
// Returns ErrClosed after Close.
func (c *Cache[K, V]) Set(key K, value V) error {
	if c.closed.Load() {
		return ErrClosed
	}

	s, idx := c.shardFor(key)

	var expiresAt int64
	if c.opts.ttl > 0 {
		expiresAt = monoNow() + c.opts.ttl.Nanoseconds()
	}

	shardLimit := 0
	if c.opts.maxEntries > 0 {
		shardLimit = c.opts.maxEntries / int(c.opts.shardCount)
	}

	return s.set(key, value, expiresAt, shardLimit, c.opts.metricsRecorder, idx)
}

func (c *Cache[K, V]) Delete(key K) {
	s, idx := c.shardFor(key)
	s.delete(key, c.opts.metricsRecorder, idx)
}

// Close stops background goroutines. Idempotent.
func (c *Cache[K, V]) Close() {
	if c.closed.CompareAndSwap(false, true) {
		close(c.stop)
	}
}

func (c *Cache[K, V]) shardFor(key K) (s *shard[K, V], idx int) {
	h := maphash.Comparable(c.seed, key)
	idx = int(h & uint64(len(c.shards)-1))
	return c.shards[idx], idx
}

func (c *Cache[K, V]) background() {
	n := int(c.opts.shardCount)

	shrinkTicker := time.NewTicker(c.opts.shrinkCycleInterval / time.Duration(n))
	defer shrinkTicker.Stop()

	var shrinkCursor, expiryCursor, agingCursor, drainCursor int

	var expiryCh <-chan time.Time
	if c.opts.activeExpiry {
		t := time.NewTicker(c.opts.activeExpiryInterval)
		defer t.Stop()
		expiryCh = t.C
	}

	var agingCh <-chan time.Time
	if c.opts.evictionPolicy == EvictionLFU {
		t := time.NewTicker(c.opts.lfuAgingInterval / time.Duration(n))
		defer t.Stop()
		agingCh = t.C
	}

	// LFU access-buffer drainer. Fixed cadence if WithLFUDrainInterval was set,
	// otherwise adaptive inside [min, max] based on last shard's fill level.
	var (
		drainCh       <-chan time.Time
		drainTimer    *time.Timer
		drainInterval time.Duration
		drainAdaptive bool
	)
	if c.opts.evictionPolicy == EvictionLFU {
		if c.opts.lfuDrainInterval > 0 {
			drainInterval = c.opts.lfuDrainInterval
		} else {
			drainInterval = c.opts.lfuDrainMinInterval
			drainAdaptive = true
		}
		drainTimer = time.NewTimer(drainInterval / time.Duration(n))
		defer drainTimer.Stop()
		drainCh = drainTimer.C
	}

	for {
		select {
		case <-c.stop:
			return

		case <-shrinkTicker.C:
			idx := shrinkCursor % n
			shrinkCursor++
			c.shards[idx].maybeShrink(monoNow(), c.opts.shrinkMinEntries, c.opts.metricsRecorder, idx)

		case <-expiryCh:
			idx := expiryCursor % n
			expiryCursor++
			c.shards[idx].sweepExpired(monoNow(), c.opts.metricsRecorder, idx)

		case <-agingCh:
			idx := agingCursor % n
			agingCursor++
			c.shards[idx].ageFreq()

		case <-drainCh:
			idx := drainCursor % n
			drainCursor++
			attempted := c.shards[idx].drainAccess(c.opts.metricsRecorder, idx)
			if drainAdaptive {
				drainInterval = adaptDrainInterval(drainInterval, attempted, c.opts.lfuBufferSize,
					c.opts.lfuDrainMinInterval, c.opts.lfuDrainMaxInterval)
			}
			drainTimer.Reset(drainInterval / time.Duration(n))
		}
	}
}

// adaptDrainInterval halves the interval when the ring was near-full (drain is
// falling behind) and grows it 1.5× when it was mostly empty (idle). Bounded
// by min/max so we can't runaway-drain or freeze counters.
func adaptDrainInterval(current time.Duration, attempted uint64, bufferSize int, min, max time.Duration) time.Duration {
	if bufferSize <= 0 {
		return current
	}
	// attempted may exceed bufferSize (drops); the ratio still meaningfully
	// signals "way too slow".
	fill := float64(attempted) / float64(bufferSize)
	switch {
	case fill > 0.90:
		d := current / 2
		if d < min {
			d = min
		}
		return d
	case fill < 0.25:
		d := current * 3 / 2
		if d > max {
			d = max
		}
		return d
	default:
		return current
	}
}
