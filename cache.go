package kahora

import (
	"hash/maphash"
	"sync/atomic"
	"time"
)

// Cache is a generic, sharded, in-memory cache.
// Safe for concurrent use by multiple goroutines.
//
// K must be comparable. V can be any type.
// Zero value is not usable — always create via New.
type Cache[K comparable, V any] struct {
	shards []*shard[K, V]
	seed   maphash.Seed
	opts   options
	closed atomic.Bool
	stop   chan struct{}
}

// New creates a new Cache with the given options.
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

	// Pass sampleSize > 0 to enable LFU per-shard. Reject mode passes 0.
	lfuSampleSize := 0
	if o.evictionPolicy == EvictionLFU {
		lfuSampleSize = o.lfuSampleSize
	}

	shards := make([]*shard[K, V], n)
	for i := range shards {
		shards[i] = newShard[K, V](initialSize, lfuSampleSize)
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

// Get returns the value associated with key.
func (c *Cache[K, V]) Get(key K) (V, bool) {
	s, idx := c.shardFor(key)
	return s.get(key, monoNow(), c.opts.metricsRecorder, idx)
}

// Set writes key/value into the cache.
//
// With EvictionReject (default), Set returns ErrCapacityExceeded when the
// shard is full. With EvictionLFU, Set evicts a victim and always succeeds.
// Returns ErrClosed if the cache has been closed.
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

// Delete removes key from the cache.
func (c *Cache[K, V]) Delete(key K) {
	s, idx := c.shardFor(key)
	s.delete(key, c.opts.metricsRecorder, idx)
}

// Close stops the background goroutine and releases resources.
// Idempotent.
func (c *Cache[K, V]) Close() {
	if c.closed.CompareAndSwap(false, true) {
		close(c.stop)
	}
}

// shardFor returns the shard and its index for the given key.
func (c *Cache[K, V]) shardFor(key K) (s *shard[K, V], idx int) {
	h := maphash.Comparable(c.seed, key)
	idx = int(h & uint64(len(c.shards)-1))
	return c.shards[idx], idx
}

// background runs the shrink, active expiry, and LFU aging loops.
// All scheduled work happens in this single goroutine — no per-shard goroutines.
func (c *Cache[K, V]) background() {
	n := int(c.opts.shardCount)

	shrinkTick := c.opts.shrinkCycleInterval / time.Duration(n)
	shrinkTicker := time.NewTicker(shrinkTick)
	defer shrinkTicker.Stop()

	var shrinkCursor int
	var expiryCursor int

	// Active expiry — nil channel blocks forever when disabled.
	var expiryCh <-chan time.Time
	if c.opts.activeExpiry {
		t := time.NewTicker(c.opts.activeExpiryInterval)
		defer t.Stop()
		expiryCh = t.C
	}

	// LFU aging — nil channel blocks forever when LFU is disabled.
	var agingCh <-chan time.Time
	var agingCursor int
	agingTick := time.Duration(0)
	if c.opts.evictionPolicy == EvictionLFU {
		// Spread aging across shards over the cycle to avoid lock spikes.
		agingTick = c.opts.lfuAgingInterval / time.Duration(n)
		t := time.NewTicker(agingTick)
		defer t.Stop()
		agingCh = t.C
	}

	for {
		select {
		case <-c.stop:
			return

		case <-shrinkTicker.C:
			idx := shrinkCursor % n
			shrinkCursor++
			c.shards[idx].maybeShrink(
				monoNow(),
				c.opts.shrinkMinEntries,
				c.opts.metricsRecorder,
				idx,
			)

		case <-expiryCh:
			idx := expiryCursor % n
			expiryCursor++
			c.shards[idx].sweepExpired(monoNow(), c.opts.metricsRecorder, idx)

		case <-agingCh:
			idx := agingCursor % n
			agingCursor++
			c.shards[idx].ageFreq()
		}
	}
}
