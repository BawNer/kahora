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
// Returns an error if any option is invalid or options are logically inconsistent.
//
// New starts a background goroutine for shrink and active expiry (if enabled).
// Always call Close when the cache is no longer needed.
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

	shards := make([]*shard[K, V], n)
	for i := range shards {
		shards[i] = newShard[K, V](initialSize)
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
// Returns (value, true) on hit, (zero, false) on miss or expiry.
// Expired entries are removed lazily on Get.
func (c *Cache[K, V]) Get(key K) (V, bool) {
	s, idx := c.shardFor(key)
	return s.get(key, monoNow(), c.opts.metricsRecorder, idx)
}

// Set writes key/value into the cache.
// If the key already exists, it is overwritten.
// Returns ErrCapacityExceeded if the cache is at its maxEntries limit.
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
// No-op if key does not exist or has already expired.
func (c *Cache[K, V]) Delete(key K) {
	s, idx := c.shardFor(key)
	s.delete(key, c.opts.metricsRecorder, idx)
}

// Close stops the background goroutine and releases resources.
// After Close, Set returns ErrClosed. Get and Delete remain safe to call
// but operate on a static snapshot — no further eviction or shrink occurs.
//
// Close is idempotent — safe to call multiple times.
func (c *Cache[K, V]) Close() {
	if c.closed.CompareAndSwap(false, true) {
		close(c.stop)
	}
}

// shardFor returns the shard and its index for the given key.
// Uses maphash.Comparable for zero-allocation hashing of comparable types.
// Requires shardCount to be a power of two — enforced in WithShardCount.
func (c *Cache[K, V]) shardFor(key K) (s *shard[K, V], idx int) {
	h := maphash.Comparable(c.seed, key)
	idx = int(h & uint64(len(c.shards)-1))
	return c.shards[idx], idx
}

// background runs the shrink and active expiry loops.
// Two independent tickers, two independent round-robin cursors.
// Single goroutine — no per-shard goroutines.
func (c *Cache[K, V]) background() {
	n := int(c.opts.shardCount)

	shrinkTick := c.opts.shrinkCycleInterval / time.Duration(n)
	shrinkTicker := time.NewTicker(shrinkTick)
	defer shrinkTicker.Stop()

	var shrinkCursor int
	var expiryCursor int

	// Active expiry ticker — nil channel blocks forever when expiry is disabled.
	// select on a nil channel is never ready — zero cost when unused.
	var expiryCh <-chan time.Time
	if c.opts.activeExpiry {
		t := time.NewTicker(c.opts.activeExpiryInterval)
		defer t.Stop()
		expiryCh = t.C
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
		}
	}
}
