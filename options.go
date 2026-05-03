package kahora

import (
	"errors"
	"time"
)

// ShardCount defines the number of shards used by the cache.
// More shards — less lock contention, more memory overhead per shard.
type ShardCount int

const (
	ShardCountXS ShardCount = 16
	ShardCountS  ShardCount = 64
	ShardCountM  ShardCount = 256 // default
	ShardCountL  ShardCount = 1024
	ShardCountXL ShardCount = 4096
)

// options holds the internal configuration for Cache.
// All fields are unexported — access only via functional options.
type options struct {
	shardCount ShardCount

	// TTL
	ttl time.Duration // 0 means no TTL

	// Entry limit.
	// Enforced per-shard via atomic counters — approximate, not a hard guarantee.
	// See WithMaxEntries for details.
	maxEntries int // 0 means unlimited

	// Metrics
	metricsRecorder MetricsRecorder

	// Shrink.
	// Always enabled. One shard reconstructed per tick via round-robin.
	// tick interval = shrinkCycleInterval / shardCount.
	shrinkCycleInterval time.Duration
	shrinkMinEntries    int // skip shrink if live entries < this value; 0 = always shrink

	// Active expiry (background sweep).
	// Requires ttl > 0.
	activeExpiry         bool
	activeExpiryInterval time.Duration
}

// Option is a functional option for Cache.
type Option func(*options) error

// defaultOptions returns a safe, opinionated default configuration.
func defaultOptions() options {
	return options{
		shardCount:          ShardCountM,
		shrinkCycleInterval: 60 * time.Second,
		shrinkMinEntries:    0,
	}
}

// validate checks cross-field constraints after all options are applied.
// Per-field validation is done inside each Option constructor.
func (o *options) validate() error {
	if o.activeExpiry && o.ttl == 0 {
		return errors.New("kahora: active expiry requires ttl to be set")
	}
	if o.activeExpiry && o.activeExpiryInterval <= 0 {
		return errors.New("kahora: active expiry interval must be positive")
	}
	return nil
}

// --- Public Option constructors ---

// WithShardCount sets the number of shards.
// Use ShardCount* constants (XS/S/M/L/XL) or a custom positive value.
// Default: ShardCountM (256).
func WithShardCount(n ShardCount) Option {
	return func(o *options) error {
		if n <= 0 {
			return errors.New("kahora: shard count must be positive")
		}
		if !isPowerOfTwo(n) {
			return errors.New("kahora: shard count must be a power of two (e.g. 16, 64, 256, 1024, 4096)")
		}
		o.shardCount = n
		return nil
	}
}

// WithTTL sets a global TTL for all entries.
// Expiry is lazy by default — checked on Get.
// To enable background sweep, also use WithActiveExpiry.
func WithTTL(ttl time.Duration) Option {
	return func(o *options) error {
		if ttl <= 0 {
			return errors.New("kahora: ttl must be positive")
		}
		o.ttl = ttl
		return nil
	}
}

// WithMaxEntries sets a best-effort cap on total live entries across all shards.
// The limit is enforced per-shard via atomic counters and may be exceeded
// slightly under concurrent load. This is intentional — avoiding a global
// lock on the hot path is worth the approximation.
// 0 means unlimited.
func WithMaxEntries(n int) Option {
	return func(o *options) error {
		if n < 0 {
			return errors.New("kahora: max entries must be non-negative")
		}
		o.maxEntries = n
		return nil
	}
}

// WithMetricsRecorder attaches a custom metrics recorder.
// If not set, a nop recorder is used — zero overhead.
func WithMetricsRecorder(r MetricsRecorder) Option {
	return func(o *options) error {
		if r == nil {
			return errors.New("kahora: metrics recorder must not be nil")
		}
		o.metricsRecorder = r
		return nil
	}
}

// WithShrinkCycleInterval sets the duration of one full shrink cycle across all shards.
// Internally: tick = cycleInterval / shardCount. One shard per tick, round-robin.
// Higher value = less frequent reconstruction, lower CPU/alloc pressure.
// Default: 60s (tick ~234ms for 256 shards).
func WithShrinkCycleInterval(d time.Duration) Option {
	return func(o *options) error {
		if d <= 0 {
			return errors.New("kahora: shrink cycle interval must be positive")
		}
		o.shrinkCycleInterval = d
		return nil
	}
}

// WithShrinkMinEntries sets the minimum number of live entries required
// for a shard to be eligible for reconstruction.
// Shards below this threshold are skipped — already small enough.
// 0 means always reconstruct when the cycle reaches this shard.
// Default: 0.
func WithShrinkMinEntries(n int) Option {
	return func(o *options) error {
		if n < 0 {
			return errors.New("kahora: shrink min entries must be non-negative")
		}
		o.shrinkMinEntries = n
		return nil
	}
}

// WithActiveExpiry enables a background goroutine that proactively sweeps
// shards and deletes expired entries. Without this, expiry is lazy —
// stale entries are only evicted on Get.
// Requires WithTTL to be set.
func WithActiveExpiry(interval time.Duration) Option {
	return func(o *options) error {
		if interval <= 0 {
			return errors.New("kahora: active expiry interval must be positive")
		}
		o.activeExpiry = true
		o.activeExpiryInterval = interval
		return nil
	}
}
