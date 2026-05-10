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

// EvictionPolicy controls behaviour when Set is called on a full cache.
//
// EvictionReject (default): Set returns ErrCapacityExceeded.
// The caller decides what to do — retry, log, drop the write, etc.
// No background work, no per-key bookkeeping. Lowest overhead.
//
// EvictionLFU: Set evicts a victim using sampled Least-Frequently-Used.
// Each Get increments a per-key counter; on eviction, k random entries are
// sampled and the one with the lowest counter is removed.
// Counters are halved periodically to age out historically hot keys
// that are no longer accessed.
// Requires WithMaxEntries.
type EvictionPolicy int

const (
	EvictionReject EvictionPolicy = iota
	EvictionLFU
)

// String returns a human-readable name. Used in error messages and logs.
func (p EvictionPolicy) String() string {
	switch p {
	case EvictionReject:
		return "reject"
	case EvictionLFU:
		return "lfu"
	default:
		return "unknown"
	}
}

// options holds the internal configuration for Cache.
// All fields are unexported — access only via functional options.
type options struct {
	shardCount ShardCount

	// TTL
	ttl time.Duration // 0 means no TTL

	// Entry limit.
	// Enforced per-shard via atomic counters — approximate, not a hard guarantee.
	maxEntries int // 0 means unlimited

	// Eviction policy when shard limit is reached.
	evictionPolicy EvictionPolicy

	// LFU-specific tuning. Ignored unless evictionPolicy == EvictionLFU.
	lfuSampleSize    int           // number of entries sampled per eviction
	lfuAgingInterval time.Duration // how often counters are halved

	// Metrics
	metricsRecorder MetricsRecorder

	// Shrink. Always enabled. Round-robin, one shard per tick.
	shrinkCycleInterval time.Duration
	shrinkMinEntries    int

	// Active expiry (background sweep). Requires ttl > 0.
	activeExpiry         bool
	activeExpiryInterval time.Duration
}

// Option is a functional option for Cache.
type Option func(*options) error

// defaultOptions returns a safe, opinionated default configuration.
func defaultOptions() options {
	return options{
		shardCount:          ShardCountM,
		evictionPolicy:      EvictionReject,
		lfuSampleSize:       5,
		lfuAgingInterval:    60 * time.Second,
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
	if o.evictionPolicy == EvictionLFU && o.maxEntries == 0 {
		return errors.New("kahora: EvictionLFU requires WithMaxEntries to be set")
	}
	return nil
}

// --- Public Option constructors ---

// WithShardCount sets the number of shards.
// Use ShardCount* constants (XS/S/M/L/XL) or a custom positive power of two.
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
// Enforced per-shard via atomic counters — may be exceeded slightly under
// concurrent load. 0 means unlimited.
//
// Required for EvictionLFU. Without a limit, eviction has nothing to do.
func WithMaxEntries(n int) Option {
	return func(o *options) error {
		if n < 0 {
			return errors.New("kahora: max entries must be non-negative")
		}
		o.maxEntries = n
		return nil
	}
}

// WithEvictionPolicy sets the eviction policy used when a shard reaches
// its share of maxEntries.
//
// EvictionReject (default): Set returns ErrCapacityExceeded.
// EvictionLFU: evict the least-frequently-used entry from a random sample.
//
// EvictionLFU requires WithMaxEntries to be set.
//
// Note: enabling EvictionLFU disables read-write lock concurrency for Get —
// every Get must increment a counter under a write lock. Expect Get latency
// to increase noticeably under high read concurrency. Measure your workload.
func WithEvictionPolicy(p EvictionPolicy) Option {
	return func(o *options) error {
		switch p {
		case EvictionReject, EvictionLFU:
			o.evictionPolicy = p
			return nil
		default:
			return errors.New("kahora: unknown eviction policy")
		}
	}
}

// WithLFUSampleSize sets the number of random entries sampled when LFU
// chooses a victim. The entry with the lowest access counter among the
// sampled set is evicted.
//
// Larger sample → eviction decision closer to true LFU, higher hit rate,
// but more work per eviction (linear in sample size).
// Smaller sample → faster eviction, slightly worse hit rate.
//
// Default: 5. Redis uses 5 by default with similar reasoning.
// Range: 2 to 64. Values outside this range are rejected.
//
// Has no effect unless EvictionLFU is selected.
func WithLFUSampleSize(k int) Option {
	return func(o *options) error {
		if k < 2 || k > 64 {
			return errors.New("kahora: lfu sample size must be between 2 and 64")
		}
		o.lfuSampleSize = k
		return nil
	}
}

// WithLFUAgingInterval sets how often LFU counters are halved.
// Halving prevents unbounded counter growth and lets historically hot
// but currently cold keys be evicted again.
//
// Higher value = counters age slower, hot keys stay hot longer.
// Lower value = faster forgetting, more responsive to changing access patterns.
//
// Default: 60s.
//
// Has no effect unless EvictionLFU is selected.
func WithLFUAgingInterval(d time.Duration) Option {
	return func(o *options) error {
		if d <= 0 {
			return errors.New("kahora: lfu aging interval must be positive")
		}
		o.lfuAgingInterval = d
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
// shards and deletes expired entries.
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
