package kahora

import (
	"errors"
	"time"
)

type ShardCount int

const (
	ShardCountXS ShardCount = 16
	ShardCountS  ShardCount = 64
	ShardCountM  ShardCount = 256
	ShardCountL  ShardCount = 1024
	ShardCountXL ShardCount = 4096
)

// EvictionPolicy controls behaviour when Set hits the per-shard limit.
type EvictionPolicy int

const (
	EvictionReject EvictionPolicy = iota
	EvictionLFU
)

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

type options struct {
	shardCount ShardCount

	ttl        time.Duration
	maxEntries int

	evictionPolicy   EvictionPolicy
	lfuSampleSize    int
	lfuAgingInterval time.Duration

	// lfuDrainInterval > 0 → fixed drainer cadence.
	// Otherwise the drainer adapts inside [lfuDrainMinInterval, lfuDrainMaxInterval].
	lfuDrainInterval    time.Duration
	lfuDrainMinInterval time.Duration
	lfuDrainMaxInterval time.Duration
	lfuBufferSize       int

	metricsRecorder MetricsRecorder

	shrinkCycleInterval time.Duration
	shrinkMinEntries    int

	activeExpiry         bool
	activeExpiryInterval time.Duration
}

type Option func(*options) error

func defaultOptions() options {
	return options{
		shardCount:          ShardCountM,
		evictionPolicy:      EvictionReject,
		lfuSampleSize:       5,
		lfuAgingInterval:    60 * time.Second,
		lfuDrainMinInterval: 50 * time.Millisecond,
		lfuDrainMaxInterval: 1 * time.Second,
		lfuBufferSize:       256,
		shrinkCycleInterval: 60 * time.Second,
	}
}

func (o *options) validate() error {
	if o.activeExpiry && o.ttl == 0 {
		return errors.New("kahora: active expiry requires ttl")
	}
	if o.activeExpiry && o.activeExpiryInterval <= 0 {
		return errors.New("kahora: active expiry interval must be positive")
	}
	if o.evictionPolicy == EvictionLFU && o.maxEntries == 0 {
		return errors.New("kahora: EvictionLFU requires WithMaxEntries")
	}
	return nil
}

func WithShardCount(n ShardCount) Option {
	return func(o *options) error {
		if n <= 0 {
			return errors.New("kahora: shard count must be positive")
		}
		if !isPowerOfTwo(n) {
			return errors.New("kahora: shard count must be a power of two")
		}
		o.shardCount = n
		return nil
	}
}

func WithTTL(ttl time.Duration) Option {
	return func(o *options) error {
		if ttl <= 0 {
			return errors.New("kahora: ttl must be positive")
		}
		o.ttl = ttl
		return nil
	}
}

// WithMaxEntries caps total live entries. Enforced per-shard, so the global
// limit is approximate under concurrent load.
func WithMaxEntries(n int) Option {
	return func(o *options) error {
		if n < 0 {
			return errors.New("kahora: max entries must be non-negative")
		}
		o.maxEntries = n
		return nil
	}
}

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

// WithLFUSampleSize sets how many random entries are sampled to pick a
// victim. Larger = closer to true LFU, more work per eviction.
func WithLFUSampleSize(k int) Option {
	return func(o *options) error {
		if k < 2 || k > 64 {
			return errors.New("kahora: lfu sample size must be 2..64")
		}
		o.lfuSampleSize = k
		return nil
	}
}

// WithLFUAgingInterval sets how often LFU counters are halved. Without
// aging, hot-once keys live forever.
func WithLFUAgingInterval(d time.Duration) Option {
	return func(o *options) error {
		if d <= 0 {
			return errors.New("kahora: lfu aging interval must be positive")
		}
		o.lfuAgingInterval = d
		return nil
	}
}

// WithLFUDrainAdaptive lets the drainer's per-shard visit cadence float
// inside [minInterval, maxInterval] based on how full the access ring is at
// drain time. This is the default (50ms..1s). Mutually exclusive with
// WithLFUDrainInterval — last option set wins.
func WithLFUDrainAdaptive(minInterval, maxInterval time.Duration) Option {
	return func(o *options) error {
		if minInterval <= 0 || maxInterval <= 0 {
			return errors.New("kahora: lfu drain interval bounds must be positive")
		}
		if minInterval > maxInterval {
			return errors.New("kahora: lfu drain min must not exceed max")
		}
		o.lfuDrainMinInterval = minInterval
		o.lfuDrainMaxInterval = maxInterval
		o.lfuDrainInterval = 0
		return nil
	}
}

// WithLFUDrainInterval fixes the drainer's per-shard visit cadence.
// Mutually exclusive with WithLFUDrainAdaptive — last option set wins.
func WithLFUDrainInterval(d time.Duration) Option {
	return func(o *options) error {
		if d <= 0 {
			return errors.New("kahora: lfu drain interval must be positive")
		}
		o.lfuDrainInterval = d
		return nil
	}
}

// WithLFUBufferSize sets the per-shard access ring capacity. Larger = fewer
// dropped accesses under burst, more memory (shardCount * n * sizeof(K)).
// Default 256.
func WithLFUBufferSize(n int) Option {
	return func(o *options) error {
		if n <= 0 {
			return errors.New("kahora: lfu buffer size must be positive")
		}
		o.lfuBufferSize = n
		return nil
	}
}

func WithMetricsRecorder(r MetricsRecorder) Option {
	return func(o *options) error {
		if r == nil {
			return errors.New("kahora: metrics recorder must not be nil")
		}
		o.metricsRecorder = r
		return nil
	}
}

// WithShrinkCycleInterval is the time to walk all shards once.
// tick = cycleInterval / shardCount.
func WithShrinkCycleInterval(d time.Duration) Option {
	return func(o *options) error {
		if d <= 0 {
			return errors.New("kahora: shrink cycle interval must be positive")
		}
		o.shrinkCycleInterval = d
		return nil
	}
}

func WithShrinkMinEntries(n int) Option {
	return func(o *options) error {
		if n < 0 {
			return errors.New("kahora: shrink min entries must be non-negative")
		}
		o.shrinkMinEntries = n
		return nil
	}
}

// WithActiveExpiry enables a background sweep that removes expired entries.
// Without it, expiry is lazy (only on Get).
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
