// Package kahora is a high-performance, sharded, in-memory cache for Go.
//
// kahora targets a specific pain point: Go's built-in map does not shrink
// after entries are deleted or expire, leading to unbounded memory growth
// in long-running services with high TTL turnover. kahora reconstructs
// shard maps gradually in the background — one shard per tick — to reclaim
// memory without spikes.
//
// # Design
//
// The cache is sharded into N independent partitions (default 256).
// Each shard has its own Mutex, its own map, and its own atomic counters.
// Operations on different shards never contend. Sharding uses
// maphash.Comparable for zero-allocation key hashing.
//
// All time measurements use a monotonic clock derived from time.Since,
// immune to NTP adjustments and wall clock jumps.
//
// # Eviction policies
//
// Two policies are available, selected via WithEvictionPolicy:
//
//   - EvictionReject (default): when a shard reaches its share of maxEntries,
//     Set returns ErrCapacityExceeded. The caller decides what to do.
//     Lowest overhead, no per-key bookkeeping.
//
//   - EvictionLFU: sampled Least-Frequently-Used. Each Get increments a
//     per-key counter; on eviction, k random entries are sampled and the
//     one with the lowest counter is removed. Counters are halved
//     periodically to age out historically hot keys.
//     Requires WithMaxEntries.
//
// # Shrink
//
// Shrink runs on a single background goroutine, processing one shard per tick
// via round-robin. The full cycle duration is configurable (default 60s).
//
// Each shrink uses a three-phase approach to minimise lock hold time:
//
//  1. Snapshot live entries under lock (brief).
//  2. Build a new map from the snapshot without holding any lock.
//  3. Delta-merge keys mutated during phase 2 and swap atomically (brief).
//
// A "dirty" set tracks keys written or deleted during phase 2, ensuring
// no mutation is lost even under heavy concurrent load.
//
// # TTL
//
// TTL is optional. When enabled, expired entries are removed lazily on Get,
// or proactively by the active expiry sweep if WithActiveExpiry is set.
//
// # Metrics
//
// kahora exposes a MetricsRecorder interface — bring your own backend.
// A DefaultRecorder is provided with thread-safe counters and a Snapshot
// method for direct inspection. If no recorder is set, metrics are discarded
// at zero cost (the no-op implementation is inlined by the compiler).
//
// # Quick start
//
//	c, err := kahora.New[string, []byte](
//	    kahora.WithShardCount(kahora.ShardCountM),
//	    kahora.WithTTL(5 * time.Minute),
//	    kahora.WithActiveExpiry(30 * time.Second),
//	    kahora.WithMaxEntries(10_000_000),
//	    kahora.WithEvictionPolicy(kahora.EvictionLFU),
//	)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer c.Close()
//
//	c.Set("user:42", payload)
//	if v, ok := c.Get("user:42"); ok {
//	    use(v)
//	}
//
// # When to use kahora
//
// kahora is designed for read-heavy in-memory caches with millions of entries
// and high TTL turnover, where Go's native map memory behaviour becomes
// a problem. It is not a distributed cache, not a byte-aware cache, and not
// a replacement for Redis. For sub-million entry counts, the standard library
// sync.Map or a simple map+mutex will likely be simpler and equally fast.
package kahora
