# kahora

A high-performance, sharded, in-memory cache for Go — with **gradual map shrink** to prevent unbounded memory growth.

```go
import "github.com/BawNer/kahora"
```

[![Go Reference](https://pkg.go.dev/badge/github.com/BawNer/kahora.svg)](https://pkg.go.dev/github.com/BawNer/kahora)
[![Go Report Card](https://goreportcard.com/badge/github.com/BawNer/kahora)](https://goreportcard.com/report/github.com/BawNer/kahora)
[![CI](https://github.com/BawNer/kahora/actions/workflows/ci.yml/badge.svg)](https://github.com/BawNer/kahora/actions/workflows/ci.yml)

---

## Why kahora

Go's built-in `map` does not shrink after entries are deleted or expire. In long-running services with high TTL turnover and millions of entries, this leads to unbounded heap growth — even when the live entry count is small.

kahora reconstructs shard maps gradually in the background — one shard per tick — to reclaim memory **without spikes**. It's designed for the painful case nobody talks about: 13–18M entries per pod, 300k+ RPS, and Go maps that never give memory back.

If you don't have this problem, you probably don't need kahora. `sync.Map` or a plain `map[K]V` with a mutex is simpler and just as fast at sub-million entry counts.

---

## Design principles

- **Simple API** — generic `Cache[K, V]`, three methods: `Get`, `Set`, `Delete`.
- **Honest metrics** — bring your own `MetricsRecorder`, or use the built-in one.
- **No magic** — no auto-tuning, no hidden goroutines per shard, no surprises.
- **Fail fast** — invalid options return errors at `New`, not at first use.
- **For developers, not over them** — every knob is documented; defaults are opinionated but overridable.

---

## Quick start

```go
package main

import (
    "log"
    "time"

    "github.com/BawNer/kahora"
)

func main() {
    c, err := kahora.New[string, []byte](
        kahora.WithShardCount(kahora.ShardCountM), // 256 shards
        kahora.WithTTL(5 * time.Minute),
        kahora.WithActiveExpiry(30 * time.Second),
        kahora.WithMaxEntries(10_000_000),
    )
    if err != nil {
        log.Fatal(err)
    }
    defer c.Close()

    c.Set("user:42", payload)

    if v, ok := c.Get("user:42"); ok {
        use(v)
    }
}
```

---

## Architecture

### Sharding

The cache is partitioned into N independent shards (default 256). Each shard has its own `RWMutex`, its own map, and its own atomic counters. Operations on different shards never contend.

Sharding uses `maphash.Comparable` for zero-allocation key hashing. Shard count must be a power of two — enforced at `New`.

| ShardCount | Value | Use case |
|------------|-------|----------|
| `ShardCountXS` | 16 | Small caches, low concurrency |
| `ShardCountS` | 64 | Moderate concurrency |
| `ShardCountM` | 256 | **Default** — high concurrency |
| `ShardCountL` | 1024 | Very high concurrency, large memory budget |
| `ShardCountXL` | 4096 | Extreme concurrency |

### Gradual shrink

Shrink runs on a single background goroutine, processing one shard per tick via round-robin. The full cycle duration is configurable via `WithShrinkCycleInterval` (default 60s).

Each shrink uses a three-phase approach to minimise lock hold time:

1. **Phase 1 (write lock, brief):** snapshot live entries, set the shrinking flag.
2. **Phase 2 (no lock):** build a new map from the snapshot — the slow part.
3. **Phase 3 (write lock, brief):** delta-merge keys mutated during phase 2, swap atomically.

A `dirty` set tracks keys written or deleted during phase 2, ensuring no mutation is lost even under heavy concurrent load.

### Monotonic clock

All time measurements use a monotonic clock derived from `time.Since(processStart)`. NTP adjustments, leap seconds, and manual time changes do not affect TTL behaviour.

### TTL

TTL is optional. When enabled:

- **Lazy expiry** is always on — expired entries are removed on `Get`.
- **Active expiry** is opt-in via `WithActiveExpiry(interval)` — a background sweep removes expired entries proactively.

Without active expiry, expired entries occupy memory until either a `Get` removes them or shrink reconstructs the shard.

---

## Configuration

All options validate eagerly. `New` returns an error if any option is invalid or options are logically inconsistent.

| Option | Default | Description |
|--------|---------|-------------|
| `WithShardCount(n)` | `ShardCountM` (256) | Number of shards. Must be power of two. |
| `WithTTL(d)` | none | Global TTL for all entries. |
| `WithActiveExpiry(interval)` | disabled | Background sweep interval. Requires TTL. |
| `WithMaxEntries(n)` | unlimited | Approximate global entry cap (per-shard enforced). |
| `WithMetricsRecorder(r)` | nop | Custom metrics backend. |
| `WithShrinkCycleInterval(d)` | 60s | Full shrink cycle duration across all shards. |
| `WithShrinkMinEntries(n)` | 0 | Skip shrink if shard has fewer live entries than this. |

### Recommended for production

```go
kahora.New[K, V](
    kahora.WithShardCount(kahora.ShardCountM),
    kahora.WithMaxEntries(estimatedPeak),  // pre-allocates maps, avoids growth
    kahora.WithTTL(ttl),
    kahora.WithActiveExpiry(ttl / 4),
    kahora.WithMetricsRecorder(recorder),
)
```

`WithMaxEntries` is especially important — it pre-allocates each shard's map to the expected size, avoiding the cost of incremental rehashing during steady-state writes.

---

## Metrics

kahora exposes a `MetricsRecorder` interface. All methods receive a shard index, allowing per-shard observability without kahora itself aggregating anything.

```go
type MetricsRecorder interface {
    RecordHit(shard int)
    RecordMiss(shard int)
    RecordSet(shard int)
    RecordDelete(shard int)
    RecordLazyEviction(shard int)
    RecordActiveEviction(shard int)
    RecordShrink(shard, before, after int)
    RecordCapacityExceeded(shard int)
}
```

Implement this against your metrics backend (Prometheus, OpenTelemetry, StatsD, etc).

### Built-in DefaultRecorder

If you don't need a specific backend, use the built-in `DefaultRecorder`:

```go
r := kahora.NewRecorder(kahora.ShardCountM)
c, _ := kahora.New[K, V](kahora.WithMetricsRecorder(r))

// ... later ...
snap := r.Snapshot()
fmt.Printf("hits=%d misses=%d shrinks=%d\n",
    snap.Hits, snap.Misses, len(snap.Shards))

// Per-shard breakdown for distribution analysis
for _, s := range snap.Shards {
    fmt.Printf("shard %d: hits=%d sets=%d shrinks=%d\n",
        s.Index, s.Hits, s.Sets, s.ShrinkCount)
}
```

Per-shard counters reveal whether keys are distributed uniformly. If one shard sees 10x more traffic than its peers, your hash function or key distribution may be off.

### Reading the metrics

| Signal | Meaning |
|--------|---------|
| High `LazyEvictions` rate | Active expiry is too infrequent or disabled. |
| High `CapacityExceeded` | `maxEntries` is too low for your workload. |
| Uneven per-shard `Sets` | Keys are not distributed uniformly. |
| Large `LastShrinkBefore - LastShrinkAfter` | Shrink is reclaiming significant memory — TTL turnover is high. |

---

## Benchmarks

Measured on Apple M1 Pro, Go 1.25.

```
BenchmarkGetHit-10                  39.45 ns/op   0 B/op   0 allocs/op
BenchmarkGetMiss-10                 31.54 ns/op   0 B/op   0 allocs/op
BenchmarkSet-10                    245.4  ns/op  78 B/op   0 allocs/op
BenchmarkSetOverwrite-10            30.65 ns/op   0 B/op   0 allocs/op

BenchmarkParallelGetHit-10          23.41 ns/op   0 B/op   0 allocs/op
BenchmarkParallelMixed-10           27.05 ns/op   0 B/op   0 allocs/op  (95% read / 5% write)
BenchmarkParallelSet-10             96.89 ns/op  96 B/op   0 allocs/op

BenchmarkGetWithTTL-10              38.26 ns/op   0 B/op   0 allocs/op
BenchmarkGetWithDefaultRecorder-10  43.08 ns/op   0 B/op   0 allocs/op
BenchmarkGetStringKey-10            43.49 ns/op   0 B/op   0 allocs/op
```

### Read scaling (ParallelGetHit)

| Cores | ns/op | Speedup |
|-------|-------|---------|
| 1 | 144.9 | 1.00x |
| 2 | 88.08 | 1.65x |
| 4 | 46.03 | 3.15x |
| 8 | 30.45 | 4.76x |
| 16 | 21.85 | 6.63x |

### Shard count comparison (parallel reads)

| Shards | ns/op |
|--------|-------|
| 16 | 25.53 |
| 64 | 17.07 |
| 256 | 15.35 |
| 1024 | 14.64 |
| 4096 | 14.30 |

The 256-shard default captures most of the benefit; going higher costs memory for diminishing returns.

Run benchmarks yourself:

```bash
go test -bench=. -benchmem -benchtime=3s ./...
go test -bench=Parallel -benchmem -cpu=1,2,4,8,16 ./...
```

---

## When NOT to use kahora

- **You have fewer than ~1M entries.** Plain `map[K]V` with a mutex, or `sync.Map`, is simpler and likely faster.
- **You need byte-aware eviction.** kahora limits entry count, not bytes. Wrap it or use a different library.
- **You need a distributed cache.** kahora is in-process only. Use Redis or similar.
- **You need LRU/LFU eviction.** kahora evicts only by TTL or explicit `Delete`. There is no recency-based eviction.

---

## Caveats

- **Approximate `maxEntries`.** Enforced per-shard via atomic counters. The global limit may be exceeded slightly under concurrent load — this is intentional, avoiding a global lock on the hot path is worth the imprecision.
- **Shrink briefly blocks readers.** Phase 1 takes a write lock to snapshot the shard. For 50k entries this is ~1–3 ms once per shrink cycle per shard. With a 60s cycle and 256 shards, any given shard is blocked once per minute.
- **`Get` after `Close`.** Get and Delete remain safe after Close, but operate on a static snapshot — no further eviction or shrink occurs.
- **Power-of-two shard counts only.** Required for fast bitmask-based shard selection. Validated at `New`.

---

## License

MIT — see [LICENSE](LICENSE).