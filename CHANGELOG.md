# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

While the project is in `0.x`, the public API may change between minor versions.
Once `1.0.0` is released, breaking changes will only happen in major versions.

## [Unreleased]

### Added

- `WithLFUBufferSize(n)` — per-shard access-ring capacity (default 256).
- `WithLFUDrainAdaptive(min, max)` — adaptive drain cadence (default,
  50ms..1s).
- `WithLFUDrainInterval(d)` — fixed drain cadence, mutually exclusive with
  the adaptive form; last option set wins.
- `MetricsRecorder.RecordAccessesDropped(shard, dropped)` — surfaces access
  records that could not be buffered between drains. `Snapshot.AccessesDropped`
  aggregates it. Off the Get hot path; diagnostic only.

### Changed

- **Breaking (internal):** shard mutex reverted to `sync.RWMutex`. Reject-mode
  `Get` is back on the RLock hot path — restores v0.1.0-class parallel read
  throughput (~24 ns/op vs v0.2.0's ~68 ns/op at 10 cores).
- LFU `Get` now runs under RLock too. The counter update is deferred: `Get`
  records the accessed key into a per-shard ring; a background drainer folds
  the batch into `freq`. Counters lag reality by up to the drain interval and
  may drop accesses when the ring fills between drains — accepted imprecision
  in exchange for RLock-only reads.
- **Breaking (public interface):** `MetricsRecorder` gains
  `RecordAccessesDropped`. Third-party implementations must add a (no-op is
  fine) method.


## [0.2.0] - 2026-05-10

### Added

- `EvictionPolicy` type with two values:
  - `EvictionReject` (default) — preserves v0.1.0 behaviour, returns
    `ErrCapacityExceeded` when a shard is full.
  - `EvictionLFU` — sampled Least-Frequently-Used eviction. Each Get
    increments a per-key counter; on eviction, k random entries are sampled
    and the one with the lowest counter is removed. Counters are halved
    periodically to allow historically hot but currently cold keys to be
    evicted.
- `WithEvictionPolicy(p)` option.
- `WithLFUSampleSize(k)` option (default 5, range 2..64).
- `WithLFUAgingInterval(d)` option (default 60s).
- `MetricsRecorder.RecordEviction(shard)` — called on policy-driven eviction
  (distinct from TTL evictions).
- `Snapshot.Evictions` and `ShardSnapshot.Evictions` counters in
  `DefaultRecorder`.

### Changed

- **Breaking (internal):** shard mutex changed from `sync.RWMutex` to
  `sync.Mutex`. This is required for LFU's Get-time counter increment.
  Effect: `EvictionReject` workloads with very high read concurrency see
  ~30% lower throughput on parallel reads compared to v0.1.0.
  This is the cost of policy support; if you don't enable LFU, you still
  pay the lock change. Future versions may add a separate read-optimised
  path.
- `New` validates that `EvictionLFU` is paired with `WithMaxEntries`.

### Notes

- The public Cache API (`Get`, `Set`, `Delete`, `Close`) is unchanged.
- v0.1.0 users upgrading without enabling LFU only see the throughput
  change above. No code changes required.

## [0.1.0] - 2026-05-04

Initial release.

### Added

- Generic `Cache[K, V]` API with `Get`, `Set`, `Delete`, and `Close`.
- Sharded architecture with configurable shard count (`ShardCountXS`/`S`/`M`/`L`/`XL`,
  default 256). Power-of-two enforced for fast bitmask-based shard selection.
- Zero-allocation key hashing via `maphash.Comparable`.
- Optional TTL with both lazy expiry (on `Get`) and active expiry (background sweep).
- Gradual per-shard map reconstruction to reclaim memory without spikes.
  Three-phase shrink algorithm: snapshot → rebuild → delta-merge swap.
- Round-robin shrink scheduler — one shard reconstructed per tick,
  full cycle duration configurable via `WithShrinkCycleInterval` (default 60s).
- Approximate per-shard `maxEntries` enforcement via atomic counters.
- Monotonic clock for all TTL calculations — immune to NTP adjustments
  and wall clock jumps.
- `MetricsRecorder` interface for bring-your-own metrics backend.
- Built-in `DefaultRecorder` with thread-safe atomic counters and `Snapshot`
  method for direct inspection. Per-shard breakdown for distribution analysis.
- Functional options pattern with eager validation.
- Sentinel errors: `ErrCapacityExceeded`, `ErrClosed`.
- Cache-line padding on shards to prevent false sharing between cores.
- Comprehensive test suite including race detector tests.
- Benchmark suite covering hot paths, concurrency, shard counts, TTL,
  metrics overhead, and steady-state shrink pressure.
- CI on Go 1.25 and Go 1.26 with race detector, coverage, and lint jobs.

[Unreleased]: https://github.com/BawNer/kahora/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/BawNer/kahora/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/BawNer/kahora/releases/tag/v0.1.0