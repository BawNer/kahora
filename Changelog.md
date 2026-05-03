# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

While the project is in `0.x`, the public API may change between minor versions.
Once `1.0.0` is released, breaking changes will only happen in major versions.

## [Unreleased]

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
- Functional options pattern with eager validation — `New` returns an error
  if any option is invalid or options are logically inconsistent.
- Sentinel errors: `ErrCapacityExceeded`, `ErrClosed`.
- Cache-line padding on shards to prevent false sharing between cores.
- Comprehensive test suite including race detector tests and concurrent
  shrink/delta-merge invariant checks.
- Benchmark suite covering hot paths, concurrency scaling, shard counts,
  TTL overhead, metrics overhead, and steady-state shrink pressure.
- CI on Go 1.25 and Go 1.26 with race detector, coverage, and lint jobs.

[Unreleased]: https://github.com/BawNer/kahora/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/BawNer/kahora/releases/tag/v0.1.0