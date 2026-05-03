package kahora_test

import (
	"runtime"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/BawNer/kahora"
)

// --- Hot path benchmarks ---

// BenchmarkGetHit measures Get on existing keys — the hottest path.
// Pre-populates the cache, then reads in a tight loop.
func BenchmarkGetHit(b *testing.B) {
	c, err := kahora.New[int, int](kahora.WithShardCount(kahora.ShardCountM))
	if err != nil {
		b.Fatal(err)
	}
	defer c.Close()

	const n = 100_000
	for i := range n {
		c.Set(i, i)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; b.Loop(); i++ {
		c.Get(i % n)
	}
}

// BenchmarkGetMiss measures Get on missing keys —
// stresses the lookup path without value copy.
func BenchmarkGetMiss(b *testing.B) {
	c, err := kahora.New[int, int]()
	if err != nil {
		b.Fatal(err)
	}
	defer c.Close()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; b.Loop(); i++ {
		c.Get(i)
	}
}

// BenchmarkSet measures Set on new keys —
// the hottest write path including allocation.
func BenchmarkSet(b *testing.B) {
	c, err := kahora.New[int, int]()
	if err != nil {
		b.Fatal(err)
	}
	defer c.Close()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; b.Loop(); i++ {
		c.Set(i, i)
	}
}

// BenchmarkSetOverwrite measures Set on existing keys — no count change.
// Isolates pure write cost from map growth.
func BenchmarkSetOverwrite(b *testing.B) {
	c, err := kahora.New[int, int]()
	if err != nil {
		b.Fatal(err)
	}
	defer c.Close()

	const n = 10_000
	for i := range n {
		c.Set(i, i)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; b.Loop(); i++ {
		c.Set(i%n, i)
	}
}

// --- Concurrent benchmarks ---

// BenchmarkParallelGetHit simulates the realistic 300k+ RPS scenario:
// many goroutines reading from a populated cache.
func BenchmarkParallelGetHit(b *testing.B) {
	c, err := kahora.New[int, int](kahora.WithShardCount(kahora.ShardCountM))
	if err != nil {
		b.Fatal(err)
	}
	defer c.Close()

	const n = 1_000_000
	for i := range n {
		c.Set(i, i)
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			c.Get(i % n)
			i++
		}
	})
}

// BenchmarkParallelMixed simulates a realistic read-heavy workload:
// 95% Get, 5% Set. Tight contention on shared keys.
func BenchmarkParallelMixed(b *testing.B) {
	c, err := kahora.New[int, int](kahora.WithShardCount(kahora.ShardCountM))
	if err != nil {
		b.Fatal(err)
	}
	defer c.Close()

	const n = 1_000_000
	for i := range n {
		c.Set(i, i)
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%20 == 0 {
				c.Set(i%n, i)
			} else {
				c.Get(i % n)
			}
			i++
		}
	})
}

// BenchmarkParallelSet — pure write contention.
// Worst case for sharding effectiveness.
func BenchmarkParallelSet(b *testing.B) {
	c, err := kahora.New[int, int](kahora.WithShardCount(kahora.ShardCountM))
	if err != nil {
		b.Fatal(err)
	}
	defer c.Close()

	b.ResetTimer()
	b.ReportAllocs()

	var counter atomic.Int64
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := counter.Add(1)
			c.Set(int(i), int(i))
		}
	})
}

// --- Shard count comparison ---

// BenchmarkShardCounts compares contention across XS/S/M/L/XL.
// Helps tune the default and understand the trade-off.
func BenchmarkShardCounts(b *testing.B) {
	cases := []struct {
		name  string
		count kahora.ShardCount
	}{
		{"XS_16", kahora.ShardCountXS},
		{"S_64", kahora.ShardCountS},
		{"M_256", kahora.ShardCountM},
		{"L_1024", kahora.ShardCountL},
		{"XL_4096", kahora.ShardCountXL},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			c, err := kahora.New[int, int](kahora.WithShardCount(tc.count))
			if err != nil {
				b.Fatal(err)
			}
			defer c.Close()

			const n = 100_000
			for i := range n {
				c.Set(i, i)
			}

			b.ResetTimer()
			b.ReportAllocs()

			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					c.Get(i % n)
					i++
				}
			})
		})
	}
}

// --- Key types ---

// BenchmarkGetStringKey measures hashing cost for string keys vs int keys.
// String hashing is more expensive — useful to know the overhead.
func BenchmarkGetStringKey(b *testing.B) {
	c, err := kahora.New[string, int]()
	if err != nil {
		b.Fatal(err)
	}
	defer c.Close()

	const n = 100_000
	keys := make([]string, n)
	for i := range n {
		keys[i] = "user:session:" + strconv.Itoa(i)
		c.Set(keys[i], i)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; b.Loop(); i++ {
		c.Get(keys[i%n])
	}
}

// --- TTL ---

// BenchmarkGetWithTTL measures Get overhead when TTL is enabled —
// adds the isExpired check on every hit.
func BenchmarkGetWithTTL(b *testing.B) {
	c, err := kahora.New[int, int](kahora.WithTTL(1 * time.Hour))
	if err != nil {
		b.Fatal(err)
	}
	defer c.Close()

	const n = 100_000
	for i := range n {
		c.Set(i, i)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; b.Loop(); i++ {
		c.Get(i % n)
	}
}

// --- Metrics overhead ---

// BenchmarkGetWithDefaultRecorder measures the overhead of DefaultRecorder
// vs nopRecorder. Critical signal — if metrics double the latency,
// users will hesitate to enable them.
func BenchmarkGetWithDefaultRecorder(b *testing.B) {
	r := kahora.NewRecorder(kahora.ShardCountM)
	c, err := kahora.New[int, int](
		kahora.WithShardCount(kahora.ShardCountM),
		kahora.WithMetricsRecorder(r),
	)
	if err != nil {
		b.Fatal(err)
	}
	defer c.Close()

	const n = 100_000
	for i := range n {
		c.Set(i, i)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; b.Loop(); i++ {
		c.Get(i % n)
	}
}

// --- Memory and shrink ---

// BenchmarkShrinkCycle measures the cost of one full shrink cycle
// across all shards under a realistic distribution.
//
// Methodology: fill, expire half, force shrink across all shards,
// measure total time and resulting heap size.
func BenchmarkShrinkCycle(b *testing.B) {
	for i := 0; b.Loop(); i++ {
		c, err := kahora.New[int, int](
			kahora.WithShardCount(kahora.ShardCountM),
			kahora.WithTTL(50*time.Millisecond),
			kahora.WithShrinkCycleInterval(100*time.Millisecond),
			kahora.WithActiveExpiry(20*time.Millisecond),
		)
		if err != nil {
			b.Fatal(err)
		}

		// Fill cache.
		for j := range 100_000 {
			c.Set(j, j)
		}

		// Wait for entries to expire and shrink to run a full cycle.
		time.Sleep(200 * time.Millisecond)

		c.Close()
	}
}

// BenchmarkSteadyStateShrink simulates the production scenario:
// constant write/expire churn while shrink runs in background.
// We measure Get latency under shrink pressure.
func BenchmarkSteadyStateShrink(b *testing.B) {
	c, err := kahora.New[int, int](
		kahora.WithShardCount(kahora.ShardCountM),
		kahora.WithTTL(100*time.Millisecond),
		kahora.WithShrinkCycleInterval(500*time.Millisecond),
		kahora.WithActiveExpiry(50*time.Millisecond),
	)
	if err != nil {
		b.Fatal(err)
	}
	defer c.Close()

	// Background writer to keep churn going.
	stopChurn := make(chan struct{})
	go func() {
		i := 0
		for {
			select {
			case <-stopChurn:
				return
			default:
				c.Set(i, i)
				i++
				if i%10_000 == 0 {
					runtime.Gosched()
				}
			}
		}
	}()
	defer close(stopChurn)

	// Let churn warm up.
	time.Sleep(100 * time.Millisecond)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; b.Loop(); i++ {
		c.Get(i)
	}
}
