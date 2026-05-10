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

func BenchmarkGetHitLFU(b *testing.B) {
	c, err := kahora.New[int, int](
		kahora.WithShardCount(kahora.ShardCountM),
		kahora.WithMaxEntries(200_000),
		kahora.WithEvictionPolicy(kahora.EvictionLFU),
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

// BenchmarkSetWithEviction measures Set cost when the cache is at limit
// and LFU is evicting on every insert. This is the worst case for LFU.
func BenchmarkSetWithEviction(b *testing.B) {
	c, err := kahora.New[int, int](
		kahora.WithShardCount(kahora.ShardCountM),
		kahora.WithMaxEntries(10_000),
		kahora.WithEvictionPolicy(kahora.EvictionLFU),
	)
	if err != nil {
		b.Fatal(err)
	}
	defer c.Close()

	// Pre-fill to capacity so every Set triggers eviction.
	for i := range 10_000 {
		c.Set(i, i)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; b.Loop(); i++ {
		c.Set(100_000+i, i)
	}
}

// --- Concurrent benchmarks ---

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

func BenchmarkParallelGetHitLFU(b *testing.B) {
	c, err := kahora.New[int, int](
		kahora.WithShardCount(kahora.ShardCountM),
		kahora.WithMaxEntries(2_000_000),
		kahora.WithEvictionPolicy(kahora.EvictionLFU),
	)
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

		for j := range 100_000 {
			c.Set(j, j)
		}

		time.Sleep(200 * time.Millisecond)

		c.Close()
	}
}

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

	time.Sleep(100 * time.Millisecond)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; b.Loop(); i++ {
		c.Get(i)
	}
}
