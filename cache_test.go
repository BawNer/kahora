package kahora_test

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/BawNer/kahora"
)

func newCache(t *testing.T, opts ...kahora.Option) *kahora.Cache[string, string] {
	t.Helper()
	c, err := kahora.New[string, string](opts...)
	if err != nil {
		t.Fatalf("kahora.New: %v", err)
	}
	t.Cleanup(c.Close)
	return c
}

// --- Basic API ---

func TestGetMiss(t *testing.T) {
	c := newCache(t)
	_, ok := c.Get("missing")
	if ok {
		t.Fatal("expected miss, got hit")
	}
}

func TestSetGet(t *testing.T) {
	c := newCache(t)
	if err := c.Set("k", "v"); err != nil {
		t.Fatalf("Set: %v", err)
	}
	v, ok := c.Get("k")
	if !ok {
		t.Fatal("expected hit, got miss")
	}
	if v != "v" {
		t.Fatalf("expected %q, got %q", "v", v)
	}
}

func TestOverwrite(t *testing.T) {
	c := newCache(t)
	c.Set("k", "first")
	c.Set("k", "second")
	v, _ := c.Get("k")
	if v != "second" {
		t.Fatalf("expected %q, got %q", "second", v)
	}
}

func TestDelete(t *testing.T) {
	c := newCache(t)
	c.Set("k", "v")
	c.Delete("k")
	_, ok := c.Get("k")
	if ok {
		t.Fatal("expected miss after delete, got hit")
	}
}

func TestDeleteNonExistent(t *testing.T) {
	c := newCache(t)
	// Should not panic.
	c.Delete("ghost")
}

// --- TTL ---

func TestTTLExpiry(t *testing.T) {
	c := newCache(t, kahora.WithTTL(50*time.Millisecond))
	c.Set("k", "v")

	// Should be alive immediately.
	if _, ok := c.Get("k"); !ok {
		t.Fatal("expected hit before TTL, got miss")
	}

	time.Sleep(100 * time.Millisecond)

	// Should be expired now.
	if _, ok := c.Get("k"); ok {
		t.Fatal("expected miss after TTL, got hit")
	}
}

func TestNoTTLEntryPersists(t *testing.T) {
	c := newCache(t)
	c.Set("k", "v")
	time.Sleep(20 * time.Millisecond)
	if _, ok := c.Get("k"); !ok {
		t.Fatal("expected entry without TTL to persist")
	}
}

func TestActiveExpiry(t *testing.T) {
	c := newCache(t,
		kahora.WithTTL(30*time.Millisecond),
		kahora.WithActiveExpiry(20*time.Millisecond),
	)
	c.Set("k", "v")
	time.Sleep(100 * time.Millisecond)

	// After TTL + sweep, entry should be gone even without a Get.
	// We verify indirectly via metrics.
	// Direct map access is internal — so we just verify Get misses.
	_, ok := c.Get("k")
	if ok {
		t.Fatal("expected miss after active expiry, got hit")
	}
}

// --- Capacity ---

func TestMaxEntriesExceeded(t *testing.T) {
	// Use XS shards and a small limit so we can fill one shard deterministically.
	c := newCache(t,
		kahora.WithShardCount(kahora.ShardCountXS),
		kahora.WithMaxEntries(16),
	)

	var exceeded bool
	for i := range 100 {
		key := string(rune('a' + i%26))
		err := c.Set(key+string(rune('0'+i%10)), "v")
		if errors.Is(err, kahora.ErrCapacityExceeded) {
			exceeded = true
			break
		}
	}
	if !exceeded {
		t.Fatal("expected ErrCapacityExceeded, never got it")
	}
}

// --- Close ---

func TestCloseIdempotent(t *testing.T) {
	c := newCache(t)
	c.Close()
	c.Close() // must not panic
}

func TestSetAfterClose(t *testing.T) {
	c := newCache(t)
	c.Close()
	err := c.Set("k", "v")
	if !errors.Is(err, kahora.ErrClosed) {
		t.Fatalf("expected ErrClosed, got %v", err)
	}
}

func TestGetAfterClose(t *testing.T) {
	c := newCache(t)
	c.Set("k", "v")
	c.Close()
	// Get should still return the value — static snapshot.
	v, ok := c.Get("k")
	if !ok {
		t.Fatal("expected hit after close, got miss")
	}
	if v != "v" {
		t.Fatalf("expected %q, got %q", "v", v)
	}
}

// --- Options validation ---

func TestInvalidOptions(t *testing.T) {
	tests := []struct {
		name string
		opts []kahora.Option
	}{
		{"negative ttl", []kahora.Option{kahora.WithTTL(-1)}},
		{"zero ttl", []kahora.Option{kahora.WithTTL(0)}},
		{"negative max entries", []kahora.Option{kahora.WithMaxEntries(-1)}},
		{"non power of two shards", []kahora.Option{kahora.WithShardCount(100)}},
		{"active expiry without ttl", []kahora.Option{kahora.WithActiveExpiry(time.Second)}},
		{"nil metrics recorder", []kahora.Option{kahora.WithMetricsRecorder(nil)}},
		{"zero shrink cycle", []kahora.Option{kahora.WithShrinkCycleInterval(0)}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := kahora.New[string, string](tt.opts...)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
		})
	}
}

// --- Metrics ---

func TestDefaultRecorderHitsAndMisses(t *testing.T) {
	r := kahora.NewRecorder(kahora.ShardCountXS)
	c := newCache(t,
		kahora.WithShardCount(kahora.ShardCountXS),
		kahora.WithMetricsRecorder(r),
	)

	c.Set("k", "v")
	c.Get("k")       // hit
	c.Get("missing") // miss

	snap := r.Snapshot()
	if snap.Hits != 1 {
		t.Errorf("expected 1 hit, got %d", snap.Hits)
	}
	if snap.Misses != 1 {
		t.Errorf("expected 1 miss, got %d", snap.Misses)
	}
	if snap.Sets != 1 {
		t.Errorf("expected 1 set, got %d", snap.Sets)
	}
}

func TestDefaultRecorderLazyEviction(t *testing.T) {
	r := kahora.NewRecorder(kahora.ShardCountXS)
	c := newCache(t,
		kahora.WithShardCount(kahora.ShardCountXS),
		kahora.WithTTL(30*time.Millisecond),
		kahora.WithMetricsRecorder(r),
	)

	c.Set("k", "v")
	time.Sleep(60 * time.Millisecond)
	c.Get("k") // triggers lazy eviction

	snap := r.Snapshot()
	if snap.LazyEvictions != 1 {
		t.Errorf("expected 1 lazy eviction, got %d", snap.LazyEvictions)
	}
}

func TestDefaultRecorderPerShardDistribution(t *testing.T) {
	r := kahora.NewRecorder(kahora.ShardCountXS)
	c := newCache(t,
		kahora.WithShardCount(kahora.ShardCountXS),
		kahora.WithMetricsRecorder(r),
	)

	// Write enough keys to spread across shards.
	for i := range 160 {
		c.Set(string(rune(i)), "v")
	}

	snap := r.Snapshot()
	if len(snap.Shards) != int(kahora.ShardCountXS) {
		t.Fatalf("expected %d shard snapshots, got %d", kahora.ShardCountXS, len(snap.Shards))
	}

	// Verify at least some shards received writes — distribution is not all-zero.
	nonZero := 0
	for _, s := range snap.Shards {
		if s.Sets > 0 {
			nonZero++
		}
	}
	if nonZero == 0 {
		t.Fatal("expected some shards to have sets, all are zero")
	}
}

// --- Concurrency ---

// TestConcurrentSetGet verifies no data races under heavy concurrent load.
// Run with: go test -race ./...
func TestConcurrentSetGet(t *testing.T) {
	c := newCache(t, kahora.WithShardCount(kahora.ShardCountS))
	var wg sync.WaitGroup
	workers := 100
	ops := 500

	for i := range workers {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := range ops {
				key := string(rune('a' + (id*ops+j)%26))
				c.Set(key, "value")
				c.Get(key)
			}
		}(i)
	}
	wg.Wait()
}

// TestConcurrentSetDelete verifies no data races between Set and Delete.
func TestConcurrentSetDelete(t *testing.T) {
	c := newCache(t)
	var wg sync.WaitGroup

	for i := range 50 {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			c.Set(string(rune('a'+i%26)), "v")
		}(i)
		go func(i int) {
			defer wg.Done()
			c.Delete(string(rune('a' + i%26)))
		}(i)
	}
	wg.Wait()
}
