package kahora

import (
	"sync"
	"testing"
	"unsafe"
)

// TestShardCacheLineAlignment verifies that shard is padded to a multiple of
// 64 bytes to prevent false sharing between adjacent shards on different CPU cores.
// If this test fails, adjust shardPadding in shard.go.
func TestShardCacheLineAlignment(t *testing.T) {
	size := shardSize[string, string]()
	if size%cacheLineSize != 0 {
		t.Fatalf("shard size %d is not a multiple of cache line size %d — adjust shardPadding in shard.go", size, cacheLineSize)
	}
}

// TestShardGetMiss verifies that Get on an empty shard returns zero value and false.
func TestShardGetMiss(t *testing.T) {
	s := newShard[string, string](0)
	v, ok := s.get("key", monoNow(), nopRecorder{}, 0)
	if ok {
		t.Fatal("expected miss, got hit")
	}
	if v != "" {
		t.Fatalf("expected zero value, got %q", v)
	}
}

// TestShardSetGet verifies basic set/get round-trip.
func TestShardSetGet(t *testing.T) {
	s := newShard[string, string](0)
	if err := s.set("key", "value", 0, 0, nopRecorder{}, 0); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	v, ok := s.get("key", monoNow(), nopRecorder{}, 0)
	if !ok {
		t.Fatal("expected hit, got miss")
	}
	if v != "value" {
		t.Fatalf("expected %q, got %q", "value", v)
	}
}

// TestShardOverwrite verifies that Set overwrites existing entries.
func TestShardOverwrite(t *testing.T) {
	s := newShard[string, string](0)
	s.set("key", "first", 0, 0, nopRecorder{}, 0)
	s.set("key", "second", 0, 0, nopRecorder{}, 0)
	v, ok := s.get("key", monoNow(), nopRecorder{}, 0)
	if !ok {
		t.Fatal("expected hit, got miss")
	}
	if v != "second" {
		t.Fatalf("expected %q, got %q", "second", v)
	}
}

// TestShardDelete verifies that deleted keys are no longer returned.
func TestShardDelete(t *testing.T) {
	s := newShard[string, string](0)
	s.set("key", "value", 0, 0, nopRecorder{}, 0)
	s.delete("key", nopRecorder{}, 0)
	_, ok := s.get("key", monoNow(), nopRecorder{}, 0)
	if ok {
		t.Fatal("expected miss after delete, got hit")
	}
}

// TestShardDeleteNonExistent verifies that deleting a missing key is a no-op.
func TestShardDeleteNonExistent(t *testing.T) {
	s := newShard[string, string](0)
	// Should not panic or error.
	s.delete("ghost", nopRecorder{}, 0)
}

// TestShardLazyExpiry verifies that expired entries are removed on Get.
func TestShardLazyExpiry(t *testing.T) {
	s := newShard[string, string](0)
	// Set entry that already expired (expiresAt in the past).
	past := monoNow() - 1
	s.set("key", "value", past, 0, nopRecorder{}, 0)

	_, ok := s.get("key", monoNow(), nopRecorder{}, 0)
	if ok {
		t.Fatal("expected miss for expired entry, got hit")
	}

	// Entry should be gone from the map.
	s.mu.RLock()
	_, exists := s.data["key"]
	s.mu.RUnlock()
	if exists {
		t.Fatal("expected expired entry to be removed from map")
	}
}

// TestShardCapacityLimit verifies that Set respects the per-shard entry limit.
func TestShardCapacityLimit(t *testing.T) {
	s := newShard[string, string](0)
	limit := 2
	s.set("a", "1", 0, limit, nopRecorder{}, 0)
	s.set("b", "2", 0, limit, nopRecorder{}, 0)
	err := s.set("c", "3", 0, limit, nopRecorder{}, 0)
	if err != ErrCapacityExceeded {
		t.Fatalf("expected ErrCapacityExceeded, got %v", err)
	}
}

// TestShardCountAccuracy verifies that the count atomic stays accurate
// across set, overwrite, and delete operations.
func TestShardCountAccuracy(t *testing.T) {
	s := newShard[string, int](0)
	s.set("a", 1, 0, 0, nopRecorder{}, 0)
	s.set("b", 2, 0, 0, nopRecorder{}, 0)
	s.set("a", 3, 0, 0, nopRecorder{}, 0) // overwrite — count must not increase
	if got := s.count.Load(); got != 2 {
		t.Fatalf("expected count 2, got %d", got)
	}
	s.delete("a", nopRecorder{}, 0)
	if got := s.count.Load(); got != 1 {
		t.Fatalf("expected count 1 after delete, got %d", got)
	}
}

// TestShardSweepExpired verifies that active expiry sweep removes all expired entries.
func TestShardSweepExpired(t *testing.T) {
	s := newShard[string, string](0)
	past := monoNow() - 1
	future := monoNow() + int64(1e18) // 1 second from now in nanoseconds

	s.set("expired1", "a", past, 0, nopRecorder{}, 0)
	s.set("expired2", "b", past, 0, nopRecorder{}, 0)
	s.set("live", "c", future, 0, nopRecorder{}, 0)

	s.sweepExpired(monoNow(), nopRecorder{}, 0)

	s.mu.RLock()
	defer s.mu.RUnlock()

	if _, ok := s.data["expired1"]; ok {
		t.Error("expected expired1 to be removed")
	}
	if _, ok := s.data["expired2"]; ok {
		t.Error("expected expired2 to be removed")
	}
	if _, ok := s.data["live"]; !ok {
		t.Error("expected live entry to remain")
	}
}

// TestShardShrink verifies that maybeShrink reconstructs the map,
// removes expired entries, and preserves live ones.
func TestShardShrink(t *testing.T) {
	s := newShard[string, string](0)
	past := monoNow() - 1
	future := monoNow() + int64(1e18)

	s.set("dead1", "x", past, 0, nopRecorder{}, 0)
	s.set("dead2", "y", past, 0, nopRecorder{}, 0)
	s.set("live1", "a", future, 0, nopRecorder{}, 0)
	s.set("live2", "b", future, 0, nopRecorder{}, 0)

	s.maybeShrink(monoNow(), 0, nopRecorder{}, 0)

	s.mu.RLock()
	defer s.mu.RUnlock()

	if _, ok := s.data["dead1"]; ok {
		t.Error("expected dead1 to be removed after shrink")
	}
	if _, ok := s.data["dead2"]; ok {
		t.Error("expected dead2 to be removed after shrink")
	}
	if _, ok := s.data["live1"]; !ok {
		t.Error("expected live1 to survive shrink")
	}
	if _, ok := s.data["live2"]; !ok {
		t.Error("expected live2 to survive shrink")
	}
	if s.shrinking.Load() {
		t.Error("shrinking flag should be false after shrink completes")
	}
	if len(s.dirty) != 0 {
		t.Error("dirty map should be empty after shrink completes")
	}
}

// TestShardShrinkDeltaMergeConcurrent verifies that keys written or deleted
// during shrink phase 2 are correctly handled in phase 3.
// We can't reliably hit the narrow timing window from a unit test, so we
// run many iterations with concurrent writers and check invariants hold.
func TestShardShrinkDeltaMergeConcurrent(t *testing.T) {
	const iterations = 50

	for iter := range iterations {
		s := newShard[int, int](0)
		future := monoNow() + int64(1e18)

		// Pre-populate with stable keys.
		for i := range 100 {
			s.set(i, i, future, 0, nopRecorder{}, 0)
		}

		var wg sync.WaitGroup

		// Writer: continuously sets new keys 1000-1099.
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 1000; i < 1100; i++ {
				s.set(i, i, future, 0, nopRecorder{}, 0)
			}
		}()

		// Deleter: continuously deletes keys 0-49 from the pre-populated set.
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 50 {
				s.delete(i, nopRecorder{}, 0)
			}
		}()

		// Shrink concurrently with writers.
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.maybeShrink(monoNow(), 0, nopRecorder{}, 0)
		}()

		wg.Wait()

		// Invariants:
		// - All deleted keys (0-49) must be absent.
		// - All preserved keys (50-99) must be present.
		// - All new keys (1000-1099) must be present.
		// - count must match actual map size.
		s.mu.RLock()
		for i := range 50 {
			if _, ok := s.data[i]; ok {
				s.mu.RUnlock()
				t.Fatalf("iter %d: deleted key %d survived shrink", iter, i)
			}
		}
		for i := 50; i < 100; i++ {
			if _, ok := s.data[i]; !ok {
				s.mu.RUnlock()
				t.Fatalf("iter %d: preserved key %d lost in shrink", iter, i)
			}
		}
		for i := 1000; i < 1100; i++ {
			if _, ok := s.data[i]; !ok {
				s.mu.RUnlock()
				t.Fatalf("iter %d: new key %d lost during shrink", iter, i)
			}
		}
		actual := len(s.data)
		s.mu.RUnlock()

		if got := s.count.Load(); int(got) != actual {
			t.Fatalf("iter %d: count %d does not match map size %d", iter, got, actual)
		}
	}
}

// TestShardConcurrentSetGet verifies there are no data races under concurrent load.
// Run with: go test -race ./...
func TestShardConcurrentSetGet(t *testing.T) {
	s := newShard[int, int](0)
	var wg sync.WaitGroup
	workers := 50
	ops := 200

	for i := range workers {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := range ops {
				key := (id*ops + j) % 100
				s.set(key, j, 0, 0, nopRecorder{}, 0)
				s.get(key, monoNow(), nopRecorder{}, 0)
			}
		}(i)
	}
	wg.Wait()
}

// TestShardSizeConstant is a compile-time-friendly check of shardPadding.
// Prints the actual size to help tune the constant if it fails.
func TestShardSizeConstant(t *testing.T) {
	var s shard[string, string]
	size := unsafe.Sizeof(s)
	t.Logf("shard[string,string] size = %d bytes", size)
	if size%cacheLineSize != 0 {
		t.Errorf("shard size %d is not aligned to %d bytes — update shardPadding", size, cacheLineSize)
	}
}
