package kahora

import (
	"sync"
	"testing"
	"time"
	"unsafe"
)

func TestShardCacheLineAlignment(t *testing.T) {
	size := shardSize[string, string]()
	if size%cacheLineSize != 0 {
		t.Fatalf("shard size %d is not a multiple of cache line size %d — adjust shardPadding in shard.go", size, cacheLineSize)
	}
}

func TestShardGetMiss(t *testing.T) {
	s := newShard[string, string](0, 0, 0)
	v, ok := s.get("key", monoNow(), nopRecorder{}, 0)
	if ok {
		t.Fatal("expected miss, got hit")
	}
	if v != "" {
		t.Fatalf("expected zero value, got %q", v)
	}
}

func TestShardSetGet(t *testing.T) {
	s := newShard[string, string](0, 0, 0)
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

func TestShardOverwrite(t *testing.T) {
	s := newShard[string, string](0, 0, 0)
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

func TestShardDelete(t *testing.T) {
	s := newShard[string, string](0, 0, 0)
	s.set("key", "value", 0, 0, nopRecorder{}, 0)
	s.delete("key", nopRecorder{}, 0)
	_, ok := s.get("key", monoNow(), nopRecorder{}, 0)
	if ok {
		t.Fatal("expected miss after delete, got hit")
	}
}

func TestShardDeleteNonExistent(t *testing.T) {
	s := newShard[string, string](0, 0, 0)
	s.delete("ghost", nopRecorder{}, 0)
}

func TestShardLazyExpiry(t *testing.T) {
	s := newShard[string, string](0, 0, 0)
	past := monoNow() - 1
	s.set("key", "value", past, 0, nopRecorder{}, 0)

	_, ok := s.get("key", monoNow(), nopRecorder{}, 0)
	if ok {
		t.Fatal("expected miss for expired entry, got hit")
	}

	s.mu.Lock()
	_, exists := s.data["key"]
	s.mu.Unlock()
	if exists {
		t.Fatal("expected expired entry to be removed from map")
	}
}

func TestShardCapacityLimitReject(t *testing.T) {
	s := newShard[string, string](0, 0, 0) // sampleSize=0 → reject mode
	limit := 2
	s.set("a", "1", 0, limit, nopRecorder{}, 0)
	s.set("b", "2", 0, limit, nopRecorder{}, 0)
	err := s.set("c", "3", 0, limit, nopRecorder{}, 0)
	if err != ErrCapacityExceeded {
		t.Fatalf("expected ErrCapacityExceeded, got %v", err)
	}
}

func TestShardCapacityLimitLFU(t *testing.T) {
	s := newShard[string, string](0, 5, 256) // sampleSize=5 → LFU mode
	limit := 2
	if err := s.set("a", "1", 0, limit, nopRecorder{}, 0); err != nil {
		t.Fatal(err)
	}
	if err := s.set("b", "2", 0, limit, nopRecorder{}, 0); err != nil {
		t.Fatal(err)
	}
	// Third set must succeed under LFU — it evicts a victim.
	if err := s.set("c", "3", 0, limit, nopRecorder{}, 0); err != nil {
		t.Fatalf("LFU should evict, not return error: %v", err)
	}

	s.mu.Lock()
	got := len(s.data)
	s.mu.Unlock()
	if got != 2 {
		t.Fatalf("expected 2 live entries after LFU eviction, got %d", got)
	}
}

func TestShardCountAccuracy(t *testing.T) {
	s := newShard[string, int](0, 0, 0)
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

func TestShardSweepExpired(t *testing.T) {
	s := newShard[string, string](0, 0, 0)
	past := monoNow() - 1
	future := monoNow() + int64(1e18)

	s.set("expired1", "a", past, 0, nopRecorder{}, 0)
	s.set("expired2", "b", past, 0, nopRecorder{}, 0)
	s.set("live", "c", future, 0, nopRecorder{}, 0)

	s.sweepExpired(monoNow(), nopRecorder{}, 0)

	s.mu.Lock()
	defer s.mu.Unlock()

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

func TestShardShrink(t *testing.T) {
	s := newShard[string, string](0, 0, 0)
	past := monoNow() - 1
	future := monoNow() + int64(1e18)

	s.set("dead1", "x", past, 0, nopRecorder{}, 0)
	s.set("dead2", "y", past, 0, nopRecorder{}, 0)
	s.set("live1", "a", future, 0, nopRecorder{}, 0)
	s.set("live2", "b", future, 0, nopRecorder{}, 0)

	s.maybeShrink(monoNow(), 0, nopRecorder{}, 0)

	s.mu.Lock()
	defer s.mu.Unlock()

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

// TestShardShrinkDeltaMergeConcurrent runs shrink concurrently with writers
// and deleters, then checks invariants. Run with -race.
func TestShardShrinkDeltaMergeConcurrent(t *testing.T) {
	const iterations = 50

	for iter := range iterations {
		s := newShard[int, int](0, 0, 0)
		future := monoNow() + int64(1e18)

		for i := range 100 {
			s.set(i, i, future, 0, nopRecorder{}, 0)
		}

		var wg sync.WaitGroup

		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 1000; i < 1100; i++ {
				s.set(i, i, future, 0, nopRecorder{}, 0)
			}
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 50 {
				s.delete(i, nopRecorder{}, 0)
			}
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			s.maybeShrink(monoNow(), 0, nopRecorder{}, 0)
		}()

		wg.Wait()

		s.mu.Lock()
		for i := range 50 {
			if _, ok := s.data[i]; ok {
				s.mu.Unlock()
				t.Fatalf("iter %d: deleted key %d survived shrink", iter, i)
			}
		}
		for i := 50; i < 100; i++ {
			if _, ok := s.data[i]; !ok {
				s.mu.Unlock()
				t.Fatalf("iter %d: preserved key %d lost in shrink", iter, i)
			}
		}
		for i := 1000; i < 1100; i++ {
			if _, ok := s.data[i]; !ok {
				s.mu.Unlock()
				t.Fatalf("iter %d: new key %d lost during shrink", iter, i)
			}
		}
		actual := len(s.data)
		s.mu.Unlock()

		if got := s.count.Load(); int(got) != actual {
			t.Fatalf("iter %d: count %d does not match map size %d", iter, got, actual)
		}
	}
}

func TestShardConcurrentSetGet(t *testing.T) {
	s := newShard[int, int](0, 0, 0)
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

func TestShardSizeConstant(t *testing.T) {
	var s shard[string, string]
	size := unsafe.Sizeof(s)
	t.Logf("shard[string,string] size = %d bytes", size)
	if size%cacheLineSize != 0 {
		t.Errorf("shard size %d is not aligned to %d bytes — update shardPadding", size, cacheLineSize)
	}
}

// TestShardLFUFreqIncrement verifies that buffered accesses land in freq
// once drained.
func TestShardLFUFreqIncrement(t *testing.T) {
	s := newShard[string, int](0, 5, 256)
	s.set("k", 1, 0, 0, nopRecorder{}, 0)

	for range 10 {
		s.get("k", monoNow(), nopRecorder{}, 0)
	}
	s.drainAccess(nopRecorder{}, 0)

	s.mu.Lock()
	got := s.freq["k"]
	s.mu.Unlock()

	if got != 10 {
		t.Fatalf("expected freq=10 after 10 Gets, got %d", got)
	}
}

// TestShardLFUAging verifies that ageFreq halves all counters.
func TestShardLFUAging(t *testing.T) {
	s := newShard[string, int](0, 5, 256)
	s.set("k1", 1, 0, 0, nopRecorder{}, 0)
	s.set("k2", 2, 0, 0, nopRecorder{}, 0)

	for range 100 {
		s.get("k1", monoNow(), nopRecorder{}, 0)
	}
	for range 50 {
		s.get("k2", monoNow(), nopRecorder{}, 0)
	}
	s.drainAccess(nopRecorder{}, 0)

	s.ageFreq()

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.freq["k1"] != 50 {
		t.Errorf("expected k1 freq=50 after aging, got %d", s.freq["k1"])
	}
	if s.freq["k2"] != 25 {
		t.Errorf("expected k2 freq=25 after aging, got %d", s.freq["k2"])
	}
}

// TestAccessBufferOverflow verifies that record-past-capacity is counted as
// dropped and does not corrupt state.
func TestAccessBufferOverflow(t *testing.T) {
	s := newShard[string, int](0, 5, 4) // tiny ring
	s.set("k", 1, 0, 0, nopRecorder{}, 0)

	for range 10 {
		s.get("k", monoNow(), nopRecorder{}, 0)
	}

	rec := &countingRecorder{}
	attempted := s.drainAccess(rec, 0)

	if attempted != 10 {
		t.Fatalf("expected attempted=10, got %d", attempted)
	}
	if rec.dropped != 6 {
		t.Fatalf("expected 6 dropped accesses (10 - buffer 4), got %d", rec.dropped)
	}

	s.mu.Lock()
	got := s.freq["k"]
	s.mu.Unlock()
	if got != 4 {
		t.Fatalf("expected freq=4 (buffer capacity), got %d", got)
	}
}

// TestDrainSkipsDeletedKey ensures the alive guard prevents resurrecting a
// key's freq entry after Delete.
func TestDrainSkipsDeletedKey(t *testing.T) {
	s := newShard[string, int](0, 5, 256)
	s.set("k", 1, 0, 0, nopRecorder{}, 0)
	s.get("k", monoNow(), nopRecorder{}, 0)
	s.delete("k", nopRecorder{}, 0)

	s.drainAccess(nopRecorder{}, 0)

	s.mu.Lock()
	_, ok := s.freq["k"]
	s.mu.Unlock()
	if ok {
		t.Fatalf("freq must not contain deleted key")
	}
}

// TestAdaptDrainInterval covers the adaptive scheduler's three branches.
func TestAdaptDrainInterval(t *testing.T) {
	minInterval := 50 * time.Millisecond
	maxInterval := 1 * time.Second

	// Full ring → halve, but not below minInterval.
	if got := adaptDrainInterval(200*time.Millisecond, 250, 256, minInterval, maxInterval); got != 100*time.Millisecond {
		t.Errorf("fill>0.9 should halve: got %v", got)
	}
	if got := adaptDrainInterval(60*time.Millisecond, 300, 256, minInterval, maxInterval); got != minInterval {
		t.Errorf("fill>0.9 must clamp to min: got %v", got)
	}

	// Idle → grow 1.5×, but not above maxInterval.
	if got := adaptDrainInterval(200*time.Millisecond, 10, 256, minInterval, maxInterval); got != 300*time.Millisecond {
		t.Errorf("fill<0.25 should grow: got %v", got)
	}
	if got := adaptDrainInterval(900*time.Millisecond, 0, 256, minInterval, maxInterval); got != maxInterval {
		t.Errorf("fill<0.25 must clamp to max: got %v", got)
	}

	// Middle band → unchanged.
	if got := adaptDrainInterval(200*time.Millisecond, 128, 256, minInterval, maxInterval); got != 200*time.Millisecond {
		t.Errorf("mid fill should keep current: got %v", got)
	}
}

type countingRecorder struct {
	nopRecorder
	dropped int
}

func (r *countingRecorder) RecordAccessesDropped(_, dropped int) {
	r.dropped += dropped
}
