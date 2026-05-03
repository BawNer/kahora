package kahora_test

import (
	"errors"
	"fmt"
	"time"

	"kahora"
)

// ExampleNew demonstrates basic cache creation, Set, and Get.
func ExampleNew() {
	c, err := kahora.New[string, string]()
	if err != nil {
		panic(err)
	}
	defer c.Close()

	c.Set("greeting", "hello")

	v, ok := c.Get("greeting")
	if ok {
		fmt.Println(v)
	}
	// Output: hello
}

// ExampleNew_withTTL demonstrates configuring a cache with a TTL
// and active background expiry.
func ExampleNew_withTTL() {
	c, err := kahora.New[string, int](
		kahora.WithTTL(time.Minute),
		kahora.WithActiveExpiry(10*time.Second),
	)
	if err != nil {
		panic(err)
	}
	defer c.Close()

	c.Set("counter", 42)

	if v, ok := c.Get("counter"); ok {
		fmt.Println(v)
	}
	// Output: 42
}

// ExampleNew_withCapacity demonstrates capping the cache to a maximum
// number of entries. Set returns ErrCapacityExceeded once the limit is reached.
func ExampleNew_withCapacity() {
	c, err := kahora.New[int, int](
		kahora.WithShardCount(kahora.ShardCountXS),
		kahora.WithMaxEntries(16),
	)
	if err != nil {
		panic(err)
	}
	defer c.Close()

	for i := range 100 {
		if err := c.Set(i, i); errors.Is(err, kahora.ErrCapacityExceeded) {
			fmt.Println("hit capacity limit")
			break
		}
	}
	// Output: hit capacity limit
}

// ExampleNewRecorder demonstrates using the built-in DefaultRecorder
// to observe cache activity without integrating an external metrics backend.
func ExampleNewRecorder() {
	r := kahora.NewRecorder(kahora.ShardCountM)
	c, err := kahora.New[string, string](
		kahora.WithMetricsRecorder(r),
	)
	if err != nil {
		panic(err)
	}
	defer c.Close()

	c.Set("k", "v")
	c.Get("k")       // hit
	c.Get("missing") // miss

	snap := r.Snapshot()
	fmt.Printf("hits=%d misses=%d sets=%d\n", snap.Hits, snap.Misses, snap.Sets)
	// Output: hits=1 misses=1 sets=1
}

// ExampleCache_Delete demonstrates explicit removal of an entry.
func ExampleCache_Delete() {
	c, err := kahora.New[string, string]()
	if err != nil {
		panic(err)
	}
	defer c.Close()

	c.Set("temp", "value")
	c.Delete("temp")

	if _, ok := c.Get("temp"); !ok {
		fmt.Println("gone")
	}
	// Output: gone
}

// ExampleCache_Close demonstrates the cache lifecycle.
// After Close, Set returns ErrClosed but Get remains usable
// against the static snapshot.
func ExampleCache_Close() {
	c, err := kahora.New[string, string]()
	if err != nil {
		panic(err)
	}

	c.Set("k", "v")
	c.Close()

	err = c.Set("k2", "v2")
	if errors.Is(err, kahora.ErrClosed) {
		fmt.Println("cache is closed")
	}
	// Output: cache is closed
}
