package cache_test

import (
	"context"
	"fmt"
	"os"
	"time"

	cache "github.com/guilhermelinosp/hellnet-lib-cache/cache"
)

// memCache builds a memory-only cache through the public zero-config API.
func memCache() *cache.HybridCache {
	_ = os.Setenv("HELLNET_CACHE_ENABLE_L1", "true")
	_ = os.Setenv("HELLNET_CACHE_ENABLE_L2", "false")
	c, _ := cache.New()
	return c
}

// ExampleHybridCache_Set demonstrates storing and retrieving a value with a
// per-key TTL.
func ExampleHybridCache_Set() {
	c := memCache()
	defer c.Close()

	if err := c.Set("user:1", map[string]string{"name": "Alice"}, time.Hour); err != nil {
		panic(err)
	}

	var u map[string]string
	if err := c.Get("user:1", &u); err != nil {
		panic(err)
	}
	fmt.Println(u["name"])
	// Output: Alice
}

// ExampleHybridCache_GetOrSet demonstrates the stampede-protected factory that
// computes and caches a value only when it is missing. The factory receives a
// library-derived context (operation-scoped child of its base context) —
// callers who don't need cancellation simply ignore it, as here.
func ExampleHybridCache_GetOrSet() {
	c := memCache()
	defer c.Close()

	computations := 0
	for i := 0; i < 3; i++ {
		var v int
		_ = c.GetOrSet("counter", &v, func(context.Context) (any, error) {
			computations++
			return 42, nil
		}, 0)
	}
	fmt.Printf("computed=%d value=%d", computations, func() int {
		var v int
		_ = c.Get("counter", &v)
		return v
	}())
	// Output: computed=1 value=42
}

// ExampleHybridCache_SetBytes demonstrates the low-level primitive that writes
// pre-serialized bytes to every layer in parallel (e.g. when the payload was
// produced by an external serializer). Cancellation/deadlines are handled
// internally: each operation runs under the library's operation timeout,
// derived from the context owned by the cache.
func ExampleHybridCache_SetBytes() {
	c := memCache()
	defer c.Close()

	data, err := cache.NewJSONSerializer().Serialize(map[string]int{"hit": 1})
	if err != nil {
		panic(err)
	}
	if err := c.SetBytes("stats", data, time.Minute); err != nil {
		panic(err)
	}

	var out map[string]int
	if err := c.Get("stats", &out); err != nil {
		panic(err)
	}
	fmt.Println(out["hit"])
	// Output: 1
}
