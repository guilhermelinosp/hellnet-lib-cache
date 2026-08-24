package cache_test

import (
	"context"
	"fmt"
	"time"

	cache "github.com/guilhermelinosp/hellnet-lib-cache/cache"
)

// memCache builds a hermetic memory-only cache for examples.
func memCache() *cache.HybridCache {
	opts := cache.DefaultOptions()
	opts.EnableL1 = true
	opts.EnableL2 = false
	c, _ := cache.New(opts)
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
// computes and caches a value only when it is missing.
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

// ExampleHybridCache_GetContext demonstrates the *Context variants with a
// timeout for cancellation control.
func ExampleHybridCache_GetContext() {
	c := memCache()
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := c.SetContext(ctx, "k", "v", time.Minute); err != nil {
		panic(err)
	}
	var out string
	if err := c.GetContext(ctx, "k", &out); err != nil {
		panic(err)
	}
	fmt.Println(out)
	// Output: v
}