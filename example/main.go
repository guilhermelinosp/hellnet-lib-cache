// Command example demonstrates usage of the hellnet cache library.
//
// Run with env vars set (L2 enabled by default):
//
//	export HELLNET_CACHE_CONNECTION=localhost:6379
//	export HELLNET_CACHE_PASSWORD=your-password   # optional
//	go run ./example
//
// Or disable L2 for a memory-only demo:
//
//	HELLNET_CACHE_ENABLE_L2=false go run ./example
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/guilhermelinosp/hellnet-lib-cache/cache"
)

// Order is a sample domain type to cache.
type Order struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

func main() {
	c, err := cache.New()
	if err != nil {
		log.Fatalf("failed to build cache: %v", err)
	}
	defer func() { _ = c.Close() }()

	// Set with per-key TTL
	if err := c.Set("order:1", Order{ID: "1", Name: "widget"}, 30*time.Minute); err != nil {
		log.Fatalf("set: %v", err)
	}
	fmt.Println("set order:1")

	// Get
	var o Order
	if err := c.Get("order:1", &o); err != nil {
		log.Fatalf("get: %v", err)
	}
	fmt.Printf("got order:1 -> %+v\n", o)

	// GetOrSet — stampede-protected factory
	var cfg string
	err = c.GetOrSet("config:global", &cfg, func(context.Context) (any, error) {
		fmt.Println("factory invoked for config:global")
		return "v1.0.0", nil
	}, 24*time.Hour)
	if err != nil {
		log.Fatalf("getOrSet: %v", err)
	}
	fmt.Printf("config:global -> %s\n", cfg)

	// Exists / Remove
	ok, _ := c.Exists("order:1")
	fmt.Printf("exists order:1 -> %v\n", ok)
	_ = c.Remove("order:1")
	ok, _ = c.Exists("order:1")
	fmt.Printf("exists order:1 after remove -> %v\n", ok)
}
