# hellnet-lib-cache

> Multi-layer cache library for Go — L1 (in-process memory), L2 (Valkey/Redis)
> and L3 (optional external, pluggable).
>
> Faithful Go port of [`hellnet-dep-cache`](https://github.com/guilhermelinosp/hellnet-dep-cache) (.NET).

Write-through, read-through, stampede protection, env-first configuration and
graceful degradation on backend failures.

## Install

```bash
go get github.com/guilhermelinosp/hellnet-lib-cache
```

Requires Go 1.24+.

## Quick start

### Env-first (recommended for microservices)

```go
package main

import (
	"context"
	"log"

	cache "github.com/guilhermelinosp/hellnet-lib-cache"
)

func main() {
	// Reads HELLNET_CACHE_VALKEY_CONNECTION + HELLNET_CACHE_VALKEY_PASSWORD.
	// Panics at startup if they are missing.
	c := cache.MustFromEnv()
	c = cache.MustNew(cache.MustFromEnv()) // build the cache from env options
	_ = c

	// Or in one step:
	c, err := cache.New(cache.MustFromEnv())
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	ctx := context.Background()

	// Set with per-key TTL
	if err := c.Set(ctx, "order:1", Order{ID: "1", Name: "x"}, durationPtr(1*time.Hour)); err != nil {
		log.Fatal(err)
	}

	// Get
	var o Order
	_ = c.Get(ctx, "order:1", &o)

	// GetOrSet — stampede-protected
	var cfg Config
	_ = c.GetOrSet(ctx, "config:global", &cfg, func(context.Context) (any, error) {
		return loadConfig(), nil
	}, nil)

	// Remove / Exists
	_ = c.Remove(ctx, "order:1")
	exists, _ := c.Exists(ctx, "order:1")
	_ = exists
}
```

### Explicit

```go
opts := cache.Options{
	EnableL1:      true,
	EnableL2:      true,
	ValkeyConnection: "valkey.hellnet.com.br:6379",
	ValkeyPassword:   "hellnet2026",
}
c, err := cache.New(opts)
```

### Minimal env

```bash
export HELLNET_CACHE_VALKEY_CONNECTION=valkey.hellnet.com.br:6379
export HELLNET_CACHE_VALKEY_PASSWORD=hellnet2026
```

## Usage

```go
type OrderService struct{ cache cache.Cache }

func (s *OrderService) SetOrder(ctx context.Context, o Order) error {
	return s.cache.Set(ctx, "order:"+o.ID, o, durationPtr(time.Hour))
}

func (s *OrderService) GetOrder(ctx context.Context, id string) (Order, error) {
	var o Order
	if err := s.cache.Get(ctx, "order:"+id, &o); err != nil {
		return o, err
	}
	return o, nil
}

func (s *OrderService) Invalidate(ctx context.Context, id string) error {
	return s.cache.Remove(ctx, "order:"+id)
}
```

## Layers

| Layer | Provider            | Default TTL | Failure behavior                |
|-------|---------------------|-------------|---------------------------------|
| L1    | `MemoryProvider`    | 5 min       | Always healthy                  |
| L2    | `ValkeyProvider`    | 30 min      | Graceful — returns nil, logs    |
| L3    | `ExternalProvider`  | 15 min      | Disabled by default             |

### Read-through

```
Get("key") → L1 → L2 → L3 → miss/nil
```

On a hit in L2/L3, lower layers are populated automatically (async, deduplicated
warming).

### Write-through

```
Set("key") → L1.Set + L2.Set + L3.Set in parallel (WaitGroup)
```

## Per-key TTL

Each `Set`/`GetOrSet` accepts `*time.Duration` TTL. When nil, the per-layer
fallback is used:

| Call              | L1        | L2        | L3        |
|-------------------|-----------|-----------|-----------|
| `Set(k, v)`       | 5min      | 30min     | 15min     |
| `Set(k, v, 1h)`   | 1h        | 1h        | 1h        |

L1 uses **absolute expiration** by default. Sliding is opt-in via
`L1SlidingExpiration`.

## Concurrency

| Mechanism           | Prevents                                            |
|---------------------|-----------------------------------------------------|
| Per-key semaphore   | Cache stampede in `GetOrSet` — 1 factory per key    |
| Auto-cleanup        | Semaphore disposed after factory (no leak)          |
| Warming dedup       | Only one warming task per key at a time             |
| Touch-on-read       | `TouchOnRead=true` extends TTL on all layers on hit |
| Parallel writes     | `WaitGroup` — Set/Remove across all layers          |

## Resilience (L2)

| Mechanism            | Behavior                                                  |
|----------------------|-----------------------------------------------------------|
| Retry                | Exponential backoff with jitter (go-redis MaxRetries)    |
| Circuit breaker      | N consecutive failures → open → half-open (gobreaker)     |
| Degradation          | Every failure returns nil/false, never errors out         |

## Options

### Env vars (`HELLNET_CACHE_*`)

| Env var                              | Default              | Description                  |
|--------------------------------------|----------------------|------------------------------|
| `VALKEY_CONNECTION`                   | *(required)*         | Valkey host:port             |
| `VALKEY_PASSWORD`                     | *(required)*         | Valkey password              |
| `VALKEY_KEY_PREFIX`                   | `hellnet:cache:`     | Key prefix in Valkey         |
| `L1_DEFAULT_TTL`                     | `00:05:00`           | L1 fallback TTL              |
| `DEFAULT_TTL`                        | `00:30:00`           | Global fallback TTL          |
| `MAX_TTL`                            | `24:00:00`           | Safety cap                   |
| `TOUCH_ON_READ`                      | `false`              | Auto-extend TTL on hit       |
| `TOUCH_TTL`                          | `00:10:00`           | Extension amount             |
| `L1_SLIDING_EXPIRATION`              | `false`              | Sliding vs Absolute          |
| `VALKEY_RETRY_COUNT`                 | `2`                  | Max retry attempts           |
| `VALKEY_RETRY_BASE_DELAY_MS`         | `200`                | Base retry delay             |
| `VALKEY_CB_FAILURES`                 | `5`                  | Circuit breaker threshold    |
| `VALKEY_CB_DURATION_SEC`             | `30`                 | Circuit breaker duration     |
| `ENABLE_L1`                          | `true`               | Enable L1                    |
| `ENABLE_L2`                          | `true`               | Enable L2                    |
| `ENABLE_EXTERNAL`                    | `false`              | Enable L3                    |

Env vars accept Go duration syntax (`5m`, `30s`) or .NET-style (`00:05:00`).

## Dependencies

- `github.com/dgraph-io/ristretto/v2` — L1 memory provider
- `github.com/redis/go-redis/v9` — L2 Valkey provider
- `github.com/sony/gobreaker` — Circuit breaker (L2 resilience)

## License

Apache 2.0 © 2026 Hellnet
