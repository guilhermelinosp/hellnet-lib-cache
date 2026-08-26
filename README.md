# hellnet-lib-cache

> Multi-layer cache library for Go — L1 (in-process memory), L2 (external,
> pluggable distributed backend).

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
	"os"
	"os/signal"
	"time"

	cache "github.com/guilhermelinosp/hellnet-lib-cache/cache"
)

func main() {
	// The application context is passed ONCE at construction — the library
	// captures and propagates it internally. Operations never take a context.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	// New() loads .env (HELLNET_CACHE_*), validates and decides L1/L2.
	c, err := cache.New(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	// Set with per-key TTL
	if err := c.Set("order:1", Order{ID: "1", Name: "x"}, time.Hour); err != nil {
		log.Fatal(err)
	}

	// Get
	var o Order
	_ = c.Get("order:1", &o)

	// GetOrSet — stampede-protected
	var cfg Config
	_ = c.GetOrSet("config:global", &cfg, func(context.Context) (any, error) {
		return loadConfig(), nil
	}, 0)

	// Remove / Exists
	_ = c.Remove("order:1")
	exists, _ := c.Exists("order:1")
	_ = exists
}
```

### Explicit

```go
opts := cache.Options{
	EnableL1:   true,
	EnableL2:   true,
	Connection: "localhost:6379",
	Password:   "your-password", // optional
}
c, err := cache.New(ctx, opts)
```

### Minimal env

```bash
export HELLNET_CACHE_CONNECTION=localhost:6379
# optional: export HELLNET_CACHE_PASSWORD=...
```

## Usage

```go
type OrderService struct{ cache cache.Cache }

func (s *OrderService) SetOrder(o Order) error {
	return s.cache.Set("order:"+o.ID, o, time.Hour)
}

func (s *OrderService) GetOrder(id string) (Order, error) {
	var o Order
	if err := s.cache.Get("order:"+id, &o); err != nil {
		return o, err
	}
	return o, nil
}

func (s *OrderService) Invalidate(id string) error {
	return s.cache.Remove("order:" + id)
}
```

### Context & timeouts

The context given to `New`/`MustNew` is captured once and propagated internally
to every operation and background goroutine (warming/touch). There are no
`*Context` method variants. Each operation runs under an internally derived
timeout bounded by `OperationTimeout` (default `5s`, env-tunable via
`HELLNET_CACHE_OPERATION_TIMEOUT_MS`); L2 network calls additionally honor
`ConnectTimeout`/`ReadTimeout`. Shutdown is coherent: cancelling the caller's
context or calling `Close()` aborts all in-flight library work.

```go
ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
defer stop()

c, err := cache.New(ctx)
if err != nil {
    log.Fatal(err)
}
defer c.Close() // cancels the internally captured context as well
```

Additional runnable examples are available in the package documentation
(`go doc github.com/guilhermelinosp/hellnet-lib-cache/cache`).

## Layers

| Layer | Provider           | Default TTL | Failure behavior                |
|-------|--------------------|-------------|---------------------------------|
| L1    | `MemoryProvider`   | 5 min       | Always healthy                  |
| L2    | `ExternalProvider` | 30 min      | Graceful — returns nil, logs    |

### Read-through

```
Get("key") → L1 → L2 → miss/nil
```

On a hit in L2, lower layers (L1) are populated automatically (async, deduplicated
warming).

### Write-through

```
Set("key") → L1.Set + L2.Set in parallel (WaitGroup)
```

## Per-key TTL

Each `Set`/`GetOrSet` accepts a `time.Duration` TTL. When `0`, the per-layer
fallback is used:

| Call              | L1        | L2        |
|-------------------|-----------|-----------|
| `Set(k, v)`       | 5min      | 30min     |
| `Set(k, v, 1h)`   | 1h        | 1h        |

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

| Env var                            | Default              | Description                    |
|------------------------------------|----------------------|--------------------------------|
| `CONNECTION`                       | *(required)*         | External backend host:port     |
| `PASSWORD`                         | *(optional)*         | External backend password (empty = no auth) |
| `KEY_PREFIX`                       | `hellnet:cache:`     | Key prefix in backend          |
| `L1_DEFAULT_TTL`                   | `00:05:00`           | L1 fallback TTL                |
| `DEFAULT_TTL`                      | `00:30:00`           | Global fallback TTL            |
| `MAX_TTL`                          | `24:00:00`           | Safety cap                     |
| `TOUCH_ON_READ`                    | `false`              | Auto-extend TTL on hit         |
| `TOUCH_TTL`                        | `00:10:00`           | Extension amount               |
| `L1_SLIDING_EXPIRATION`            | `false`              | Sliding vs Absolute            |
| `RETRY_COUNT`                      | `2`                  | Max retry attempts             |
| `RETRY_BASE_DELAY_MS`              | `200`                | Base retry delay               |
| `CB_FAILURES`                      | `5`                  | Circuit breaker threshold      |
| `CB_DURATION_SEC`                  | `30`                 | Circuit breaker duration       |
| `OPERATION_TIMEOUT_MS`             | `5000`               | Per-operation timeout (integer ms) |
| `ENABLE_L1`                        | `true`               | Enable L1                      |
| `ENABLE_L2`                        | `true`               | Enable L2                      |

Env vars accept Go duration syntax (`5m`, `30s`) or clock-style (`00:05:00`).

## Dependencies

- `github.com/dgraph-io/ristretto/v2` — L1 memory provider
- `github.com/guilhermelinosp/hellnet-lib-environments` — env binding
- `github.com/redis/go-redis/v9` — L2 external backend
- `github.com/sony/gobreaker` — Circuit breaker (L2 resilience)

## License

Apache 2.0 © 2026 Hellnet

<!-- Release tags are GPG-signed by ci-templates (key fingerprint B58DF1F750BBFE4EC60CC5918367B6CA2DE60761). -->

<!-- verified GPG signing -->

<!-- release signing confirmed -->

<!-- signed release OK -->

<!-- release-sign-test 2026-08-26T12:30 -->
