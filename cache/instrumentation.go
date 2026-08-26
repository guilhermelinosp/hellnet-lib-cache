package cache

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/dgraph-io/ristretto/v2"
	"github.com/redis/go-redis/v9"
	"github.com/sony/gobreaker"
)

// MemoryProvider is the L1 in-process memory cache provider, backed by ristretto.
//
// TTL behavior:
//   - default: absolute expiration (key expires exactly TTL after set)
//   - when L1SlidingExpiration=true: sliding expiration (extends on each read)
//
// Operations are synchronous in-process calls: they never block on I/O, so no
// context plumbing is needed (part of the Provider contract — implementations
// derive internal contexts themselves).
type MemoryProvider struct {
	cache   *ristretto.Cache[string, []byte]
	opts    Options
	metrics *Metrics
}

// NewMemoryProvider builds an L1 memory provider from options.
func NewMemoryProvider(opts Options) (*MemoryProvider, error) {
	rc, err := ristretto.NewCache(&ristretto.Config[string, []byte]{
		NumCounters: 1e7,
		MaxCost:     int64(opts.L1SizeLimitMB) * 1024 * 1024,
		BufferItems: 64,
	})
	if err != nil {
		return nil, err
	}
	return &MemoryProvider{
		cache:   rc,
		opts:    opts,
		metrics: newMetrics("L1-Memory"),
	}, nil
}

// Name returns the layer name.
func (p *MemoryProvider) Name() string { return "L1-Memory" }

// Get retrieves raw bytes by key. Returns nil on miss.
func (p *MemoryProvider) Get(key string) ([]byte, error) {
	v, found := p.cache.Get(key)
	if !found || v == nil {
		p.metrics.RecordMiss()
		return nil, nil
	}
	p.metrics.RecordHit()

	if p.opts.L1SlidingExpiration {
		ttl := p.opts.L1DefaultTTL
		p.cache.SetWithTTL(key, v, int64(len(v)), ttl)
	}
	return v, nil
}

// Set stores raw bytes with optional TTL.
func (p *MemoryProvider) Set(key string, value []byte, ttl time.Duration) error {
	actual := p.opts.capTTL(defaultTTL(ttl, p.opts.L1DefaultTTL))
	p.cache.SetWithTTL(key, value, int64(len(value)), actual)
	// ristretto processes sets asynchronously; Wait ensures the item is visible
	// before returning, matching synchronous behavior.
	p.cache.Wait()
	p.metrics.RecordSet()
	return nil
}

// Remove deletes a key.
func (p *MemoryProvider) Remove(key string) error {
	p.cache.Del(key)
	p.metrics.RecordRemove()
	return nil
}

// Exists reports whether a key exists.
func (p *MemoryProvider) Exists(key string) (bool, error) {
	_, found := p.cache.Get(key)
	return found, nil
}

// HealthCheck reports whether the backend is reachable. The in-process L1 is
// always healthy while alive.
func (p *MemoryProvider) HealthCheck() bool { return true }

// Close releases resources held by the provider.
func (p *MemoryProvider) Close() error {
	p.cache.Close()
	return nil
}

// Metrics returns the provider metrics.
func (p *MemoryProvider) Metrics() *Metrics { return p.metrics }

// ExternalProvider is the L2 distributed cache provider. It speaks the Redis
// wire protocol (compatible with Redis and Valkey) but the public API stays
// backend-agnostic — no backend-specific name is exposed.
//
// Context model: a base context is captured once at construction (New's
// context) and every operation derives an internal per-op timeout context
// from it (Options.OperationTimeout). go-redis requires a context; callers
// never supply one.
type ExternalProvider struct {
	baseCtx context.Context
	opts    Options
	metrics *Metrics
	client  *redis.Client
	breaker *gobreaker.CircuitBreaker
}

// NewExternalProvider builds the L2 external provider backed by a
// Redis-compatible server using the options. The given context is captured
// once and propagated internally to all operations.
func NewExternalProvider(ctx context.Context, opts Options) *ExternalProvider {
	if ctx == nil {
		ctx = context.Background()
	}
	client := redis.NewClient(&redis.Options{
		Addr:         opts.Connection,
		Password:     opts.Password,
		DB:           opts.Database,
		DialTimeout:  opts.ConnectTimeout,
		ReadTimeout:  opts.ReadTimeout,
		WriteTimeout: opts.ReadTimeout,
		MaxRetries:   opts.RetryCount,
	})

	// clamp to uint32 range (safe conversion, avoids gosec G115 on repeated casts).
	//nolint:gosec // clampInt bounds the value to [0, MaxUint32], so the cast is safe.
	maxRequests := uint32(clampInt(opts.CircuitBreakerFailures, 0, int(^uint32(0))))

	cb := gobreaker.Settings{
		Name:        "L2-External",
		Interval:    opts.CircuitBreakerDuration,
		Timeout:     opts.CircuitBreakerDuration,
		MaxRequests: maxRequests,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.ConsecutiveFailures >= maxRequests
		},
		IsSuccessful: func(err error) bool {
			return err == nil || errors.Is(err, redis.Nil)
		},
		OnStateChange: func(name string, from, to gobreaker.State) {
			log.Printf("[hellnet-cache] external circuit breaker %s: %s -> %s", name, from, to)
		},
	}

	return &ExternalProvider{
		baseCtx: ctx,
		opts:    opts,
		metrics: newMetrics("L2-External"),
		client:  client,
		breaker: gobreaker.NewCircuitBreaker(cb),
	}
}

// opCtx derives the per-operation context from the provider's captured base
// context, bounded by Options.OperationTimeout. Callers must invoke the
// returned CancelFunc.
func (p *ExternalProvider) opCtx() (context.Context, context.CancelFunc) {
	t := p.opts.OperationTimeout
	if t <= 0 {
		t = defaultOperationTimeout
	}
	return context.WithTimeout(p.baseCtx, t)
}

// Name returns the layer name.
func (p *ExternalProvider) Name() string { return "L2-External" }

// Get retrieves raw bytes by key. Returns nil on miss or backend failure
// (graceful degradation).
func (p *ExternalProvider) Get(key string) ([]byte, error) {
	ctx, cancel := p.opCtx()
	defer cancel()

	res, err := p.breaker.Execute(func() (any, error) {
		v, err := p.client.Get(ctx, p.opts.formatKey(key)).Bytes()
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		return v, nil
	})
	if err != nil {
		log.Printf("[hellnet-cache] external get failed for %s: %v", key, err)
		return nil, nil // graceful degradation
	}
	if v, ok := res.([]byte); ok && v != nil {
		p.metrics.RecordHit()
		return v, nil
	}
	p.metrics.RecordMiss()
	return nil, nil
}

// Set stores raw bytes with optional TTL.
func (p *ExternalProvider) Set(key string, value []byte, ttl time.Duration) error {
	ctx, cancel := p.opCtx()
	defer cancel()

	actual := p.opts.capTTL(defaultTTL(ttl, p.opts.DefaultTTL))
	_, err := p.breaker.Execute(func() (any, error) {
		return nil, p.client.Set(ctx, p.opts.formatKey(key), value, actual).Err()
	})
	if err != nil {
		log.Printf("[hellnet-cache] external set failed for %s: %v", key, err)
	}
	p.metrics.RecordSet()
	return nil
}

// Remove deletes a key.
func (p *ExternalProvider) Remove(key string) error {
	ctx, cancel := p.opCtx()
	defer cancel()

	_, err := p.breaker.Execute(func() (any, error) {
		return p.client.Del(ctx, p.opts.formatKey(key)).Result()
	})
	if err != nil {
		log.Printf("[hellnet-cache] external delete failed for %s: %v", key, err)
	}
	p.metrics.RecordRemove()
	return nil
}

// Exists reports whether a key exists.
func (p *ExternalProvider) Exists(key string) (bool, error) {
	ctx, cancel := p.opCtx()
	defer cancel()

	res, err := p.breaker.Execute(func() (any, error) {
		n, err := p.client.Exists(ctx, p.opts.formatKey(key)).Result()
		return n > 0, err
	})
	if err != nil {
		return false, nil
	}
	if b, ok := res.(bool); ok {
		return b, nil
	}
	return false, nil
}

// HealthCheck reports whether the backend is reachable, using the internal
// operation context derived from the captured base context.
func (p *ExternalProvider) HealthCheck() bool {
	ctx, cancel := p.opCtx()
	defer cancel()
	return p.client.Ping(ctx).Err() == nil
}

// Close releases resources held by the provider.
func (p *ExternalProvider) Close() error {
	return p.client.Close()
}

// Metrics returns the provider metrics.
func (p *ExternalProvider) Metrics() *Metrics { return p.metrics }

// clampInt clamps v to the inclusive [lo, hi] range. Compatible with any Go
// version (avoids the built-in min/max which require Go 1.21+).
func clampInt(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}
