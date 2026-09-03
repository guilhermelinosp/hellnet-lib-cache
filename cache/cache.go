// Package cache provides a multi-layer cache for Go with L1 (in-process memory)
// and L2 (external, pluggable distributed backend).
//
// It features write-through, read-through, stampede protection, env-first
// configuration and graceful degradation on backend failures. The distributed
// backend is backend-agnostic in the public API. On top of caching it offers
// coordination primitives sharing the same stack: Idempotent (at-most-once
// execution within a TTL), Allow (fixed-window distributed rate limiting) and
// Lock (TTL-based distributed mutual exclusion).
//
// Context model: the library creates and owns a base context at construction.
// Individual operations never take a context; each one runs under an internally
// derived timeout configured through HELLNET_CACHE_OPERATION_TIMEOUT_MS.
package cache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/guilhermelinosp/hellnet-lib-environments/environments"
	"golang.org/x/sync/singleflight"
)

// defaultOperationTimeout bounds every cache operation when
// Options.OperationTimeout is not explicitly configured.
const defaultOperationTimeout = 5 * time.Second

// Provider is an individual cache layer (L1 memory, L2 external).
//
// Implementations receive no context: each derives its own bounded,
// short-lived context internally from the provider's captured base context.
type Provider interface {
	Name() string
	Get(key string) ([]byte, error)
	Set(key string, value []byte, ttl time.Duration) error
	Remove(key string) error
	Exists(key string) (bool, error)
	HealthCheck() bool
	Close() error
}

// Serializer marshals/unmarshals cache values.
type Serializer interface {
	Serialize(any) ([]byte, error)
	Deserialize([]byte, any) error
}

// Cache is the multi-layer cache abstraction. Write-through, read-through.
// Every method runs under the internal operation context derived from the
// context captured at construction; callers never pass contexts. A ttl of 0
// uses the layer's default TTL.
type Cache interface {
	Get(key string, out any) error
	Set(key string, value any, ttl time.Duration) error
	Remove(key string) error
	Exists(key string) (bool, error)
	GetOrSet(key string, out any, factory func(context.Context) (any, error), ttl time.Duration) error
	Close() error
}

// Options is the internal cache configuration resolved by New from the
// environment. It remains visible because provider constructors use it.
type Options struct {
	L1Provider                string
	L1SizeLimitMB             int
	L1DefaultTTL              time.Duration
	L1ExpirationScanFrequency time.Duration
	L1SlidingExpiration       bool

	Connection             string
	Password               string
	Database               int
	KeyPrefix              string
	ConnectTimeout         time.Duration
	ReadTimeout            time.Duration
	RetryCount             int
	RetryBaseDelay         time.Duration
	CircuitBreakerFailures int
	CircuitBreakerDuration time.Duration

	// OperationTimeout bounds every cache operation issued by this library
	// (get/set/remove/get-or-set/warm/touch/health-check). Zero or negative
	// values fall back to defaultOperationTimeout (5s). Environment override:
	// HELLNET_CACHE_OPERATION_TIMEOUT_MS (integer milliseconds).
	OperationTimeout time.Duration

	DefaultSerializer string

	EnableL1    bool
	EnableL2    bool
	DefaultTTL  time.Duration
	MaxTTL      time.Duration
	TouchOnRead bool
	TouchTTL    time.Duration
}

// validate checks that required fields are set when their feature is enabled.
func (o Options) validate() error {
	var missing []string
	if o.EnableL2 {
		if o.Connection == "" {
			missing = append(missing, "HELLNET_CACHE_CONNECTION")
		}
	}
	if len(missing) == 0 {
		return nil
	}
	return fmt.Errorf("hellnet-cache: required environment variables are missing: %v\n"+
		"set them before startup, e.g.:\n"+
		"  export HELLNET_CACHE_CONNECTION=localhost:6379", missing)
}

// formatKey applies the external backend key prefix.
func (o Options) formatKey(key string) string {
	return o.KeyPrefix + key
}

// capTTL clamps ttl to MaxTTL. A non-positive MaxTTL is treated as "no limit
// configured": ttl is returned untouched instead of collapsing to zero or
// negative durations (which would disable expiration downstream).
func (o Options) capTTL(ttl time.Duration) time.Duration {
	if o.MaxTTL > 0 && ttl > o.MaxTTL {
		return o.MaxTTL
	}
	return ttl
}

// resolveTTL is the single TTL-resolution point for every duration entering
// the provider stack (user Set paths as well as background warm/touch writes):
// a non-positive ttl falls back to Options.DefaultTTL, then the result is
// clamped to Options.MaxTTL by capTTL. All TTL writes MUST route through this
// helper so clamping cannot be bypassed.
func (o Options) resolveTTL(ttl time.Duration) time.Duration {
	return o.capTTL(defaultTTL(ttl, o.DefaultTTL))
}

// Metrics tracks hits, misses, sets and removes per layer. Safe for concurrent use.
type Metrics struct {
	layerName string

	hits    int64
	misses  int64
	sets    int64
	removes int64
}

func newMetrics(layerName string) *Metrics {
	return &Metrics{layerName: layerName}
}

// RecordHit records a cache hit.
func (m *Metrics) RecordHit() { atomic.AddInt64(&m.hits, 1) }

// RecordMiss records a cache miss.
func (m *Metrics) RecordMiss() { atomic.AddInt64(&m.misses, 1) }

// RecordSet records a write.
func (m *Metrics) RecordSet() { atomic.AddInt64(&m.sets, 1) }

// RecordRemove records a delete.
func (m *Metrics) RecordRemove() { atomic.AddInt64(&m.removes, 1) }

// Hits returns the number of hits.
func (m *Metrics) Hits() int64 { return atomic.LoadInt64(&m.hits) }

// Misses returns the number of misses.
func (m *Metrics) Misses() int64 { return atomic.LoadInt64(&m.misses) }

// Sets returns the number of writes.
func (m *Metrics) Sets() int64 { return atomic.LoadInt64(&m.sets) }

// Removes returns the number of deletes.
func (m *Metrics) Removes() int64 { return atomic.LoadInt64(&m.removes) }

// Total returns hits + misses.
func (m *Metrics) Total() int64 { return atomic.LoadInt64(&m.hits) + atomic.LoadInt64(&m.misses) }

// HitRate returns the hit rate as a percentage.
func (m *Metrics) HitRate() float64 {
	total := m.Total()
	if total == 0 {
		return 0
	}
	return float64(atomic.LoadInt64(&m.hits)) / float64(total) * 100
}

// Reset zeroes all counters.
func (m *Metrics) Reset() {
	atomic.StoreInt64(&m.hits, 0)
	atomic.StoreInt64(&m.misses, 0)
	atomic.StoreInt64(&m.sets, 0)
	atomic.StoreInt64(&m.removes, 0)
}

func (m *Metrics) String() string {
	return fmt.Sprintf("[%s] Hits: %d, Misses: %d, HitRate: %.1f%%, Sets: %d, Removes: %d",
		m.layerName, m.Hits(), m.Misses(), m.HitRate(), m.Sets(), m.Removes())
}

// JSONSerializer is the default serializer using encoding/json.
type JSONSerializer struct{}

// NewJSONSerializer returns a JSON serializer.
func NewJSONSerializer() *JSONSerializer { return &JSONSerializer{} }

// Serialize marshals a value to bytes.
func (s *JSONSerializer) Serialize(v any) ([]byte, error) {
	return json.Marshal(v)
}

// Deserialize unmarshals bytes into out.
func (s *JSONSerializer) Deserialize(data []byte, out any) error {
	return json.Unmarshal(data, out)
}

// HybridCache orchestrates the multi-layer cache with read-through and
// write-through semantics: L1 (memory) -> L2 (external).
//
// Context model: the library-owned baseCtx is propagated internally — public
// methods never accept a context. Each logical
// operation derives a per-operation timeout context from baseCtx via opCtx();
// background goroutines (warming/touch) do the same so everything halts
// coherently when Close is called.
//
// Concurrency guarantees:
//   - GetOrSet: stampede-protected via singleflight — at most one factory
//     execution per key at any instant; coalesced waiters receive the leader's
//     result instead of re-running the factory
//   - Idempotent: same singleflight coalescing per record key; completion
//     records are never poisoned by failed executions (they stay retriable)
//   - Allow/Lock: fixed-window counting and compare-and-delete leases,
//     atomically server-side when an L2 implements Scripter, else bounded
//     mutex-guarded process-local structures
//   - read-through warming: deduplicated (only one warming task per key)
//   - touch-on-read: optionally extends TTL on hit in upper layers
//   - all layer writes are parallel (goroutines + WaitGroup)
type HybridCache struct {
	baseCtx    context.Context
	cancel     context.CancelFunc
	opts       Options
	serializer Serializer
	providers  []Provider
	logger     *log.Logger

	flight  singleflight.Group // GetOrSet stampede protection, keyed by cache key
	warming sync.Map           // key string -> struct{}

	// Process-local coordination state backing the Idempotent/Lock memory
	// fallback paths (no Scripter provider wired). Guarded by memMu;
	// nil maps are created lazily.
	memMu sync.Mutex
	memRL map[string]*rateWindow // fixed-window rate counters ("rl:"+key)
	memLK map[string]*lockEntry  // process-local locks ("lock:"+key)
}

// Compile-time proof that HybridCache continues to satisfy the Cache
// abstraction after API surface changes.
var _ Cache = (*HybridCache)(nil)

// New follows the hellnet-lib-telemetry constructor pattern: it creates the
// base context, loads .env, and resolves configuration from HELLNET_CACHE_*
// with HELLNET_* as fallback.
// If L2 is enabled but no HELLNET_CACHE_CONNECTION is present, the library
// automatically falls back to memory-only (L2 disabled) instead of erroring.
func New() (*HybridCache, error) {
	ctx := context.Background()

	_ = environments.LoadDotEnv()

	o := Options{
		L1Provider:                environments.GetString("HELLNET_CACHE_", "HELLNET_", "L1_PROVIDER", "memory"),
		L1SizeLimitMB:             environments.GetInt("HELLNET_CACHE_", "HELLNET_", "L1_SIZE_LIMIT_MB", 100),
		L1DefaultTTL:              environments.GetDuration("HELLNET_CACHE_", "HELLNET_", "L1_DEFAULT_TTL", 5*time.Minute),
		L1ExpirationScanFrequency: environments.GetDuration("HELLNET_CACHE_", "HELLNET_", "L1_EXPIRATION_SCAN_FREQUENCY", time.Minute),
		L1SlidingExpiration:       environments.GetBool("HELLNET_CACHE_", "HELLNET_", "L1_SLIDING_EXPIRATION", false),
		Connection:                environments.GetString("HELLNET_CACHE_", "HELLNET_", "CONNECTION", ""),
		Password:                  environments.GetString("HELLNET_CACHE_", "HELLNET_", "PASSWORD", ""),
		Database:                  environments.GetInt("HELLNET_CACHE_", "HELLNET_", "DATABASE", 0),
		KeyPrefix:                 environments.GetString("HELLNET_CACHE_", "HELLNET_", "KEY_PREFIX", "hellnet:cache:"),
		ConnectTimeout:            environments.GetDuration("HELLNET_CACHE_", "HELLNET_", "CONNECT_TIMEOUT", 5*time.Second),
		ReadTimeout:               environments.GetDuration("HELLNET_CACHE_", "HELLNET_", "SYNC_TIMEOUT", time.Second),
		RetryCount:                environments.GetInt("HELLNET_CACHE_", "HELLNET_", "RETRY_COUNT", 2),
		RetryBaseDelay:            environments.GetDuration("HELLNET_CACHE_", "HELLNET_", "RETRY_BASE_DELAY_MS", 200*time.Millisecond),
		CircuitBreakerFailures:    environments.GetInt("HELLNET_CACHE_", "HELLNET_", "CB_FAILURES", 5),
		CircuitBreakerDuration:    environments.GetDuration("HELLNET_CACHE_", "HELLNET_", "CB_DURATION_SEC", 30*time.Second),
		OperationTimeout:          time.Duration(environments.GetInt("HELLNET_CACHE_", "HELLNET_", "OPERATION_TIMEOUT_MS", 5000)) * time.Millisecond,
		DefaultSerializer:         environments.GetString("HELLNET_CACHE_", "HELLNET_", "DEFAULT_SERIALIZER", "json"),
		EnableL1:                  environments.GetBool("HELLNET_CACHE_", "HELLNET_", "ENABLE_L1", true),
		EnableL2:                  environments.GetBool("HELLNET_CACHE_", "HELLNET_", "ENABLE_L2", true),
		DefaultTTL:                environments.GetDuration("HELLNET_CACHE_", "HELLNET_", "DEFAULT_TTL", 30*time.Minute),
		MaxTTL:                    environments.GetDuration("HELLNET_CACHE_", "HELLNET_", "MAX_TTL", 24*time.Hour),
		TouchOnRead:               environments.GetBool("HELLNET_CACHE_", "HELLNET_", "TOUCH_ON_READ", false),
		TouchTTL:                  environments.GetDuration("HELLNET_CACHE_", "HELLNET_", "TOUCH_TTL", 10*time.Minute),
	}
	return newWithOptions(ctx, o)
}

// newWithOptions is the explicit construction seam used by package tests.
func newWithOptions(ctx context.Context, o Options) (*HybridCache, error) {
	if o.EnableL2 && o.Connection == "" {
		log.Printf("[hellnet-cache] HELLNET_CACHE_CONNECTION not set — falling back to memory-only (L2 disabled)")
		o.EnableL2 = false
	}

	if err := o.validate(); err != nil {
		return nil, err
	}

	// Derive our own child so Close() can tear down library-owned work without
	// touching the caller's context lifecycle (and vice versa).
	baseCtx, cancel := context.WithCancel(ctx)

	var providers []Provider

	if o.EnableL1 {
		mp, err := NewMemoryProvider(o)
		if err != nil {
			cancel()
			return nil, err
		}
		providers = append(providers, mp)
	}

	if o.EnableL2 {
		providers = append(providers, NewExternalProvider(baseCtx, o))
	}

	return &HybridCache{
		baseCtx:    baseCtx,
		cancel:     cancel,
		opts:       o,
		serializer: NewJSONSerializer(),
		providers:  providers,
		logger:     log.Default(),
	}, nil
}

// MustNew is like New but panics on error. Use at startup.
func MustNew() *HybridCache {
	c, err := New()
	if err != nil {
		panic(err)
	}
	return c
}

func mustNewWithOptions(ctx context.Context, o Options) *HybridCache {
	c, err := newWithOptions(ctx, o)
	if err != nil {
		panic(err)
	}
	return c
}

// opCtx derives an operation-scoped context from the context captured at
// construction, bounded by Options.OperationTimeout. It governs library-owned
// work: the GetOrSet factory, and the background warming/touch goroutines.
// Backend I/O is additionally bounded by each provider's internal timeout
// context. Callers must invoke the returned CancelFunc.
func (h *HybridCache) opCtx() (context.Context, context.CancelFunc) {
	t := h.opts.OperationTimeout
	if t <= 0 {
		t = defaultOperationTimeout
	}
	return context.WithTimeout(h.baseCtx, t)
}

// Get retrieves a value by key under the internal operation context. Returns
// the zero value if not found in any layer.
func (h *HybridCache) Get(key string, out any) error {
	data, foundAtIndex := h.getRaw(key)
	if foundAtIndex < 0 || data == nil {
		return nil // not found; out stays zero value
	}
	return h.serializer.Deserialize(data, out)
}

// getRaw walks providers L1->L2 returning the first hit and its index,
// triggering warming/touch side-effects. Each provider bounds its own I/O via
// its internal operation context.
func (h *HybridCache) getRaw(key string) (data []byte, foundAtIndex int) {
	for i, p := range h.providers {
		v, err := p.Get(key)
		if err != nil {
			continue
		}
		if v != nil {
			if h.opts.TouchOnRead && i > 0 {
				h.touch(key, v)
			}
			if i > 0 {
				h.warm(key, v, i)
			}
			return v, i
		}
	}
	return nil, -1
}

// Set stores a value with optional TTL, written to all enabled layers under
// the internal operation context. A ttl of 0 uses the layer's default.
func (h *HybridCache) Set(key string, value any, ttl time.Duration) error {
	data, err := h.serializer.Serialize(value)
	if err != nil {
		return err
	}
	return h.SetBytes(key, data, ttl)
}

// SetBytes writes pre-serialized bytes to all enabled layers in parallel under
// each provider's internal operation context. It is the low-level primitive
// behind Set; prefer Set unless you already have the serialized representation.
// A ttl of 0 uses the layer's default.
//
// Failure semantics: if at least one layer persists the value, nil is returned
// even when other layers fail — degraded-but-successful, since lost copies are
// re-populated by read-through warming (each failure is logged as a warning).
// An error is returned only when NO layer managed to persist, so callers can
// react to a total write failure. Per-provider failures are aggregated with
// errors.Join and prefixed with the failing layer's name.
func (h *HybridCache) SetBytes(key string, data []byte, ttl time.Duration) error {
	actual := h.opts.resolveTTL(ttl)

	errs := make([]error, len(h.providers)) // index-disjoint writes: race-safe
	var wg sync.WaitGroup
	for i, p := range h.providers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = p.Set(key, data, actual)
		}()
	}
	wg.Wait()

	var failures []error
	for i, err := range errs {
		if err == nil {
			continue
		}
		h.logger.Printf("[hellnet-cache] set failed on %s for key %s: %v", h.providers[i].Name(), key, err)
		failures = append(failures, fmt.Errorf("%s: %w", h.providers[i].Name(), err))
	}
	switch len(failures) {
	case 0:
		return nil
	case len(h.providers):
		// Total failure: every layer refused the write. Callers must know.
		return errors.Join(failures...)
	default:
		// Degraded-but-successful: some layer still holds the value.
		h.logger.Printf("[hellnet-cache] set degraded for key %s (some layers failed): %v",
			key, errors.Join(failures...))
		return nil
	}
}

// Remove deletes a key from all layers under each provider's internal
// operation context.
func (h *HybridCache) Remove(key string) error {
	var wg sync.WaitGroup
	for _, p := range h.providers {
		wg.Add(1)
		go func(pr Provider) {
			defer wg.Done()
			if err := pr.Remove(key); err != nil {
				h.logger.Printf("[hellnet-cache] remove failed on %s for key %s: %v", pr.Name(), key, err)
			}
		}(p)
	}
	wg.Wait()
	return nil
}

// Exists reports whether a key exists in any layer under each provider's
// internal operation context.
func (h *HybridCache) Exists(key string) (bool, error) {
	for _, p := range h.providers {
		ok, err := p.Exists(key)
		if err != nil {
			continue
		}
		if ok {
			return true, nil
		}
	}
	return false, nil
}

// Healthy aggregates the health of every wired provider: it returns nil when
// all providers pass their health check, otherwise a joined error naming each
// unhealthy layer. The in-memory L1 is trivially healthy while alive; an
// unreachable L2 surfaces here as an error without breaking reads (reads and
// writes degrade gracefully instead).
func (h *HybridCache) Healthy() error {
	var errs []error
	for _, p := range h.providers {
		if !p.HealthCheck() {
			errs = append(errs, fmt.Errorf("%s: unhealthy", p.Name()))
		}
	}
	return errors.Join(errs...)
}

// GetOrSet retrieves a value or, if missing, runs the factory and caches the
// result. Stampede-protected per key: concurrent callers for the same key are
// coalesced into a single factory execution and every waiter receives the
// leader's result (serialize + write happen once).
//
// The factory receives a LIBRARY-DERIVED context (an operation-scoped child of
// the context captured at New, bounded by Options.OperationTimeout): callers
// who don't care may ignore it, long computations should honor its
// cancellation. When calls are coalesced, the shared execution runs under the
// operation context of the caller that won the execution slot.
func (h *HybridCache) GetOrSet(key string, out any, factory func(context.Context) (any, error), ttl time.Duration) error {
	ctx, cancel := h.opCtx()
	defer cancel()

	// fast path: serve hits without contending on the flight group.
	if data, foundAtIndex := h.getRaw(key); foundAtIndex >= 0 && data != nil {
		return h.serializer.Deserialize(data, out)
	}

	type flightResult struct{ data []byte }

	res, err, _ := h.flight.Do(key, func() (any, error) {
		// double-check after winning the execution slot: another call may
		// have populated the entry between our fast path and acquiring the key.
		if data, foundAtIndex := h.getRaw(key); foundAtIndex >= 0 && data != nil {
			return flightResult{data: data}, nil
		}

		value, ferr := factory(ctx)
		if ferr != nil {
			return nil, ferr
		}
		// Serialize once; SetBytes persists it and out is populated from the
		// same bytes below — also for coalesced waiters.
		data, serr := h.serializer.Serialize(value)
		if serr != nil {
			return nil, serr
		}
		if serr := h.SetBytes(key, data, ttl); serr != nil {
			return nil, serr
		}
		return flightResult{data: data}, nil
	})
	if err != nil {
		return err
	}

	fr, ok := res.(flightResult)
	if !ok || fr.data == nil {
		return nil // unreachable with current callback; defensive
	}
	return h.serializer.Deserialize(fr.data, out)
}

// Close releases all providers and cancels the context captured at New,
// aborting any in-flight internal work (including fire-and-forget warm/touch
// goroutines).
func (h *HybridCache) Close() error {
	if h.cancel != nil {
		h.cancel()
	}
	var firstErr error
	for _, p := range h.providers {
		if err := p.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// warm populates lower layers (deduplicated fire-and-forget). Runs on its own
// operation context derived from the context captured at New — deliberately
// decoupled from the request-scoped read that triggered it.
func (h *HybridCache) warm(key string, data []byte, foundAtIndex int) {
	if _, loaded := h.warming.LoadOrStore(key, struct{}{}); loaded {
		return // already warming
	}
	go func() {
		defer h.warming.Delete(key)

		ctx, cancel := h.opCtx()
		defer cancel()

		// Route the raw L1 default through the same resolution path as user
		// sets: an oversized L1DefaultTTL must still be clamped by MaxTTL.
		ttl := h.opts.resolveTTL(h.opts.L1DefaultTTL)
		for i := 0; i < foundAtIndex; i++ {
			if err := ctx.Err(); err != nil {
				// Captured context cancelled or op deadline exceeded — stop
				// warming early instead of fanning out doomed writes.
				return
			}
			if err := h.providers[i].Set(key, data, ttl); err != nil {
				h.logger.Printf("[hellnet-cache] warming failed for key %s: %v", key, err)
			}
		}
	}()
}

// touch extends TTL on hit (fire-and-forget, non-critical). Each goroutine
// derives its own operation context from the context captured at New and uses
// it as a shutdown guard — the write itself inherits cancellation through the
// provider's captured base context.
func (h *HybridCache) touch(key string, data []byte) {
	// Same resolution path as every other TTL write (default fallback + MaxTTL clamp).
	ttl := h.opts.resolveTTL(h.opts.TouchTTL)
	for _, p := range h.providers {
		go func(pr Provider) {
			ctx, cancel := h.opCtx()
			defer cancel()

			select {
			case <-ctx.Done():
				return // captured context cancelled — skip doomed write
			default:
			}
			_ = pr.Set(key, data, ttl)
		}(p)
	}
}

// defaultTTL returns ttl when non-zero, else fallback.
func defaultTTL(ttl, fallback time.Duration) time.Duration {
	if ttl > 0 {
		return ttl
	}
	return fallback
}
