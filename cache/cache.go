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
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/guilhermelinosp/hellnet-lib-cache/internal/env"
	"github.com/guilhermelinosp/hellnet-lib-telemetry/instrument"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
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

// contextProvider is an optional extension. Provider keeps its original
// public shape while ctx-first cache calls can pass the request context to I/O.
type contextProvider interface {
	GetContext(context.Context, string) ([]byte, error)
	SetContext(context.Context, string, []byte, time.Duration) error
	RemoveContext(context.Context, string) error
	ExistsContext(context.Context, string) (bool, error)
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
	// L1Provider selects the in-process provider. Currently only "memory" is supported.
	L1Provider    string
	L1SizeLimitMB int
	L1DefaultTTL  time.Duration
	// Deprecated: ristretto owns expiration scanning; retained for compatibility.
	L1ExpirationScanFrequency time.Duration
	L1SlidingExpiration       bool

	Connection     string
	Password       string
	Database       int
	KeyPrefix      string
	ConnectTimeout time.Duration
	ReadTimeout    time.Duration
	RetryCount     int
	// RetryBaseDelay controls the base Redis retry backoff.
	RetryBaseDelay         time.Duration
	CircuitBreakerFailures int
	CircuitBreakerDuration time.Duration

	// OperationTimeout bounds every cache operation issued by this library
	// (get/set/remove/get-or-set/warm/touch/health-check). Zero or negative
	// values fall back to defaultOperationTimeout (5s). Environment override:
	// HELLNET_CACHE_OPERATION_TIMEOUT_MS (integer milliseconds).
	OperationTimeout time.Duration

	// DefaultSerializer selects the built-in serializer. "json" is supported.
	DefaultSerializer string

	EnableL1    bool
	EnableL2    bool
	DefaultTTL  time.Duration
	MaxTTL      time.Duration
	TouchOnRead bool
	TouchTTL    time.Duration

	l2Explicit bool
}

// validate checks that required fields are set when their feature is enabled.
func (o Options) validate() error {
	if o.L1Provider != "" && o.L1Provider != "memory" {
		return fmt.Errorf("cache: unsupported L1 provider %q", o.L1Provider)
	}
	if o.DefaultSerializer != "" && o.DefaultSerializer != "json" {
		return fmt.Errorf("cache: unsupported serializer %q", o.DefaultSerializer)
	}
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
// Deprecated: use the OpenTelemetry metrics emitted through WithInstrumentation.
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
	baseCtx    context.Context //nolint:containedctx // TODO(telemetry-fase-D): legacy wrapper retains construction context.
	cancel     context.CancelFunc
	opts       Options
	serializer Serializer
	providers  []Provider
	obs        observability
	now        func() time.Time

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

// New creates a cache from the environment: it loads .env and resolves
// configuration from HELLNET_CACHE_* with HELLNET_* as fallback. inst is the
// Hellnet observability contract (for example a *telemetry.Telemetry, or nil to
// emit no telemetry). If L2 is explicitly enabled but no
// HELLNET_CACHE_CONNECTION is present, New returns an error. Without explicit
// enablement it falls back to memory-only. Use NewWithOptions to supply
// explicit options, providers or a serializer.
func New(ctx context.Context, inst instrument.Instrumentation) (*HybridCache, error) {
	_ = env.Environment()
	o, err := optionsFromEnv()
	if err != nil {
		return nil, err
	}
	return newWithDependencies(ctx, o, nil, nil, false, inst)
}

func optionsFromEnv() (Options, error) {
	prefixes := []string{"HELLNET_CACHE_", "HELLNET_"}
	var readErr error
	readDuration := func(key string, fallback time.Duration) time.Duration {
		value, err := cacheDurationEnv(prefixes, key, fallback)
		if err != nil && readErr == nil {
			readErr = err
		}
		return value
	}
	o := Options{
		L1Provider:                cacheEnv(prefixes, "L1_PROVIDER", "memory"),
		L1SizeLimitMB:             cacheIntEnv(prefixes, "L1_SIZE_LIMIT_MB", 100),
		L1DefaultTTL:              readDuration("L1_DEFAULT_TTL", 5*time.Minute),
		L1ExpirationScanFrequency: readDuration("L1_EXPIRATION_SCAN_FREQUENCY", time.Minute),
		L1SlidingExpiration:       cacheBoolEnv(prefixes, "L1_SLIDING_EXPIRATION", false),
		Connection:                cacheEnv(prefixes, "CONNECTION", ""),
		Password:                  cacheEnv(prefixes, "PASSWORD", ""),
		Database:                  cacheIntEnv(prefixes, "DATABASE", 0),
		KeyPrefix:                 cacheEnv(prefixes, "KEY_PREFIX", "hellnet:cache:"),
		ConnectTimeout:            readDuration("CONNECT_TIMEOUT", 5*time.Second),
		ReadTimeout:               readDuration("SYNC_TIMEOUT", time.Second),
		RetryCount:                cacheIntEnv(prefixes, "RETRY_COUNT", 2),
		RetryBaseDelay:            readDuration("RETRY_BASE_DELAY_MS", 200*time.Millisecond),
		CircuitBreakerFailures:    cacheIntEnv(prefixes, "CB_FAILURES", 5),
		CircuitBreakerDuration:    readDuration("CB_DURATION_SEC", 30*time.Second),
		OperationTimeout:          readDuration("OPERATION_TIMEOUT_MS", 5*time.Second),
		DefaultSerializer:         cacheEnv(prefixes, "DEFAULT_SERIALIZER", "json"),
		EnableL1:                  cacheBoolEnv(prefixes, "ENABLE_L1", true),
		EnableL2:                  cacheBoolEnv(prefixes, "ENABLE_L2", true),
		DefaultTTL:                readDuration("DEFAULT_TTL", 30*time.Minute),
		MaxTTL:                    readDuration("MAX_TTL", 24*time.Hour),
		TouchOnRead:               cacheBoolEnv(prefixes, "TOUCH_ON_READ", false),
		TouchTTL:                  readDuration("TOUCH_TTL", 10*time.Minute),
	}
	if raw, ok := cacheLookupEnv(prefixes, "ENABLE_L2"); ok && strings.EqualFold(strings.TrimSpace(raw), "true") {
		o.l2Explicit = true
	}
	if readErr != nil {
		return Options{}, readErr
	}
	return o, nil
}

// newWithOptions is the explicit construction seam used by package tests.
func newWithOptions(ctx context.Context, o Options) (*HybridCache, error) {
	return newWithDependencies(ctx, o, nil, nil, false, nil)
}

// Option configures NewWithOptions without changing the existing constructors.
type Option func(*constructorOptions) error

type constructorOptions struct {
	options         Options
	hasOptions      bool
	providers       []Provider
	customProvider  bool
	serializer      Serializer
	instrumentation instrument.Instrumentation
}

// WithInstrumentation supplies the Hellnet observability contract.
func WithInstrumentation(inst instrument.Instrumentation) Option {
	return func(config *constructorOptions) error {
		config.instrumentation = inst
		return nil
	}
}

// WithOptions supplies explicit cache configuration.
func WithOptions(options Options) Option {
	return func(config *constructorOptions) error {
		config.options = options
		config.hasOptions = true
		return nil
	}
}

// WithProviders supplies an explicit provider stack for tests or custom backends.
func WithProviders(providers ...Provider) Option {
	return func(config *constructorOptions) error {
		config.providers = append([]Provider(nil), providers...)
		config.customProvider = true
		return nil
	}
}

// WithSerializer supplies the serializer used for cache values.
func WithSerializer(serializer Serializer) Option {
	return func(config *constructorOptions) error {
		if serializer == nil {
			return fmt.Errorf("cache: serializer must not be nil")
		}
		config.serializer = serializer
		return nil
	}
}

// NewWithOptions constructs a cache without loading dotenv files. New remains
// the environment-first constructor and keeps its existing dotenv behavior.
func NewWithOptions(ctx context.Context, options ...Option) (*HybridCache, error) {
	config := constructorOptions{}
	var err error
	for _, option := range options {
		if option == nil {
			continue
		}
		if err := option(&config); err != nil {
			return nil, err
		}
	}
	if !config.hasOptions {
		config.options, err = optionsFromEnv()
		if err != nil {
			return nil, err
		}
	}
	return newWithDependencies(ctx, config.options, config.providers, config.serializer, config.customProvider, config.instrumentation)
}

func newWithDependencies(ctx context.Context, o Options, supplied []Provider, serializer Serializer, customProviders bool, inst instrument.Instrumentation) (*HybridCache, error) {
	if o.EnableL2 && o.Connection == "" {
		if o.l2Explicit {
			return nil, fmt.Errorf("cache: L2 explicitly enabled but HELLNET_CACHE_CONNECTION is not configured")
		}
		warnL2DegradedOnce(inst) //nolint:contextcheck // constructor warning has no caller context.
		o.EnableL2 = false
	}

	if err := o.validate(); err != nil {
		return nil, err
	}

	// Derive our own child so Close() can tear down library-owned work without
	// touching the caller's context lifecycle (and vice versa).
	baseCtx, cancel := context.WithCancel(ctx)

	var providers []Provider
	if customProviders {
		providers = append(providers, supplied...)
	} else {
		if o.EnableL1 {
			mp, err := NewMemoryProvider(o)
			if err != nil {
				cancel()
				return nil, err
			}
			providers = append(providers, mp)
		}

		if o.EnableL2 {
			providers = append(providers, newExternalProvider(baseCtx, o, inst))
		}
	}
	if serializer == nil {
		serializer = NewJSONSerializer()
	}

	return &HybridCache{
		baseCtx:    baseCtx,
		cancel:     cancel,
		opts:       o,
		serializer: serializer,
		providers:  providers,
		obs:        newObservability(inst), //nolint:contextcheck // constructor initializes providers, not request work.
		now:        time.Now,
	}, nil
}

var l2DegradedWarning sync.Once

func warnL2DegradedOnce(inst instrument.Instrumentation) {
	l2DegradedWarning.Do(func() {
		if inst == nil {
			inst = instrument.Noop()
		}
		inst.Logger(instrumentationScope).Warn(context.TODO(), "cache L2 disabled: connection not configured")
	})
}

// MustNew is like New but panics on error. Use at startup.
func MustNew(ctx context.Context, inst instrument.Instrumentation) *HybridCache {
	c, err := New(ctx, inst)
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
	return h.opCtxFrom(h.baseCtx)
}

func (h *HybridCache) opCtxFrom(parent context.Context) (context.Context, context.CancelFunc) {
	t := h.opts.OperationTimeout
	if t <= 0 {
		t = defaultOperationTimeout
	}
	return context.WithTimeout(ctxOrBackground(parent), t)
}

func (h *HybridCache) nowTime() time.Time {
	if h.now != nil {
		return h.now()
	}
	return time.Now()
}

// Get retrieves a value by key under the internal operation context. Returns
// the zero value if not found in any layer.
func (h *HybridCache) Get(key string, out any) error {
	return h.GetContext(h.baseCtx, key, out)
}

// GetContext retrieves a value using the caller's context as span and I/O parent.
func (h *HybridCache) GetContext(ctx context.Context, key string, out any) (err error) {
	ctx, span := h.obs.tracer.Start(ctxOrBackground(ctx), "cache.get", trace.WithAttributes(attribute.String("hellnet.cache.operation", "get")))
	defer span.End()
	started := time.Now()
	result := "miss"
	defer func() {
		if err != nil {
			result = "error"
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		h.obs.observe(ctx, "get", result, started)
	}()
	data, foundAtIndex := h.getRawContext(ctx, key)
	hit := foundAtIndex >= 0 && data != nil
	span.SetAttributes(attribute.Bool("hellnet.cache.hit", hit))
	if hit {
		result = "hit"
		span.SetAttributes(attribute.String("hellnet.cache.layer", h.providers[foundAtIndex].Name()))
	}
	if foundAtIndex < 0 || data == nil {
		return nil // not found; out stays zero value
	}
	return h.serializer.Deserialize(data, out)
}

// getRaw walks providers L1->L2 returning the first hit and its index,
// triggering warming/touch side-effects. Each provider bounds its own I/O via
// its internal operation context.
func (h *HybridCache) getRaw(key string) (data []byte, foundAtIndex int) {
	return h.getRawContext(h.baseCtx, key)
}

func (h *HybridCache) getRawContext(ctx context.Context, key string) (data []byte, foundAtIndex int) {
	for i, p := range h.providers {
		v, err := providerGet(ctx, p, key)
		if err != nil {
			continue
		}
		if v != nil {
			if h.opts.TouchOnRead && i > 0 {
				h.touch(key, v)
			}
			if i > 0 {
				h.warm(key, v, i) //nolint:contextcheck // legacy background warming receives no caller ownership.
			}
			return v, i
		}
	}
	return nil, -1
}

// Set stores a value with optional TTL, written to all enabled layers under
// the internal operation context. A ttl of 0 uses the layer's default.
func (h *HybridCache) Set(key string, value any, ttl time.Duration) error {
	return h.SetContext(h.baseCtx, key, value, ttl)
}

// SetContext stores a value using the caller's context as span and I/O parent.
func (h *HybridCache) SetContext(ctx context.Context, key string, value any, ttl time.Duration) (err error) {
	ctx, span := h.obs.tracer.Start(ctxOrBackground(ctx), "cache.set", trace.WithAttributes(attribute.String("hellnet.cache.operation", "set")))
	defer span.End()
	started := time.Now()
	defer func() {
		result := "success"
		if err != nil {
			result = "error"
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		h.obs.observe(ctx, "set", result, started)
	}()
	data, err := h.serializer.Serialize(value)
	if err != nil {
		return err
	}
	return h.setBytesContext(ctx, key, data, ttl)
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
	return h.setBytesContext(h.baseCtx, key, data, ttl)
}

// SetBytesContext writes serialized data using the caller's context.
func (h *HybridCache) SetBytesContext(ctx context.Context, key string, data []byte, ttl time.Duration) (err error) {
	ctx, span := h.obs.tracer.Start(ctxOrBackground(ctx), "cache.set", trace.WithAttributes(attribute.String("hellnet.cache.operation", "set")))
	defer span.End()
	started := time.Now()
	defer func() {
		result := "success"
		if err != nil {
			result = "error"
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		h.obs.observe(ctx, "set", result, started)
	}()
	return h.setBytesContext(ctx, key, data, ttl)
}

func (h *HybridCache) setBytesContext(ctx context.Context, key string, data []byte, ttl time.Duration) error {
	actual := h.opts.resolveTTL(ttl)

	errs := make([]error, len(h.providers)) // index-disjoint writes: race-safe
	var wg sync.WaitGroup
	for i, p := range h.providers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = providerSet(ctx, p, key, data, actual)
		}()
	}
	wg.Wait()

	var failures []error
	for i, err := range errs {
		if err == nil {
			continue
		}
		h.obs.logger.Warn(ctx, "cache set failed", "layer", h.providers[i].Name(), "error", err)
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
		h.obs.logger.Warn(ctx, "cache set degraded", "error", errors.Join(failures...))
		return nil
	}
}

// Remove deletes a key from all layers under each provider's internal
// operation context.
func (h *HybridCache) Remove(key string) error {
	return h.RemoveContext(h.baseCtx, key)
}

// RemoveContext removes a value using the caller's context.
func (h *HybridCache) RemoveContext(ctx context.Context, key string) error {
	ctx, span := h.obs.tracer.Start(ctxOrBackground(ctx), "cache.remove", trace.WithAttributes(attribute.String("hellnet.cache.operation", "remove")))
	defer span.End()
	started := time.Now()
	defer func() { h.obs.observe(ctx, "remove", "success", started) }()
	var wg sync.WaitGroup
	for _, p := range h.providers {
		wg.Add(1)
		go func(pr Provider) {
			defer wg.Done()
			if err := providerRemove(ctx, pr, key); err != nil {
				h.obs.logger.Warn(ctx, "cache remove failed", "layer", pr.Name(), "error", err)
			}
		}(p)
	}
	wg.Wait()
	return nil
}

// Exists reports whether a key exists in any layer under each provider's
// internal operation context.
func (h *HybridCache) Exists(key string) (bool, error) {
	return h.ExistsContext(h.baseCtx, key)
}

// ExistsContext checks presence using the caller's context.
func (h *HybridCache) ExistsContext(ctx context.Context, key string) (bool, error) {
	ctx, span := h.obs.tracer.Start(ctxOrBackground(ctx), "cache.exists", trace.WithAttributes(attribute.String("hellnet.cache.operation", "exists")))
	defer span.End()
	started := time.Now()
	defer func() { h.obs.observe(ctx, "exists", "success", started) }()
	for _, p := range h.providers {
		ok, err := providerExists(ctx, p, key)
		if err != nil {
			continue
		}
		if ok {
			return true, nil
		}
	}
	return false, nil
}

func ctxOrBackground(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}
func providerGet(ctx context.Context, p Provider, key string) ([]byte, error) {
	if cp, ok := p.(contextProvider); ok {
		return cp.GetContext(ctx, key)
	}
	return p.Get(key)
}
func providerSet(ctx context.Context, p Provider, key string, value []byte, ttl time.Duration) error {
	if cp, ok := p.(contextProvider); ok {
		return cp.SetContext(ctx, key, value, ttl)
	}
	return p.Set(key, value, ttl)
}
func providerRemove(ctx context.Context, p Provider, key string) error {
	if cp, ok := p.(contextProvider); ok {
		return cp.RemoveContext(ctx, key)
	}
	return p.Remove(key)
}
func providerExists(ctx context.Context, p Provider, key string) (bool, error) {
	if cp, ok := p.(contextProvider); ok {
		return cp.ExistsContext(ctx, key)
	}
	return p.Exists(key)
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

// HealthCheck reports provider health for registration in a service /health
// endpoint. It intentionally does not represent readiness: cache can degrade
// to L1 or a caller-selected fallback.
func (h *HybridCache) HealthCheck(ctx context.Context) error {
	started := time.Now()
	_, span := h.obs.tracer.Start(ctxOrBackground(ctx), "cache.health", trace.WithAttributes(attribute.String("hellnet.cache.operation", "health")))
	defer span.End()
	err := h.Healthy()
	result := "success"
	if err != nil {
		result = "error"
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	h.obs.observe(ctxOrBackground(ctx), "health", result, started)
	return err
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
	return h.GetOrSetContext(h.baseCtx, key, out, factory, ttl)
}

// GetOrSetContext retrieves or computes a value using the caller's context.
// Coalesced execution is detached from caller cancellation but retains trace
// values and is bounded by OperationTimeout.
func (h *HybridCache) GetOrSetContext(parent context.Context, key string, out any, factory func(context.Context) (any, error), ttl time.Duration) (err error) {
	ctx, span := h.obs.tracer.Start(ctxOrBackground(parent), "cache.get_or_set", trace.WithAttributes(attribute.String("hellnet.cache.operation", "get_or_set")))
	defer span.End()
	started := time.Now()
	resultName := "miss"
	defer func() {
		if err != nil {
			resultName = "error"
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		h.obs.observe(ctx, "get_or_set", resultName, started)
	}()
	ctx, cancel := h.opCtxFrom(ctx)
	defer cancel()

	// fast path: serve hits without contending on the flight group.
	if data, foundAtIndex := h.getRawContext(ctx, key); foundAtIndex >= 0 && data != nil {
		resultName = "hit"
		return h.serializer.Deserialize(data, out)
	}

	type flightResult struct{ data []byte }

	resultCh := h.flight.DoChan(key, func() (any, error) {
		sharedCtx, sharedCancel := h.opCtxFrom(context.WithoutCancel(ctx))
		defer sharedCancel()
		// double-check after winning the execution slot: another call may
		// have populated the entry between our fast path and acquiring the key.
		if data, foundAtIndex := h.getRawContext(sharedCtx, key); foundAtIndex >= 0 && data != nil {
			return flightResult{data: data}, nil
		}

		value, ferr := factory(sharedCtx)
		if ferr != nil {
			return nil, ferr
		}
		// Serialize once; SetBytes persists it and out is populated from the
		// same bytes below — also for coalesced waiters.
		data, serr := h.serializer.Serialize(value)
		if serr != nil {
			return nil, serr
		}
		if serr := h.setBytesContext(sharedCtx, key, data, ttl); serr != nil {
			return nil, serr
		}
		return flightResult{data: data}, nil
	})
	var result singleflight.Result
	select {
	case result = <-resultCh:
	case <-ctx.Done():
		return ctx.Err()
	}
	res, err := result.Val, result.Err
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
				h.obs.logger.Warn(ctx, "cache warming failed", "error", err)
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
