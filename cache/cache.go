// Package cache provides a multi-layer cache for Go with L1 (in-process memory)
// and L2 (external, pluggable distributed backend).
//
// It features write-through, read-through, stampede protection, env-first
// configuration and graceful degradation on backend failures. The distributed
// backend is backend-agnostic in the public API.
//
// This is a faithful port of the .NET Hellnet.Cache library.
package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/joho/godotenv"
)

// Provider is an individual cache layer (L1 memory, L2 external).
type Provider interface {
	Name() string
	Get(ctx context.Context, key string) ([]byte, error)
	Set(ctx context.Context, key string, value []byte, ttl *time.Duration) error
	Remove(ctx context.Context, key string) error
	Exists(ctx context.Context, key string) (bool, error)
	HealthCheck(ctx context.Context) bool
	Close() error
}

// Serializer marshals/unmarshals cache values.
type Serializer interface {
	Serialize(any) ([]byte, error)
	Deserialize([]byte, any) error
}

// Cache is the multi-layer cache abstraction. Write-through, read-through.
type Cache interface {
	Get(ctx context.Context, key string, out any) error
	Set(ctx context.Context, key string, value any, ttl *time.Duration) error
	Remove(ctx context.Context, key string) error
	Exists(ctx context.Context, key string) (bool, error)
	GetOrSet(ctx context.Context, key string, out any, factory func(context.Context) (any, error), ttl *time.Duration) error
	Close() error
}

// Options configures the cache. All fields are populated from environment
// variables (HELLNET_CACHE_*) via LoadFromEnv, or set explicitly.
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

	DefaultSerializer string

	EnableL1    bool
	EnableL2    bool
	DefaultTTL  time.Duration
	MaxTTL      time.Duration
	TouchOnRead bool
	TouchTTL    time.Duration
}

// DefaultOptions returns the default configuration.
func DefaultOptions() Options {
	return Options{
		L1Provider:                "memory",
		L1SizeLimitMB:             100,
		L1DefaultTTL:              5 * time.Minute,
		L1ExpirationScanFrequency: time.Minute,
		L1SlidingExpiration:       false,
		Connection:                "",
		Password:                  "",
		Database:                  0,
		KeyPrefix:                 "hellnet:cache:",
		ConnectTimeout:            5 * time.Second,
		ReadTimeout:               time.Second,
		RetryCount:                2,
		RetryBaseDelay:            200 * time.Millisecond,
		CircuitBreakerFailures:    5,
		CircuitBreakerDuration:    30 * time.Second,
		DefaultSerializer:         "json",
		EnableL1:                  true,
		EnableL2:                  true,
		DefaultTTL:                30 * time.Minute,
		MaxTTL:                    24 * time.Hour,
		TouchOnRead:               false,
		TouchTTL:                  10 * time.Minute,
	}
}

// LoadFromEnv reads configuration from HELLNET_CACHE_* environment variables,
// starting from DefaultOptions as the fallback for any unset value.
func LoadFromEnv() Options {
	o := DefaultOptions()
	o.L1Provider = env("HELLNET_CACHE_L1_PROVIDER", o.L1Provider)
	o.L1SizeLimitMB = envInt("HELLNET_CACHE_L1_SIZE_LIMIT_MB", o.L1SizeLimitMB)
	o.L1DefaultTTL = envDur("HELLNET_CACHE_L1_DEFAULT_TTL", o.L1DefaultTTL)
	o.L1ExpirationScanFrequency = envDur("HELLNET_CACHE_L1_EXPIRATION_SCAN_FREQUENCY", o.L1ExpirationScanFrequency)
	o.L1SlidingExpiration = envBool("HELLNET_CACHE_L1_SLIDING_EXPIRATION", o.L1SlidingExpiration)

	o.Connection = env("HELLNET_CACHE_CONNECTION", o.Connection)
	o.Password = env("HELLNET_CACHE_PASSWORD", o.Password)
	o.Database = envInt("HELLNET_CACHE_DATABASE", o.Database)
	o.KeyPrefix = env("HELLNET_CACHE_KEY_PREFIX", o.KeyPrefix)
	o.ConnectTimeout = envDur("HELLNET_CACHE_CONNECT_TIMEOUT", o.ConnectTimeout)
	o.ReadTimeout = envDur("HELLNET_CACHE_SYNC_TIMEOUT", o.ReadTimeout)
	o.RetryCount = envInt("HELLNET_CACHE_RETRY_COUNT", o.RetryCount)
	o.RetryBaseDelay = envDur("HELLNET_CACHE_RETRY_BASE_DELAY_MS", o.RetryBaseDelay)
	o.CircuitBreakerFailures = envInt("HELLNET_CACHE_CB_FAILURES", o.CircuitBreakerFailures)
	o.CircuitBreakerDuration = envDur("HELLNET_CACHE_CB_DURATION_SEC", o.CircuitBreakerDuration)

	o.DefaultSerializer = env("HELLNET_CACHE_DEFAULT_SERIALIZER", o.DefaultSerializer)

	o.EnableL1 = envBool("HELLNET_CACHE_ENABLE_L1", o.EnableL1)
	o.EnableL2 = envBool("HELLNET_CACHE_ENABLE_L2", o.EnableL2)
	o.DefaultTTL = envDur("HELLNET_CACHE_DEFAULT_TTL", o.DefaultTTL)
	o.MaxTTL = envDur("HELLNET_CACHE_MAX_TTL", o.MaxTTL)
	o.TouchOnRead = envBool("HELLNET_CACHE_TOUCH_ON_READ", o.TouchOnRead)
	o.TouchTTL = envDur("HELLNET_CACHE_TOUCH_TTL", o.TouchTTL)

	return o
}

// Validate checks that required fields are set when their feature is enabled.
func (o Options) Validate() error {
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

// Loading loads the .env file (dev or HELLNET_ENVIRONMENT undefined) and
// validates the required HELLNET_CACHE_* envs (fail-fast). In explicit
// Production/Staging it is a no-op — configuration is expected from the real
// environment. Mirrors the loading pattern of hellnet-lib-telemetry.
func Loading() {
	if !isDev() {
		return
	}
	loadEnvFiles()
	if err := LoadFromEnv().Validate(); err != nil {
		log.Fatalf("%v", err)
	}
}

// loadEnvFiles loads the .env file from disk (HELLNET_CACHE_ENV_FILE, then
// ./.env in the working directory, then ./.env alongside the executable). Used
// by Loading() (with validation) and New() (without fail-fast, so it can degrade
// to memory-only). No-op outside Development/Staging/Production default.
func loadEnvFiles() {
	if !isDev() {
		return
	}

	if custom := os.Getenv("HELLNET_CACHE_ENV_FILE"); custom != "" {
		_ = godotenv.Load(custom)
		return
	}

	// Prefer the .env alongside the executable, then the working directory and
	// its parents (so a repo-root .env is found even when running from a
	// subpackage).
	candidates := []string{".env"}
	if exe, err := os.Executable(); err == nil {
		candidates = append([]string{filepath.Join(filepath.Dir(exe), ".env")}, candidates...)
	}
	if wd, err := os.Getwd(); err == nil {
		for dir := wd; ; {
			candidates = append(candidates, filepath.Join(dir, ".env"))
			parent := filepath.Dir(dir)
			if parent == dir {
				break
			}
			dir = parent
		}
	}
	for _, c := range candidates {
		if _, err := os.Stat(c); err == nil {
			_ = godotenv.Load(c)
			return
		}
	}
}

// isDev reports whether HELLNET_ENVIRONMENT is Development or unset.
func isDev() bool {
	env := deploymentEnv()
	return env != "Production" && env != "Staging"
}

func deploymentEnv() string {
	if v := os.Getenv("HELLNET_ENVIRONMENT"); v != "" {
		return v
	}
	return "Development"
}

// formatKey applies the external backend key prefix.
func (o Options) formatKey(key string) string {
	return o.KeyPrefix + key
}

func (o Options) capTTL(ttl time.Duration) time.Duration {
	if ttl > o.MaxTTL {
		return o.MaxTTL
	}
	return ttl
}

func env(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return fallback
}

func envBool(key string, fallback bool) bool {
	v, ok := os.LookupEnv(key)
	if !ok {
		return fallback
	}
	if v == "true" || v == "1" || v == "yes" || v == "on" {
		return true
	}
	if v == "false" || v == "0" || v == "no" || v == "off" {
		return false
	}
	return fallback
}

func envInt(key string, fallback int) int {
	v, ok := os.LookupEnv(key)
	if !ok {
		return fallback
	}
	var n int
	if _, err := fmt.Sscanf(v, "%d", &n); err == nil {
		return n
	}
	return fallback
}

func envDur(key string, fallback time.Duration) time.Duration {
	v, ok := os.LookupEnv(key)
	if !ok {
		return fallback
	}
	if d, err := time.ParseDuration(v); err == nil {
		return d
	}
	if d, err := parseDotNetDuration(v); err == nil {
		return d
	}
	return fallback
}

// parseDotNetDuration parses "HH:MM:SS" or "HH:MM:SS.FFF" into a Duration.
func parseDotNetDuration(v string) (time.Duration, error) {
	var h, m, s int
	var frac float64
	n, err := fmt.Sscanf(v, "%d:%d:%d.%f", &h, &m, &s, &frac)
	if err != nil || n < 3 {
		n, err = fmt.Sscanf(v, "%d:%d:%d", &h, &m, &s)
		if err != nil || n < 3 {
			return 0, fmt.Errorf("invalid .NET duration: %q", v)
		}
	}
	return time.Duration(h)*time.Hour +
		time.Duration(m)*time.Minute +
		time.Duration(s)*time.Second +
		time.Duration(frac*float64(time.Second)), nil
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
// Concurrency guarantees:
//   - GetOrSet: stampede-protected via per-key semaphore (auto-cleaned)
//   - read-through warming: deduplicated (only one warming task per key)
//   - touch-on-read: optionally extends TTL on hit in upper layers
//   - all layer writes are parallel (goroutines + WaitGroup)
type HybridCache struct {
	opts       Options
	serializer Serializer
	providers  []Provider
	logger     *log.Logger

	locks   sync.Map // key string -> *semaphoreHolder
	warming sync.Map // key string -> struct{}
}

type semaphoreHolder struct {
	sem chan struct{} // capacity 1
}

// New builds a HybridCache, wiring up the enabled providers.
//
// Without arguments it is env-first: loads HELLNET_CACHE_* via LoadFromEnv().
// If L2 is enabled but no HELLNET_CACHE_CONNECTION is present, the library
// automatically falls back to memory-only (L2 disabled) instead of erroring.
func New(opts ...Options) (*HybridCache, error) {
	// Load env (embedded .env + ./.env from disk) so callers only need New().
	loadEnvFiles()

	o := LoadFromEnv()
	if len(opts) > 0 {
		o = opts[0]
	}

	if o.EnableL2 && o.Connection == "" {
		log.Printf("[hellnet-cache] HELLNET_CACHE_CONNECTION not set — falling back to memory-only (L2 disabled)")
		o.EnableL2 = false
	}

	if err := o.Validate(); err != nil {
		return nil, err
	}

	var providers []Provider

	if o.EnableL1 {
		mp, err := NewMemoryProvider(o)
		if err != nil {
			return nil, err
		}
		providers = append(providers, mp)
	}

	if o.EnableL2 {
		providers = append(providers, NewExternalProvider(o))
	}

	return &HybridCache{
		opts:       o,
		serializer: NewJSONSerializer(),
		providers:  providers,
		logger:     log.Default(),
	}, nil
}

// MustNew is like New but panics on error. Use at startup.
func MustNew(opts ...Options) *HybridCache {
	c, err := New(opts...)
	if err != nil {
		panic(err)
	}
	return c
}

// Get retrieves a value by key. Returns the zero value if not found in any layer.
func (c *HybridCache) Get(ctx context.Context, key string, out any) error {
	data, foundAtIndex := c.getRaw(ctx, key)
	if foundAtIndex < 0 || data == nil {
		return nil // not found; out stays zero value
	}
	return c.serializer.Deserialize(data, out)
}

// getRaw walks providers L1->L2 returning the first hit and its index,
// triggering warming/touch side-effects.
func (c *HybridCache) getRaw(ctx context.Context, key string) (data []byte, foundAtIndex int) {
	for i, p := range c.providers {
		v, err := p.Get(ctx, key)
		if err != nil {
			continue
		}
		if v != nil {
			if c.opts.TouchOnRead && i > 0 {
				c.touch(key, v)
			}
			if i > 0 {
				c.warm(key, v, i)
			}
			return v, i
		}
	}
	return nil, -1
}

// Set stores a value with optional TTL, written to all enabled layers.
func (c *HybridCache) Set(ctx context.Context, key string, value any, ttl *time.Duration) error {
	data, err := c.serializer.Serialize(value)
	if err != nil {
		return err
	}
	return c.SetBytes(ctx, key, data, ttl)
}

// SetBytes writes pre-serialized bytes to all enabled layers in parallel.
func (c *HybridCache) SetBytes(ctx context.Context, key string, data []byte, ttl *time.Duration) error {
	actual := c.opts.capTTL(derefTTL(ttl, c.opts.DefaultTTL))

	var wg sync.WaitGroup
	for _, p := range c.providers {
		wg.Add(1)
		go func(pr Provider) {
			defer wg.Done()
			if err := pr.Set(ctx, key, data, &actual); err != nil {
				c.logger.Printf("[hellnet-cache] set failed on %s for key %s: %v", pr.Name(), key, err)
			}
		}(p)
	}
	wg.Wait()
	return nil
}

// Remove deletes a key from all layers.
func (c *HybridCache) Remove(ctx context.Context, key string) error {
	var wg sync.WaitGroup
	for _, p := range c.providers {
		wg.Add(1)
		go func(pr Provider) {
			defer wg.Done()
			if err := pr.Remove(ctx, key); err != nil {
				c.logger.Printf("[hellnet-cache] remove failed on %s for key %s: %v", pr.Name(), key, err)
			}
		}(p)
	}
	wg.Wait()
	return nil
}

// Exists reports whether a key exists in any layer.
func (c *HybridCache) Exists(ctx context.Context, key string) (bool, error) {
	for _, p := range c.providers {
		ok, err := p.Exists(ctx, key)
		if err != nil {
			continue
		}
		if ok {
			return true, nil
		}
	}
	return false, nil
}

// GetOrSet retrieves a value or, if missing, runs the factory and caches the
// result. Stampede-protected per key.
func (c *HybridCache) GetOrSet(ctx context.Context, key string, out any, factory func(context.Context) (any, error), ttl *time.Duration) error {
	// fast path
	if _, found := c.getRaw(ctx, key); found >= 0 {
		return c.Get(ctx, key, out)
	}

	// stampede protection
	h := c.semaphoreFor(key)
	h.sem <- struct{}{} // acquire
	defer func() {
		<-h.sem // release
		c.releaseSemaphore(key, h)
	}()

	// double-check after acquiring lock
	if _, found := c.getRaw(ctx, key); found >= 0 {
		return c.Get(ctx, key, out)
	}

	value, err := factory(ctx)
	if err != nil {
		return err
	}
	// Serialize once; SetBytes writes it and we populate out from the same bytes.
	data, err := c.serializer.Serialize(value)
	if err != nil {
		return err
	}
	if err := c.SetBytes(ctx, key, data, ttl); err != nil {
		return err
	}
	return c.serializer.Deserialize(data, out)
}

// Close releases all providers.
func (c *HybridCache) Close() error {
	var firstErr error
	for _, p := range c.providers {
		if err := p.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (c *HybridCache) semaphoreFor(key string) *semaphoreHolder {
	v, _ := c.locks.LoadOrStore(key, &semaphoreHolder{sem: make(chan struct{}, 1)})
	return v.(*semaphoreHolder)
}

func (c *HybridCache) releaseSemaphore(key string, h *semaphoreHolder) {
	c.locks.LoadAndDelete(key)
}

// warm populates lower layers (deduplicated fire-and-forget).
func (c *HybridCache) warm(key string, data []byte, foundAtIndex int) {
	if _, loaded := c.warming.LoadOrStore(key, struct{}{}); loaded {
		return // already warming
	}
	go func() {
		defer c.warming.Delete(key)
		ttl := c.opts.L1DefaultTTL
		for i := 0; i < foundAtIndex; i++ {
			if err := c.providers[i].Set(context.Background(), key, data, &ttl); err != nil {
				c.logger.Printf("[hellnet-cache] warming failed for key %s: %v", key, err)
			}
		}
	}()
}

// touch extends TTL on hit (fire-and-forget, non-critical).
func (c *HybridCache) touch(key string, data []byte) {
	ttl := c.opts.TouchTTL
	for _, p := range c.providers {
		go func(pr Provider) {
			_ = pr.Set(context.Background(), key, data, &ttl)
		}(p)
	}
}

// derefTTL returns ttl when non-nil, else fallback.
func derefTTL(ttl *time.Duration, fallback time.Duration) time.Duration {
	if ttl != nil {
		return *ttl
	}
	return fallback
}
