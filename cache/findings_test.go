package cache

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Helpers (in-package fakes for hermetic failure injection)
// ---------------------------------------------------------------------------

// stubProvider is a fake Provider recording Set traffic with injectable
// behavior — used to simulate L1/L2 failure modes without external deps.
type stubProvider struct {
	name  string
	onGet func(key string) ([]byte, error)
	onSet func(key string, value []byte, ttl time.Duration) error

	setCalls atomic.Int64
	ttlSeen  atomic.Int64       // last TTL passed to Set (int64 storage)
	setTTLs  chan time.Duration // optional signal channel, drained by tests
}

func newStub(name string) *stubProvider {
	return &stubProvider{name: name}
}

func (s *stubProvider) Name() string { return s.name }

func (s *stubProvider) Get(key string) ([]byte, error) {
	if s.onGet != nil {
		return s.onGet(key)
	}
	return nil, nil // miss
}

func (s *stubProvider) Set(key string, value []byte, ttl time.Duration) error {
	s.setCalls.Add(1)
	s.ttlSeen.Store(int64(ttl))
	if s.setTTLs != nil {
		select {
		case s.setTTLs <- ttl:
		default: // never block the caller on undrained signals
		}
	}
	if s.onSet != nil {
		return s.onSet(key, value, ttl)
	}
	return nil
}

func (s *stubProvider) Remove(string) error         { return nil }
func (s *stubProvider) Exists(string) (bool, error) { return false, nil }
func (s *stubProvider) HealthCheck() bool           { return true }
func (s *stubProvider) Close() error                { return nil }

// newIsolatedHybrid builds a HybridCache with NO auto-wired providers
// (avoids ristretto/env machinery) and installs the given provider stack.
// It preserves the real baseCtx/serializer/logger wiring from New.
func newIsolatedHybrid(t *testing.T, opts Options, providers ...Provider) *HybridCache {
	t.Helper()
	o := opts
	o.EnableL1 = false
	o.EnableL2 = false
	c, err := newWithOptions(context.Background(), o)
	if err != nil {
		t.Fatalf("new isolated hybrid: %v", err)
	}
	c.providers = providers
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// closedPort returns a loopback address that is guaranteed to have nothing
// listening right now (bind then release).
func closedPort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

// ---------------------------------------------------------------------------
// C2 — total write failure must surface to the caller
// ---------------------------------------------------------------------------

// When EVERY layer fails to persist, Set/SetBytes must return an error
// instead of silently reporting success.
func TestSetBytes_AllProvidersFailReturnsError(t *testing.T) {
	failing := func(name string) *stubProvider {
		p := newStub(name)
		p.onSet = func(string, []byte, time.Duration) error {
			return errors.New("backend down")
		}
		return p
	}

	cases := []struct {
		name      string
		providers []Provider
	}{
		{"single-l1-fail", []Provider{failing("L1-Memory")}},
		{"l1-and-l2-fail", []Provider{failing("L1-Memory"), failing("L2-External")}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := newIsolatedHybrid(t, optsL1Only(), tc.providers...)

			err := c.SetBytes("k", []byte("v"), 0)
			if err == nil {
				t.Fatal("SetBytes: expected error when ALL providers fail to persist")
			}
			for _, p := range tc.providers {
				if !strings.Contains(err.Error(), p.Name()) {
					t.Errorf("aggregated error should name failing layer %q: %v", p.Name(), err)
				}
			}

			// Set shares the same contract through the serialization path.
			if serr := c.Set("k", map[string]string{"a": "b"}, 0); serr == nil {
				t.Fatal("Set: expected error when ALL providers fail to persist")
			}
		})
	}
}

// Degraded-but-successful semantics: if L1 persisted but L2 failed, SetBytes
// returns nil (repopulation happens via read-through warming) while still
// attempting the failed layer.
func TestSetBytes_L1SuccessL2FailureStillSucceeds(t *testing.T) {
	mp, err := NewMemoryProvider(optsL1Only())
	if err != nil {
		t.Fatalf("new memory provider: %v", err)
	}

	l2 := newStub("L2-External")
	l2.onSet = func(string, []byte, time.Duration) error {
		return errors.New("connection refused")
	}

	c := newIsolatedHybrid(t, optsL1Only(), mp, l2)

	if err := c.SetBytes("k", []byte(`"v"`), time.Minute); err != nil {
		t.Fatalf("expected nil error when at least one layer persists, got: %v", err)
	}

	// Value must actually be readable from the surviving layer.
	v, err := mp.Get("k")
	if err != nil || v == nil {
		t.Fatalf("L1 should hold the value: v=%q err=%v", v, err)
	}
	if string(v) != `"v"` {
		t.Fatalf("L1 got %q want %q", v, `"v"`)
	}
	// The failed layer must still have been attempted exactly once.
	if n := l2.setCalls.Load(); n != 1 {
		t.Fatalf("L2 attempted %d times, want 1", n)
	}
}

// The ExternalProvider itself must report its real backend/breaker error
// (previously it recorded metrics and returned nil unconditionally).
func TestExternalProvider_SetReturnsBackendError(t *testing.T) {
	addr := closedPort(t)
	opts := testDefaultOptions()
	opts.EnableL1 = true
	opts.EnableL2 = true
	opts.Connection = addr

	p := NewExternalProvider(context.Background(), opts)
	defer func() { _ = p.Close() }()

	err := p.Set("k", []byte("v"), time.Minute)
	if err == nil {
		t.Fatalf("expected dial error against dead endpoint %s", addr)
	}
	// Metric recording remains unchanged: every attempt counts.
	if got := p.Metrics().Sets(); got != 1 {
		t.Fatalf("RecordSet called %d times, want 1 (metrics unchanged)", got)
	}
}

// ---------------------------------------------------------------------------
// C3 — stampede protection must guarantee ONE factory execution per key
// ---------------------------------------------------------------------------

// Replaces the old semaphore-based stampede guarantee (racy holder deletion)
// with a hard invariant: under 200-goroutine contention, across repeated
// iterations and timing variants, the factory executes EXACTLY once per key
// and every waiting caller receives the computed result.
func TestGetOrSet_ConcurrentSingleFactory(t *testing.T) {
	const callers = 200
	const iterations = 50

	variants := []struct {
		name         string
		staggerStart bool // vary goroutine start order to shake interleavings
		factoryDelay time.Duration
	}{
		{name: "no-delay"},
		{name: "slow-factory", factoryDelay: 10 * time.Millisecond},
		{name: "staggered-starts", staggerStart: true},
	}

	for _, variant := range variants {
		t.Run(variant.name, func(t *testing.T) {
			c := mustNewWithOptions(context.Background(), optsL1Only())
			defer func() { _ = c.Close() }()

			for iter := 0; iter < iterations; iter++ {
				key := fmt.Sprintf("sf:%s:%d", variant.name, iter)

				var exec atomic.Int64
				factory := func(context.Context) (any, error) {
					exec.Add(1)
					if variant.factoryDelay > 0 {
						time.Sleep(variant.factoryDelay)
					}
					return "computed", nil
				}

				results := make([]string, callers)
				errs := make([]error, callers)
				var wg sync.WaitGroup
				for i := 0; i < callers; i++ {
					wg.Add(1)
					go func(i int) {
						defer wg.Done()
						if variant.staggerStart {
							time.Sleep(time.Duration(i%5) * time.Millisecond)
						}
						errs[i] = c.GetOrSet(key, &results[i], factory, 0)
					}(i)
				}
				wg.Wait()

				if n := exec.Load(); n != 1 {
					t.Fatalf("iter %d: factory executed %d times, want exactly 1", iter, n)
				}
				for i := range errs {
					if errs[i] != nil {
						t.Fatalf("iter %d: caller %d got error: %v", iter, i, errs[i])
					}
					if results[i] != "computed" {
						t.Fatalf("iter %d: caller %d got %q, want computed result", iter, i, results[i])
					}
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// I11 — Healthy aggregates provider health
// ---------------------------------------------------------------------------

func TestHealthy_AllHealthyReturnsNil(t *testing.T) {
	c := mustNewWithOptions(context.Background(), optsL1Only())
	defer func() { _ = c.Close() }()

	if err := c.Healthy(); err != nil {
		t.Fatalf("memory-only cache should be healthy, got: %v", err)
	}
}

func TestHealthy_UnhealthyL2AggregatedAsError(t *testing.T) {
	addr := closedPort(t)
	o := testDefaultOptions()
	o.EnableL1 = true
	o.EnableL2 = true
	o.Connection = addr

	c := mustNewWithOptions(context.Background(), o)
	defer func() { _ = c.Close() }()

	err := c.Healthy()
	if err == nil {
		t.Fatal("expected aggregated error when L2 points at a closed port")
	}
	if !strings.Contains(err.Error(), "L2-External") {
		t.Fatalf("error should name the unhealthy layer, got: %v", err)
	}
	if strings.Contains(err.Error(), "L1-Memory") {
		t.Fatalf("healthy L1 must not appear in aggregated error: %v", err)
	}
}

// ---------------------------------------------------------------------------
// I12 — resolveTTL: single resolution point for every TTL entering providers
// ---------------------------------------------------------------------------

func TestOptions_ResolveTTL(t *testing.T) {
	def := testDefaultOptions()
	cases := []struct {
		name string
		max  time.Duration
		in   time.Duration
		want time.Duration
	}{
		{"zero-falls-back-to-default", 24 * time.Hour, 0, def.DefaultTTL},
		{"negative-falls-back-to-default", 24 * time.Hour, -time.Second, def.DefaultTTL},
		{"positive-passthrough", 24 * time.Hour, time.Minute, time.Minute},
		{"clamped-to-max", 24 * time.Hour, 72 * time.Hour, 24 * time.Hour},
		{"exact-max-not-clamped", 24 * time.Hour, 24 * time.Hour, 24 * time.Hour},
		{"user-ttl-clamped-by-smaller-max", time.Hour, 8 * time.Hour, time.Hour},
		{"no-max-configured-no-clamp", 0, 100 * time.Hour, 100 * time.Hour},
		{"negative-max-no-instant-expiry", -time.Second, 5 * time.Minute, 5 * time.Minute},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			o := def
			o.MaxTTL = tc.max
			if got := o.resolveTTL(tc.in); got != tc.want {
				t.Fatalf("resolveTTL(%v) with MaxTTL=%v = %v, want %v", tc.in, tc.max, got, tc.want)
			}
		})
	}
}

// Regression: warm() previously wrote entries using the RAW L1DefaultTTL,
// bypassing the MaxTTL clamp entirely. Warm writes must route through
// resolveTTL like every other TTL path.
func TestWarm_ClampsToMaxTTL(t *testing.T) {
	supplier, err := NewMemoryProvider(optsL1Only())
	if err != nil {
		t.Fatalf("new memory provider: %v", err)
	}
	defer func() { _ = supplier.Close() }()
	if err := supplier.Set("warm:k", []byte("payload"), time.Hour); err != nil {
		t.Fatalf("seed supplier: %v", err)
	}

	recorder := newStub("warm-target")
	recorder.setTTLs = make(chan time.Duration, 4)

	o := testDefaultOptions()
	o.L1DefaultTTL = 10_000 * time.Hour // absurd: must be clamped by MaxTTL below
	o.MaxTTL = 2 * time.Minute

	c := newIsolatedHybrid(t, o, recorder, supplier)

	// Reading from index 1 (supplier) triggers warm of layer 0 (recorder).
	data, idx := c.getRaw("warm:k")
	if idx < 0 || data == nil {
		t.Fatalf("supplier should serve the hit (idx=%d)", idx)
	}

	select {
	case got := <-recorder.setTTLs:
		if got != o.MaxTTL {
			t.Fatalf("warm wrote TTL %v, want clamped MaxTTL %v", got, o.MaxTTL)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("warm did not reach the lower layer within deadline")
	}
}
