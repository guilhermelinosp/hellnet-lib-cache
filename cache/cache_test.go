package cache

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/guilhermelinosp/hellnet-lib-environments/environments"
)

// optsL1Only returns options with only L1 enabled (no external deps).
func optsL1Only() Options {
	o := DefaultOptions()
	o.EnableL1 = true
	o.EnableL2 = false
	return o
}

func TestMemoryProvider_SetGet(t *testing.T) {
	p, err := NewMemoryProvider(optsL1Only())
	if err != nil {
		t.Fatalf("new memory provider: %v", err)
	}
	defer p.Close()

	if err := p.Set("k", []byte("v"), 0); err != nil {
		t.Fatalf("set: %v", err)
	}
	v, err := p.Get("k")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(v) != "v" {
		t.Fatalf("got %q want v", v)
	}

	if ok, _ := p.Exists("k"); !ok {
		t.Fatal("expected key to exist")
	}
	if err := p.Remove("k"); err != nil {
		t.Fatalf("remove: %v", err)
	}
	v, _ = p.Get("k")
	if v != nil {
		t.Fatalf("expected miss after remove, got %q", v)
	}
}

func TestMemoryProvider_TTLExpiry(t *testing.T) {
	p, _ := NewMemoryProvider(optsL1Only())
	defer p.Close()
	ttl := 50 * time.Millisecond
	if err := p.Set("exp", []byte("x"), ttl); err != nil {
		t.Fatal(err)
	}
	time.Sleep(120 * time.Millisecond)
	v, _ := p.Get("exp")
	if v != nil {
		t.Fatalf("expected expiry, got %q", v)
	}
}

func TestHybrid_WriteThroughReadThrough(t *testing.T) {
	c, err := New(context.Background(), optsL1Only())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	type Order struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}
	want := Order{ID: "1", Name: "test"}
	if err := c.Set("order:1", want, 0); err != nil {
		t.Fatal(err)
	}
	var got Order
	if err := c.Get("order:1", &got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("got %+v want %+v", got, want)
	}

	if ok, _ := c.Exists("order:1"); !ok {
		t.Fatal("expected exists")
	}
	if err := c.Remove("order:1"); err != nil {
		t.Fatal(err)
	}
	if ok, _ := c.Exists("order:1"); ok {
		t.Fatal("expected not exists after remove")
	}
}

func TestHybrid_GetOrSet(t *testing.T) {
	c, _ := New(context.Background(), optsL1Only())
	defer c.Close()

	calls := 0
	factory := func(context.Context) (any, error) {
		calls++
		return "computed", nil
	}

	var out string
	if err := c.GetOrSet("gs", &out, factory, 0); err != nil {
		t.Fatal(err)
	}
	if out != "computed" {
		t.Fatalf("got %q", out)
	}
	if calls != 1 {
		t.Fatalf("factory called %d times, want 1", calls)
	}

	// second call should hit cache, not call factory
	if err := c.GetOrSet("gs", &out, factory, 0); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("factory called %d times after cache hit, want 1", calls)
	}
}

func TestHybrid_Stampede(t *testing.T) {
	c, _ := New(context.Background(), optsL1Only())
	defer c.Close()

	var mu sync.Mutex
	calls := 0
	factory := func(context.Context) (any, error) {
		mu.Lock()
		calls++
		mu.Unlock()
		time.Sleep(20 * time.Millisecond) // simulate work
		return "v", nil
	}

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var out string
			_ = c.GetOrSet("stampede", &out, factory, 0)
		}()
	}
	wg.Wait()

	if calls != 1 {
		t.Fatalf("stampede: factory called %d times, want 1", calls)
	}
}

func TestOptions_EnvBinding(t *testing.T) {
	t.Setenv("HELLNET_CACHE_CONNECTION", "cache.example:6379")
	t.Setenv("HELLNET_CACHE_PASSWORD", "secret")
	t.Setenv("HELLNET_CACHE_DEFAULT_TTL", "1h")
	t.Setenv("HELLNET_CACHE_ENABLE_L2", "true")

	o := LoadFromEnv()
	if o.Connection != "cache.example:6379" {
		t.Fatalf("connection not bound: %q", o.Connection)
	}
	if o.Password != "secret" {
		t.Fatalf("password not bound")
	}
	if o.DefaultTTL != time.Hour {
		t.Fatalf("ttl not bound: %v", o.DefaultTTL)
	}
	if err := o.Validate(); err != nil {
		t.Fatalf("validate should pass: %v", err)
	}
}

func TestOptions_RequiredEnvMissing(t *testing.T) {
	// Ensure no connection leaks in (password is optional, only connection matters).
	t.Setenv("HELLNET_CACHE_CONNECTION", "")
	t.Setenv("HELLNET_CACHE_PASSWORD", "")
	o := DefaultOptions()
	o.EnableL2 = true
	if err := o.Validate(); err == nil {
		t.Fatal("expected validation error when L2 enabled without connection")
	}
}

func TestOptions_PasswordOptional(t *testing.T) {
	// connection present, password absent -> valid (no-auth backend)
	t.Setenv("HELLNET_CACHE_CONNECTION", "localhost:6379")
	t.Setenv("HELLNET_CACHE_PASSWORD", "")
	o := LoadFromEnv()
	o.EnableL2 = true
	if err := o.Validate(); err != nil {
		t.Fatalf("password is optional; validate should pass: %v", err)
	}
}

func TestOptions_ClockDuration(t *testing.T) {
	d, err := environments.ParseDuration("00:05:00")
	if err != nil {
		t.Fatal(err)
	}
	if d != 5*time.Minute {
		t.Fatalf("got %v want 5m", d)
	}
	d, err = environments.ParseDuration("24:00:00")
	if err != nil {
		t.Fatal(err)
	}
	if d != 24*time.Hour {
		t.Fatalf("got %v want 24h", d)
	}
}

// optsL2Real returns options pointing at a real external backend (Redis/Valkey)
// read from the environment. If HELLNET_CACHE_CONNECTION is unset, the test is
// skipped — this keeps the unit suite hermetic while enabling real integration
// runs when a backend is available.
func optsL2Real(t *testing.T) Options {
	t.Helper()
	// Load .env if present (cwd or alongside executable) so integration tests
	// honor a repo-level .env, not just process environment variables.
	loadEnvFiles()
	conn := os.Getenv("HELLNET_CACHE_CONNECTION")
	if conn == "" {
		t.Skip("HELLNET_CACHE_CONNECTION not set; skipping L2 integration test")
	}
	o := DefaultOptions()
	o.EnableL1 = true
	o.EnableL2 = true
	o.Connection = conn
	o.Password = os.Getenv("HELLNET_CACHE_PASSWORD") // optional
	if err := o.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	return o
}

func TestIntegration_L2SetGet(t *testing.T) {
	opts := optsL2Real(t)
	c, err := New(context.Background(), opts)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer c.Close()

	key := "it:order:1"
	defer func() { _ = c.Remove(key) }()

	type Order struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}
	want := Order{ID: "1", Name: "widget"}

	if err := c.Set(key, want, 30*time.Second); err != nil {
		t.Fatalf("set: %v", err)
	}

	// L1 should have it immediately; L2 in parallel too.
	var got Order
	if err := c.Get(key, &got); err != nil {
		t.Fatalf("get: %v", err)
	}
	if got != want {
		t.Fatalf("got %+v want %+v", got, want)
	}

	exists, err := c.Exists(key)
	if err != nil || !exists {
		t.Fatalf("exists=%v err=%v", exists, err)
	}
}

func TestIntegration_L2PersistenceAfterL1Eviction(t *testing.T) {
	opts := optsL2Real(t)
	// Disable L1 to force reads through L2 only.
	opts.EnableL1 = false
	c, err := New(context.Background(), opts)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer c.Close()

	key := "it:l2only:cfg"
	defer func() { _ = c.Remove(key) }()

	// C2 semantics: with L1 disabled, a dead L2 is a TOTAL write failure and
	// Set correctly surfaces the error. This scenario requires a reachable
	// backend, so treat an unwritable layer as environmental (skip).
	if err := c.Set(key, "v2", time.Minute); err != nil {
		t.Skipf("set failed — scenario requires a reachable L2 backend: %v", err)
	}
	var out string
	if err := c.Get(key, &out); err != nil {
		t.Fatalf("get: %v", err)
	}
	if out != "v2" {
		t.Fatalf("got %q want v2", out)
	}
}

func TestIntegration_L2GetOrSet(t *testing.T) {
	opts := optsL2Real(t)
	c, err := New(context.Background(), opts)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer c.Close()

	key := "it:gs:price"
	defer func() { _ = c.Remove(key) }()

	calls := 0
	factory := func(context.Context) (any, error) {
		calls++
		return 42, nil
	}

	var out int
	if err := c.GetOrSet(key, &out, factory, time.Minute); err != nil {
		t.Fatalf("getOrSet: %v", err)
	}
	if out != 42 || calls != 1 {
		t.Fatalf("out=%v calls=%d", out, calls)
	}

	// Second call should hit cache (L1 or L2), not invoke factory.
	if err := c.GetOrSet(key, &out, factory, time.Minute); err != nil {
		t.Fatalf("getOrSet 2: %v", err)
	}
	if calls != 1 {
		t.Fatalf("factory called %d times after cache hit, want 1", calls)
	}
}

func TestIntegration_L2Remove(t *testing.T) {
	opts := optsL2Real(t)
	c, err := New(context.Background(), opts)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer c.Close()

	key := "it:rm:key"
	if err := c.Set(key, "x", time.Minute); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := c.Remove(key); err != nil {
		t.Fatalf("remove: %v", err)
	}
	exists, _ := c.Exists(key)
	if exists {
		t.Fatal("expected not exists after remove")
	}
}
func TestDeploymentEnv(t *testing.T) {
	cases := []struct {
		env  string
		want string
	}{
		{"", "Development"},
		{"Development", "Development"},
		{"Staging", "Staging"},
		{"Production", "Production"},
	}
	for _, c := range cases {
		t.Run(c.want, func(t *testing.T) {
			if c.env == "" {
				t.Setenv("HELLNET_ENVIRONMENT", "")
			} else {
				t.Setenv("HELLNET_ENVIRONMENT", c.env)
			}
			if got := deploymentEnv(); got != c.want {
				t.Fatalf("deploymentEnv()=%q want %q", got, c.want)
			}
		})
	}
}

func TestLoading_ProductionSkipsDotEnv(t *testing.T) {
	t.Setenv("HELLNET_ENVIRONMENT", "Production")
	t.Setenv("HELLNET_CACHE_CONNECTION", "")
	t.Setenv("HELLNET_CACHE_ENV_FILE", "")
	// Must not panic/exit without a connection (skips .env, no validation).
	Loading()
}

func TestLoading_DevLoadsEnvFileOverride(t *testing.T) {
	t.Setenv("HELLNET_ENVIRONMENT", "Development")
	t.Setenv("HELLNET_CACHE_ENV_FILE", "custom.env")
	// Leave HELLNET_CACHE_CONNECTION unset so godotenv can provide it, then
	// restore the original value so later tests (e.g. integration) are unaffected.
	prev, had := os.LookupEnv("HELLNET_CACHE_CONNECTION")
	os.Unsetenv("HELLNET_CACHE_CONNECTION")
	t.Cleanup(func() {
		if had {
			_ = os.Setenv("HELLNET_CACHE_CONNECTION", prev)
		} else {
			os.Unsetenv("HELLNET_CACHE_CONNECTION")
		}
	})
	os.WriteFile("custom.env", []byte("HELLNET_CACHE_CONNECTION=fromcustom:6379\n"), 0o600)
	defer os.Remove("custom.env")
	Loading()
	if os.Getenv("HELLNET_CACHE_CONNECTION") != "fromcustom:6379" {
		t.Fatalf("Loading() did not load HELLNET_CACHE_ENV_FILE: CONNECTION=%q", os.Getenv("HELLNET_CACHE_CONNECTION"))
	}
}

// New must degrade to memory-only automatically when L2 is enabled but no
// connection is configured — the decision lives in the library, not the caller.
func TestNew_DegradesToMemoryOnlyWithoutConnection(t *testing.T) {
	t.Setenv("HELLNET_CACHE_CONNECTION", "")
	t.Setenv("HELLNET_CACHE_ENV_FILE", "")
	t.Setenv("HELLNET_ENVIRONMENT", "Development")

	c, err := New(context.Background())
	if err != nil {
		t.Fatalf("New() should not error on missing connection (degrades): %v", err)
	}
	defer c.Close()

	if err := c.Set("k", "v", time.Second); err != nil {
		t.Fatalf("set: %v", err)
	}
	var out string
	if err := c.Get("k", &out); err != nil {
		t.Fatalf("get: %v", err)
	}
	if out != "v" {
		t.Fatalf("got %q want v", out)
	}
	// L2 must be disabled (memory-only).
	for _, p := range c.providers {
		if p.Name() == "L2-External" {
			t.Fatal("L2 should be disabled when no connection is configured")
		}
	}
}

// The OperationTimeout knob must be bindable from env (integer milliseconds)
// and default to 5s.
func TestOptions_OperationTimeoutEnvBinding(t *testing.T) {
	t.Setenv("HELLNET_CACHE_OPERATION_TIMEOUT_MS", "250")

	if got := DefaultOptions().OperationTimeout; got != 5*time.Second {
		t.Fatalf("default OperationTimeout = %v, want 5s", got)
	}
	o := LoadFromEnv()
	if o.OperationTimeout != 250*time.Millisecond {
		t.Fatalf("OperationTimeout not bound: %v, want 250ms", o.OperationTimeout)
	}
}

// A zero/negative OperationTimeout (misconfig or empty struct) must normalize
// to the library default instead of producing instantly-expired contexts.
func TestOpCtx_NormalizesZeroOperationTimeout(t *testing.T) {
	o := optsL1Only()
	o.OperationTimeout = 0 // simulate misconfiguration / unset field
	c := MustNew(context.Background(), o)
	defer func() { _ = c.Close() }()

	ctx, cancel := c.opCtx()
	defer cancel()

	dl, ok := ctx.Deadline()
	if !ok {
		t.Fatal("expected operation context to carry a deadline")
	}
	remaining := time.Until(dl)
	if remaining <= 0 || remaining > defaultOperationTimeout {
		t.Fatalf("op ctx deadline = %v from now, want within %v", remaining, defaultOperationTimeout)
	}
}

// End-to-end contract check: the context captured once at New propagates
// internally — cancelling the caller-side parent must abort an in-flight
// factory inside GetOrSet.
func TestNew_ParentCancelAbortsFactory(t *testing.T) {
	ctx, stop := context.WithCancel(context.Background())
	c := MustNew(ctx, optsL1Only())
	defer func() { _ = c.Close() }()

	started := make(chan struct{})
	factory := func(fctx context.Context) (any, error) {
		close(started)
		<-fctx.Done()
		return nil, fctx.Err()
	}

	errCh := make(chan error, 1)
	go func() {
		var out string
		errCh <- c.GetOrSet("cancel-me", &out, factory, 0)
	}()

	<-started
	stop()

	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("GetOrSet err = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("factory was not aborted by parent cancellation")
	}
}
