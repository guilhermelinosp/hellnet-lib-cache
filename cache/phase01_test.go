package cache

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	tempDir, err := os.MkdirTemp("", "hellnet-lib-cache-tests-")
	if err != nil {
		panic(err)
	}
	oldDir, err := os.Getwd()
	if err != nil || os.Chdir(tempDir) != nil {
		panic("cache tests: cannot isolate working directory")
	}
	goleak.VerifyTestMain(m)
	_ = os.Chdir(oldDir)
	_ = os.RemoveAll(tempDir)
}

func TestNew_ExplicitL2WithoutConnectionReturnsError(t *testing.T) {
	t.Setenv("HELLNET_CACHE_ENABLE_L2", "true")
	t.Setenv("HELLNET_CACHE_CONNECTION", "")
	t.Setenv("HELLNET_CACHE_LOAD_DOTENV", "false")

	if _, err := New(); err == nil {
		t.Fatal("New should reject explicitly enabled L2 without connection")
	}
}

func TestNew_DurationSuffixUsesDeclaredUnit(t *testing.T) {
	t.Setenv("HELLNET_CACHE_ENABLE_L2", "false")
	t.Setenv("HELLNET_CACHE_RETRY_BASE_DELAY_MS", "500")
	t.Setenv("HELLNET_CACHE_CB_DURATION_SEC", "2")
	t.Setenv("HELLNET_CACHE_LOAD_DOTENV", "false")

	c, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if got, want := c.opts.RetryBaseDelay, 500*time.Millisecond; got != want {
		t.Fatalf("RetryBaseDelay = %v, want %v", got, want)
	}
	if got, want := c.opts.CircuitBreakerDuration, 2*time.Second; got != want {
		t.Fatalf("CircuitBreakerDuration = %v, want %v", got, want)
	}
}

func TestNew_InvalidDefinedDurationReturnsError(t *testing.T) {
	t.Setenv("HELLNET_CACHE_ENABLE_L2", "false")
	t.Setenv("HELLNET_CACHE_RETRY_BASE_DELAY_MS", "not-a-duration")
	t.Setenv("HELLNET_CACHE_LOAD_DOTENV", "false")

	if _, err := New(); err == nil {
		t.Fatal("New should reject invalid defined duration")
	}
}

func TestHybridCache_InjectableClockControlsLocalRateLimit(t *testing.T) {
	c := mustNewWithOptions(context.Background(), optsL1Only())
	defer c.Close()

	now := time.Unix(100, 0)
	c.now = func() time.Time { return now }
	if allowed, _, _, err := c.Allow("clock", 1, time.Minute); err != nil || !allowed {
		t.Fatalf("first Allow = %v, err=%v", allowed, err)
	}
	now = now.Add(time.Minute)
	if allowed, _, _, err := c.Allow("clock", 1, time.Minute); err != nil || !allowed {
		t.Fatalf("Allow after simulated expiry = %v, err=%v", allowed, err)
	}
}

func TestExternalProvider_MiniredisSetGet(t *testing.T) {
	server := miniredis.RunT(t)
	o := testDefaultOptions()
	o.EnableL1 = false
	o.Connection = server.Addr()

	p := NewExternalProvider(context.Background(), o)
	t.Cleanup(func() { _ = p.Close() })
	if err := p.Set("fake", []byte("value"), time.Minute); err != nil {
		t.Fatal(err)
	}
	got, err := p.Get("fake")
	if err != nil || string(got) != "value" {
		t.Fatalf("Get = %q, err=%v", got, err)
	}
}

func TestNewWithOptions_InjectsProviderAndSerializer(t *testing.T) {
	provider := newStub("test")
	serializer := NewJSONSerializer()
	c, err := NewWithOptions(context.Background(),
		WithOptions(optsL1Only()),
		WithProviders(provider),
		WithSerializer(serializer),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if len(c.providers) != 1 || c.providers[0] != provider {
		t.Fatalf("providers = %#v, want injected provider", c.providers)
	}
}

func TestNewWithOptions_ExplicitOptionsIgnoreEnvironment(t *testing.T) {
	t.Setenv("HELLNET_CACHE_RETRY_BASE_DELAY_MS", "invalid")
	c, err := NewWithOptions(context.Background(), WithOptions(optsL1Only()))
	if err != nil {
		t.Fatalf("explicit options must not read environment: %v", err)
	}
	defer c.Close()
}
