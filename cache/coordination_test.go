package cache

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// mustHybridL1 builds a real L1-only HybridCache (ristretto stack wired) so
// coordination features exercise the genuine serialize/getRaw/SetBytes path.
func mustHybridL1(t *testing.T) *HybridCache {
	t.Helper()
	c := mustNewWithOptions(context.Background(), optsL1Only())
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// randomHex returns a short crypto-random tag for unique-per-run keys.
func randomHex(t *testing.T, byteLen int) string {
	t.Helper()
	buf := make([]byte, byteLen)
	if _, err := rand.Read(buf); err != nil {
		t.Fatalf("random suffix: %v", err)
	}
	return hex.EncodeToString(buf)
}

// overlapTracker records the maximum number of simultaneous critical-section
// holders observed across goroutines.
type overlapTracker struct {
	mu      sync.Mutex
	current int64
	peak    int64
}

func (o *overlapTracker) enter() {
	o.mu.Lock()
	defer o.mu.Unlock()
	atomic.AddInt64(&o.current, 1)
	if cur := atomic.LoadInt64(&o.current); cur > o.peak {
		o.peak = cur
	}
}

func (o *overlapTracker) leave() { atomic.AddInt64(&o.current, -1) }

// ---------------------------------------------------------------------------
// Feature 1 — Idempotency
// ---------------------------------------------------------------------------

func TestIdempotent_FirstExecutes_SecondReturnsStoredWithoutExecuting(t *testing.T) {
	c := mustHybridL1(t)

	var calls int
	fn := func() (any, error) {
		calls++
		return map[string]int{"amount": 42}, nil
	}

	res1, executed1, err := c.Idempotent("idem:first", time.Minute, fn)
	if err != nil {
		t.Fatalf("first call: %v", err)
	}
	if !executed1 {
		t.Fatal("first call must report executed=true")
	}
	if calls != 1 {
		t.Fatalf("fn called %d times, want 1", calls)
	}
	m1, ok := res1.(map[string]int)
	if !ok || m1["amount"] != 42 {
		t.Fatalf("first result = %#v, want amount=42", res1)
	}

	res2, executed2, err := c.Idempotent("idem:first", time.Minute, fn)
	if err != nil {
		t.Fatalf("second call: %v", err)
	}
	if executed2 {
		t.Fatal("repeat within TTL must report executed=false")
	}
	if calls != 1 {
		t.Fatalf("fn called %d times after repeat, want 1 (cached)", calls)
	}
	// Stored payload decodes through the generic JSON path: numbers become
	// float64, mirroring Get-into-any round-tripping.
	stored, ok := res2.(map[string]any)
	if !ok {
		t.Fatalf("stored result type = %T, want decoded map", res2)
	}
	if got := stored["amount"]; got != float64(42) {
		t.Fatalf("stored amount = %#v, want 42", got)
	}
}

func TestIdempotent_FailureIsNotCached_NextCallRetries(t *testing.T) {
	c := mustHybridL1(t)

	var calls int
	sentinel := errors.New("charge declined")
	fnFail := func() (any, error) {
		calls++
		return nil, sentinel
	}

	for attempt := 1; attempt <= 2; attempt++ {
		res, executed, err := c.Idempotent("idem:failing", time.Minute, fnFail)
		if !errors.Is(err, sentinel) {
			t.Fatalf("attempt %d: err = %v, want sentinel verbatim", attempt, err)
		}
		if res != nil {
			t.Fatalf("attempt %d: result = %#v, want nil on failure", attempt, res)
		}
		if !executed {
			t.Fatalf("attempt %d: failed execution must report executed=true", attempt)
		}
	}
	if calls != 2 {
		t.Fatalf("failed fn ran %d times, want 2 (no poisoning, retried)", calls)
	}

	// A later success finally caches: subsequent calls stop executing.
	fnOK := func() (any, error) {
		calls++
		return "done", nil
	}
	if _, _, err := c.Idempotent("idem:failing", time.Minute, fnOK); err != nil {
		t.Fatalf("success path: %v", err)
	}
	var out string
	gotVal, executed, err := c.Idempotent("idem:failing", time.Minute, fnOK)
	out, _ = gotVal.(string)
	if err != nil || executed || out != "done" || calls != 3 {
		t.Fatalf("cached repeat mismatch: val=%v executed=%v err=%v calls=%d", gotVal, executed, err, calls)
	}
}

func TestIdempotent_ConcurrentSameKey_ExecutesAtMostFewTimes(t *testing.T) {
	c := mustHybridL1(t)

	const callers = 100
	var executions atomic.Int64
	fn := func() (any, error) {
		executions.Add(1)
		time.Sleep(5 * time.Millisecond) // widen the race window
		return "shared-outcome", nil
	}

	results := make([]any, callers)
	executedFlags := make([]bool, callers)
	errs := make([]error, callers)
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i], executedFlags[i], errs[i] = c.Idempotent("idem:concurrent", time.Minute, fn)
		}(i)
	}
	wg.Wait()

	if n := executions.Load(); n == 0 || n > 20 {
		t.Fatalf("fn executed %d times, want small (1 expected, <=20 documented best-effort)", n)
	}
	for i := range callers {
		if errs[i] != nil {
			t.Fatalf("caller %d: %v", i, errs[i])
		}
		if v, ok := results[i].(string); !ok || v != "shared-outcome" {
			t.Fatalf("caller %d got %#v, want shared-outcome", i, results[i])
		}
		_ = executedFlags[i]
	}
}

func TestIdempotent_DifferentKeysAreIndependent(t *testing.T) {
	c := mustHybridL1(t)

	counterFor := func() func() (any, error) {
		var n int
		return func() (any, error) {
			n++
			return n, nil
		}
	}
	fa, fb := counterFor(), counterFor()

	a1, execA, err := c.Idempotent("idem:key-a", time.Minute, fa)
	if err != nil || !execA || a1 != 1 {
		t.Fatalf("key-a first = %v/%v/%v", a1, execA, err)
	}
	b1, execB, err := c.Idempotent("idem:key-b", time.Minute, fb)
	if err != nil || !execB || b1 != 1 {
		t.Fatalf("key-b first = %v/%v/%v", b1, execB, err)
	}
	// Same key twice advances nothing; other key untouched.
	if a2, executed, _ := c.Idempotent("idem:key-a", time.Minute, fa); executed || a2 != float64(1) {
		t.Fatalf("key-a repeat = %v executed=%v, want 1/false", a2, executed)
	}
	if b2, executed, _ := c.Idempotent("idem:key-b", time.Minute, fb); executed || b2 != float64(1) {
		t.Fatalf("key-b repeat = %v executed=%v, want 1/false", b2, executed)
	}
}

func TestIdempotent_TTLExpiryAllowsReexecution(t *testing.T) {
	c := mustHybridL1(t)

	var calls int
	fn := func() (any, error) {
		calls++
		return calls, nil
	}

	if _, executed, err := c.Idempotent("idem:ttl", 50*time.Millisecond, fn); err != nil || !executed {
		t.Fatalf("first = executed=%v err=%v", executed, err)
	}
	time.Sleep(150 * time.Millisecond) // let the record expire everywhere
	if _, executed, err := c.Idempotent("idem:ttl", 50*time.Millisecond, fn); err != nil || !executed {
		t.Fatalf("after expiry executed=%v err=%v, want re-execution", executed, err)
	}
	if calls != 2 {
		t.Fatalf("calls=%d, want 2 after expiry", calls)
	}
}

// ---------------------------------------------------------------------------
// Feature 2 — Rate limiting (memory fallback paths)
// ---------------------------------------------------------------------------

func TestAllow_MemoryAllowsUpToLimitThenBlocks(t *testing.T) {
	c := mustHybridL1(t)
	key := "rl:quota"

	const limit = 3
	for i := 0; i < limit; i++ {
		allowed, remaining, resetIn, err := c.Allow(key, limit, time.Second)
		if err != nil {
			t.Fatalf("call %d: %v", i+1, err)
		}
		if !allowed {
			t.Fatalf("call %d unexpectedly blocked", i+1)
		}
		if remaining != int64(limit-i-1) {
			t.Fatalf("call %d remaining=%d, want %d", i+1, remaining, limit-i-1)
		}
		if resetIn <= 0 || resetIn > time.Second {
			t.Fatalf("call %d resetIn=%v, want within (0,window]", i+1, resetIn)
		}
	}

	allowed, remaining, resetIn, err := c.Allow(key, limit, time.Second)
	if err != nil || allowed {
		t.Fatalf("over-limit call: allowed=%v err=%v, want blocked", allowed, err)
	}
	if remaining != 0 {
		t.Fatalf("over-limit remaining=%d, want 0", remaining)
	}
	if resetIn <= 0 {
		t.Fatalf("over-limit resetIn=%v, want positive time-to-reset", resetIn)
	}
}

func TestAllow_MemoryWindowResets(t *testing.T) {
	c := mustHybridL1(t)
	key := "rl:reset"

	if allowed, _, _, err := c.Allow(key, 1, 30*time.Millisecond); err != nil || !allowed {
		t.Fatalf("first: allowed=%v err=%v", allowed, err)
	}
	if allowed, _, _, _ := c.Allow(key, 1, 30*time.Millisecond); allowed {
		t.Fatal("second within window must be blocked")
	}
	time.Sleep(70 * time.Millisecond) // window elapsed
	allowed, remaining, _, err := c.Allow(key, 1, 30*time.Millisecond)
	if err != nil || !allowed || remaining != 0 {
		t.Fatalf("post-window: allowed=%v remaining=%d err=%v, want fresh allowance", allowed, remaining, err)
	}
}

func TestAllow_MemoryKeysIndependent(t *testing.T) {
	c := mustHybridL1(t)

	if _, _, _, err := c.Allow("rl:k1", 1, time.Second); err != nil {
		t.Fatal(err)
	}
	if allowed, _, _, _ := c.Allow("rl:k1", 1, time.Second); allowed {
		t.Fatal("k1 exhausted yet allowed again")
	}
	if allowed, remaining, _, err := c.Allow("rl:k2", 1, time.Second); err != nil || !allowed || remaining != 0 {
		t.Fatalf("independent k2 compromised: allowed=%v remaining=%d err=%v", allowed, remaining, err)
	}
}

func TestAllow_InputValidationErrors(t *testing.T) {
	c := mustHybridL1(t)

	if _, _, _, err := c.Allow("", 5, time.Second); err == nil || !strings.HasPrefix(err.Error(), "cache:") {
		t.Fatalf("empty key: err=%v, want 'cache:' prefixed", err)
	}
	if _, _, _, err := c.Allow("rl:x", 0, time.Second); err == nil {
		t.Fatal("limit=0 must error")
	}
	if _, _, _, err := c.Allow("rl:x", -1, time.Second); err == nil {
		t.Fatal("negative limit must error")
	}
	if _, _, _, err := c.Allow("rl:x", 5, 0); err == nil {
		t.Fatal("window=0 must error")
	}
}

// Degrade-to-local counting: when the Scripter-capable layer errors, Allow
// keeps enforcing SOME budget instead of failing open silently.
func TestAllow_BackendErrorDegradesToLocalCounting(t *testing.T) {
	mp, err := NewMemoryProvider(optsL1Only())
	if err != nil {
		t.Fatalf("new memory provider: %v", err)
	}
	dead := newDeadExternalProvider(t)

	c := newIsolatedHybrid(t, optsL1Only(), mp, dead)

	const limit = 2
	for i := 0; i < limit; i++ {
		if allowed, _, _, err := c.Allow("rl:degrade", limit, time.Second); err != nil || !allowed {
			t.Fatalf("degraded call %d: allowed=%v err=%v", i+1, allowed, err)
		}
	}
	if allowed, _, _, _ := c.Allow("rl:degrade", limit, time.Second); allowed {
		t.Fatal("degraded limiter lost the local budget")
	}
}

// ---------------------------------------------------------------------------
// Feature 3 — Locks (memory fallback paths)
// ---------------------------------------------------------------------------

func TestLock_AcquireUnlockCycle_DoubleUnlockErrors(t *testing.T) {
	c := mustHybridL1(t)

	unlock, ok, err := c.Lock("lk:cycle", time.Second, 0)
	if err != nil || !ok {
		t.Fatalf("acquire: ok=%v err=%v", ok, err)
	}
	if unlock == nil {
		t.Fatal("successful acquire must return a non-nil unlock")
	}
	if err := unlock(); err != nil {
		t.Fatalf("first unlock: %v", err)
	}
	err = unlock()
	if err == nil || !errors.Is(err, ErrLockNotHeld) || !strings.Contains(err.Error(), "already released") {
		t.Fatalf("second unlock err=%v, want wrapped ErrLockNotHeld mentioning release", err)
	}
}

func TestLock_NotAcquiredReturnsNilUnlockFalseNoError(t *testing.T) {
	c := mustHybridL1(t)

	holder, ok, err := c.Lock("lk:busy", time.Second, 0)
	if err != nil || !ok {
		t.Fatalf("holder acquire: %v/%v", ok, err)
	}
	defer func() { _ = holder() }()

	unlock, ok, err := c.Lock("lk:busy", time.Second, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("contention reported as error: %v", err)
	}
	if ok || unlock != nil {
		t.Fatalf("lost contention must be (nil,false,nil); got unlock=%v ok=%v", unlock != nil, ok)
	}
}

func TestLock_WaitAcquiresAfterHolderReleases(t *testing.T) {
	c := mustHybridL1(t)

	holder, ok, _ := c.Lock("lk:wait", time.Second, 0)
	if !ok {
		t.Fatal("holder must win immediately")
	}
	go func() {
		time.Sleep(10 * time.Millisecond)
		_ = holder()
	}()

	unlock, ok, err := c.Lock("lk:wait", time.Second, 500*time.Millisecond)
	if err != nil || !ok {
		t.Fatalf("waiter should win after release: ok=%v err=%v", ok, err)
	}
	_ = unlock()
}

func TestLock_ContentionSingleHolderAtAnyInstant(t *testing.T) {
	c := mustHybridL1(t)

	const workers = 8
	tracker := &overlapTracker{}
	successes := make([]bool, workers)
	errs := make([]error, workers)

	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start // maximize initial collision
			unlock, ok, err := c.Lock("lk:contended", time.Second, time.Second)
			if err != nil {
				errs[i] = err
				return
			}
			if !ok {
				return
			}
			tracker.enter()
			time.Sleep(20 * time.Millisecond) // hold long enough to collide
			tracker.leave()
			successes[i] = true
			_ = unlock()
		}(i)
	}
	close(start)
	wg.Wait()

	for i := range workers {
		if errs[i] != nil {
			t.Fatalf("worker %d: unexpected infra error %v", i, errs[i])
		}
		if !successes[i] {
			t.Fatalf("worker %d never acquired within wait budget", i)
		}
	}
	if tracker.peak != 1 {
		t.Fatalf("peak simultaneous holders = %d, want exactly 1", tracker.peak)
	}
}

func TestLock_TTLExpiryAutoReleases(t *testing.T) {
	c := mustHybridL1(t)

	first, ok, err := c.Lock("lk:crashed-holder", 30*time.Millisecond, 0)
	if err != nil || !ok {
		t.Fatalf("first acquire: %v/%v", ok, err)
	}
	// No unlock: holder "crashes". Lease must self-expire, letting the next
	// caller in without anyone cleaning up.
	second, ok, err := c.Lock("lk:crashed-holder", time.Second, 200*time.Millisecond)
	if err != nil || !ok {
		t.Fatalf("expired lease not reclaimable: ok=%v err=%v", ok, err)
	}
	// The stale first unlock reports already-released/expired rather than
	// deleting the NEW owner's lease.
	if err := first(); !errors.Is(err, ErrLockNotHeld) {
		t.Fatalf("stale unlock err=%v, want ErrLockNotHeld", err)
	}
	_ = second()
}

func TestNewLockToken_IsRandomAndHexEncoded(t *testing.T) {
	seen := make(map[string]bool)
	for i := 0; i < 32; i++ {
		tok, err := newLockToken()
		if err != nil {
			t.Fatalf("token: %v", err)
		}
		if len(tok) != 32 { // 16 bytes -> 32 hex chars
			t.Fatalf("token length %d, want 32 hex chars", len(tok))
		}
		if seen[tok] {
			t.Fatalf("duplicate token %q generated", tok)
		}
		seen[tok] = true
	}
}

// ---------------------------------------------------------------------------
// Scripter capability surface (ExternalProvider, dead-backend guards)
// ---------------------------------------------------------------------------

// newDeadExternalProvider wires an ExternalProvider pointed at a guaranteed-
// closed port so every scripting op surfaces a real error path.
func newDeadExternalProvider(t *testing.T) *ExternalProvider {
	t.Helper()
	opts := testDefaultOptions()
	opts.Connection = closedPort(t)
	opts.OperationTimeout = 300 * time.Millisecond
	opts.RetryCount = 0
	opts.ConnectTimeout = 150 * time.Millisecond
	opts.ReadTimeout = 150 * time.Millisecond
	p := NewExternalProvider(context.Background(), opts)
	t.Cleanup(func() { _ = p.Close() })
	return p
}

func TestExternalProvider_ScriptMethods_ErrorOnUnreachableBackend(t *testing.T) {
	p := newDeadExternalProvider(t)

	if _, _, err := p.AllowN("dead-rl", 1, time.Second); err == nil || !strings.HasPrefix(err.Error(), "cache:") {
		t.Fatalf("AllowN err=%v, want 'cache:' prefixed dial failure", err)
	}
	if _, err := p.TryLock("dead-lk", "tok", time.Second); err == nil || !strings.HasPrefix(err.Error(), "cache:") {
		t.Fatalf("TryLock err=%v, want 'cache:' prefixed dial failure", err)
	}
	if err := p.Release("dead-lk", "tok"); err == nil || !strings.HasPrefix(err.Error(), "cache:") {
		t.Fatalf("Release err=%v, want 'cache:' prefixed dial failure", err)
	}
}

// ---------------------------------------------------------------------------
// Integration — live external backend (skips cleanly when unreachable)
// ---------------------------------------------------------------------------

func TestIntegration_ScripterPrimitivesOnLiveBackend(t *testing.T) {
	opts := optsL2Real(t)
	runTag := randomHex(t, 4)

	p := NewExternalProvider(context.Background(), opts)
	defer func() { _ = p.Close() }()
	if !p.HealthCheck() {
		t.Skip("external backend configured but unreachable; skipping")
	}

	fullKey := opts.formatKey(runTag + ":scr-counter")
	if _, err := p.client.Del(context.Background(), fullKey).Result(); err != nil {
		t.Fatalf("reset key: %v", err)
	}

	count, ttlLeft, err := p.AllowN(runTag+":scr-counter", 1, time.Second)
	if err != nil {
		t.Fatalf("AllowN first: %v", err)
	}
	if count != 1 {
		t.Fatalf("first increment count=%d, want 1", count)
	}
	if ttlLeft <= 0 || ttlLeft > time.Second {
		t.Fatalf("ttlLeft=%v, want within (0,window]", ttlLeft)
	}
	count, _, err = p.AllowN(runTag+":scr-counter", 2, time.Second)
	if err != nil || count != 3 {
		t.Fatalf("increment by 2: count=%d err=%v, want 3", count, err)
	}

	lockKey := runTag + ":scr-lock"
	token := "tok-" + runTag
	ok, err := p.TryLock(lockKey, token, time.Second)
	if err != nil || !ok {
		t.Fatalf("TryLock fresh: ok=%v err=%v", ok, err)
	}
	ok, err = p.TryLock(lockKey, "other-token", time.Second)
	if err != nil || ok {
		t.Fatalf("TryLock conflicting must lose: ok=%v err=%v", ok, err)
	}
	if err := p.Release(lockKey, "wrong-token"); !errors.Is(err, ErrLockNotHeld) {
		t.Fatalf("wrong-token release err=%v, want ErrLockNotHeld", err)
	}
	if err := p.Release(lockKey, token); err != nil {
		t.Fatalf("owner release: %v", err)
	}
	if err := p.Release(lockKey, token); !errors.Is(err, ErrLockNotHeld) {
		t.Fatalf("double release err=%v, want ErrLockNotHeld", err)
	}
}

func TestIntegration_LockDistributedAcrossInstances(t *testing.T) {
	opts := optsL2Real(t)
	runTag := randomHex(t, 4)

	a := mustNewWithOptions(context.Background(), opts)
	defer func() { _ = a.Close() }()
	b := mustNewWithOptions(context.Background(), opts)
	defer func() { _ = b.Close() }()

	key := fmt.Sprintf("it-dlk:%s", runTag)
	cleanup := func() {
		_ = a.Remove(lockKey(key))
	}

	unlockA, okA, err := a.Lock(key, 2*time.Second, 0)
	if err != nil || !okA {
		t.Fatalf("instance A acquire: ok=%v err=%v", okA, err)
	}

	// Instance B must NOT steal while A holds: capped-wait fails fast.
	if unlockB, okB, errB := b.Lock(key, time.Second, 60*time.Millisecond); okB || errB != nil || unlockB != nil {
		t.Fatalf("instance B stole held lock: ok=%v err=%v unlock=%v", okB, errB, unlockB != nil)
	}

	if err := unlockA(); err != nil {
		t.Fatalf("A release: %v", err)
	}
	if err := unlockA(); !errors.Is(err, ErrLockNotHeld) {
		t.Fatalf("double release: %v, want ErrLockNotHeld", err)
	}

	unlockB, okB, err := b.Lock(key, time.Second, time.Second)
	if err != nil || !okB {
		t.Fatalf("instance B acquire after release: ok=%v err=%v", okB, err)
	}
	if err := unlockB(); err != nil {
		t.Fatalf("B release: %v", err)
	}
	cleanup()
}

func TestIntegration_AllowSharesBudgetAcrossInstances(t *testing.T) {
	opts := optsL2Real(t)
	runTag := randomHex(t, 4)

	a := mustNewWithOptions(context.Background(), opts)
	defer func() { _ = a.Close() }()
	b := mustNewWithOptions(context.Background(), opts)
	defer func() { _ = b.Close() }()

	key := fmt.Sprintf("it-rl:%s", runTag)
	cleanup := func() {
		_ = a.Remove(rateLimitKey(key))
	}

	const limit = 5
	totalAllowed := 0
	blockedSeen := false
	for call := 0; call < limit*3 && !blockedSeen; call++ {
		target := a
		if call%2 == 1 {
			target = b // alternate consumers across the two instances
		}
		allowed, remaining, resetIn, err := target.Allow(key, limit, 10*time.Second)
		if err != nil {
			t.Fatalf("call %d: %v", call, err)
		}
		if allowed {
			totalAllowed++
			if call < limit && remaining < 0 {
				t.Fatalf("remaining went negative: %d", remaining)
			}
		} else {
			blockedSeen = true
			if remaining != 0 || resetIn <= 0 {
				t.Fatalf("blocked tuple wrong: remaining=%d resetIn=%v", remaining, resetIn)
			}
		}
	}
	if !blockedSeen {
		t.Fatal("limiter never blocked despite exceeding limit")
	}
	if totalAllowed != limit {
		t.Fatalf("shared budget admitted %d actions, want exactly %d across both instances", totalAllowed, limit)
	}
	cleanup()
}

func TestIntegration_IdempotentDeduplicatesAcrossInstances(t *testing.T) {
	opts := optsL2Real(t)
	runTag := randomHex(t, 4)

	producer := mustNewWithOptions(context.Background(), opts)
	defer func() { _ = producer.Close() }()
	consumer := mustNewWithOptions(context.Background(), opts)
	defer func() { _ = consumer.Close() }()

	key := fmt.Sprintf("it-idem:%s", runTag)
	recordKey := idempotentKey(key)
	cleanup := func() {
		_ = producer.Remove(recordKey)
	}

	var producerCalls int
	val, executed, err := producer.Idempotent(key, 30*time.Second, func() (any, error) {
		producerCalls++
		return map[string]string{"tx": runTag}, nil
	})
	if err != nil || !executed || producerCalls != 1 {
		t.Fatalf("producer: executed=%v calls=%d err=%v", executed, producerCalls, err)
	}
	_ = val

	// Independent instance, cold caches: reads the remote completion record
	// and must NOT execute.
	consumerCalls := 0
	seen, executed, err := consumer.Idempotent(key, 30*time.Second, func() (any, error) {
		consumerCalls++
		return "should-not-run", nil
	})
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}
	if executed || consumerCalls != 0 {
		t.Fatalf("consumer executed again: executed=%v calls=%d", executed, consumerCalls)
	}
	m, okMap := seen.(map[string]any)
	if !okMap || m["tx"] != runTag {
		t.Fatalf("consumer saw %#v, want stored tx payload", seen)
	}
	cleanup()
}
