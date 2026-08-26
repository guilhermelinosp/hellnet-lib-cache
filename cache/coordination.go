package cache

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"time"
)

// Scripter is an optional capability extension for providers that can execute
// atomic server-side operations (Lua/Eval-style) needed by the coordination
// features: distributed rate limiting and distributed locks.
//
// HybridCache probes its provider stack via type assertion: when no layer
// implements Scripter (memory-only setups), Allow and Lock transparently fall
// back to per-process implementations. Correctness is then PROCESS-LOCAL —
// documented individually on each method.
type Scripter interface {
	// AllowN atomically adds increment to a fixed-window counter identified
	// by key, creating it with the given expiration window when absent (and
	// repairing a lost expiry on any subsequent observation). It returns the
	// resulting count and the remaining time until the current window resets.
	AllowN(key string, increment int64, window time.Duration) (count int64, ttlLeft time.Duration, err error)

	// TryLock attempts to acquire key with the given identity token,
	// atomically: success only when the key did not exist. The lock auto-
	// expires after ttl.
	TryLock(key, token string, ttl time.Duration) (bool, error)

	// Release removes key only when it still holds token (compare-and-delete).
	// Returns ErrLockNotHeld when the key was absent or held by another token.
	Release(key, token string) error
}

// ErrLockNotHeld reports that a lock release found no owned lock: it was never
// acquired by this token, already released, or auto-expired by its TTL.
var ErrLockNotHeld = errors.New("cache: lock already released or expired")

// Namespacing constants: coordination records live beside regular entries in
// the same backends, so every feature derives storage keys with a dedicated
// tag to stay independent of user Set/Get traffic on equal-looking keys.
const (
	idempotencyTag = "idem:"
	rateLimitTag   = "rl:"
	lockTag        = "lock:"
)

func idempotentKey(key string) string { return idempotencyTag + key }
func rateLimitKey(key string) string  { return rateLimitTag + key }
func lockKey(key string) string       { return lockTag + key }

// ---------------------------------------------------------------------------
// Idempotency
// ---------------------------------------------------------------------------

// idempotentEnvelope is the persisted completion record: the serialized result
// of fn wrapped in a stable envelope so the record shape can evolve without
// re-decoding raw payloads. Failures are deliberately NEVER written — the
// absence of a record always means "not completed".
type idempotentEnvelope struct {
	Result []byte `json:"result"`
}

// Idempotent executes fn at most once per key within ttl and shares the result.
//
// Semantics:
//   - First call for a key (no completion record): fn runs; on success its
//     result is stored as a completion record (same encoding path as Set,
//     under an "idem:" namespace, honoring Options.resolveTTL like every TTL
//     write) and (result, true, nil) is returned.
//   - Subsequent calls within ttl: the stored result is decoded and returned
//     as (result, false, nil) — fn is NOT invoked again.
//   - If fn FAILS: nothing is cached, and (nil, true, err) is returned with
//     fn's error verbatim. The next call retries fn — failures don't poison
//     the key.
//   - If fn succeeded but persisting the record totally failed (SetBytes'
//     total-failure contract), (nil, true, err) is returned with a wrapped
//     persistence error: fn DID run and the caller should compensate via its
//     business outcome, because the next call WILL retry fn while the record
//     is absent.
//
// Concurrency: calls within one process are stampede-protected by the same
// singleflight group used by GetOrSet — concurrent first calls coalesce and
// invoke fn exactly once in-process; every waiter receives the shared result
// with executed=true (it belongs to an execution wave that happened NOW).
//
// Cross-instance guarantee: BEST-EFFORT last-write-wins. Two service instances
// that concurrently miss (record not yet visible remotely) may both execute
// fn; both write a record and either copy wins. It is NOT an exactly-once
// barrier between processes — pair it with a distributed Lock when strict
// cross-instance dedup matters.
//
// The context model applies unchanged: fn takes no context; orchestration runs
// under the internally derived operation context.
func (h *HybridCache) Idempotent(key string, ttl time.Duration, fn func() (any, error)) (any, bool, error) {
	if key == "" {
		return nil, false, fmt.Errorf("cache: idempotent: key must not be empty")
	}
	if fn == nil {
		return nil, false, fmt.Errorf("cache: idempotent %q: fn must not be nil", key)
	}

	// Fast path: completed before → serve the stored result.
	if rec, ok := h.idempotentLoad(key); ok {
		return rec, false, nil
	}

	type flightResult struct {
		value    any
		executed bool
	}

	res, err, _ := h.flight.Do(idempotentKey(key), func() (any, error) {
		// Double-check after winning the execution slot: another caller may
		// have completed the record between our fast path and here.
		if rec, ok := h.idempotentLoad(key); ok {
			return flightResult{value: rec}, nil
		}

		value, ferr := fn()
		if ferr != nil {
			// Failures are never cached — the key remains free for retry.
			return nil, ferr
		}

		data, serr := h.serializer.Serialize(value)
		if serr != nil {
			return nil, fmt.Errorf("cache: idempotent %q: %w", key, serr)
		}
		record, rerr := h.serializer.Serialize(idempotentEnvelope{Result: data})
		if rerr != nil {
			return nil, fmt.Errorf("cache: idempotent %q: %w", key, rerr)
		}
		if serr := h.SetBytes(idempotentKey(key), record, ttl); serr != nil {
			// Executed-and-succeeded but the completion record could not be
			// stored anywhere: surface the failure instead of pretending the
			// key is protected going forward.
			return nil, fmt.Errorf("cache: idempotent %q: record persist failed (fn already executed successfully): %w", key, serr)
		}
		return flightResult{value: value, executed: true}, nil
	})
	if err != nil {
		return nil, true, err
	}

	fr, ok := res.(flightResult)
	if !ok {
		return nil, false, nil // unreachable with current callback; defensive
	}
	return fr.value, fr.executed, nil
}

// idempotentLoad fetches and decodes a completion record, reporting miss on
// any corruption (a corrupt record behaves as "never completed": safe retry).
func (h *HybridCache) idempotentLoad(key string) (any, bool) {
	data, foundAtIndex := h.getRaw(idempotentKey(key))
	if foundAtIndex < 0 || len(data) == 0 {
		return nil, false
	}
	var env idempotentEnvelope
	if err := h.serializer.Deserialize(data, &env); err != nil || len(env.Result) == 0 {
		return nil, false
	}
	var stored any
	if err := h.serializer.Deserialize(env.Result, &stored); err != nil {
		return nil, false
	}
	return stored, true
}

// ---------------------------------------------------------------------------
// Distributed rate limiting — fixed-window counter
// ---------------------------------------------------------------------------

// rateWindow is the per-process fixed-window bucket used when no Scripter
// provider is available (or while the external backend is unreachable).
type rateWindow struct {
	expiresAt time.Time
	count     int64
}

// maxLocalRateWindows bounds the lazy cleanup cost: past this many tracked
// keys, reads sweep expired buckets instead of letting the map grow freely.
const maxLocalRateWindows = 1024

// Allow reports whether an action identified by key may proceed now, enforcing
// limit actions per fixed window of the given duration (the window length is
// used verbatim — no DefaultTTL/MaxTTL rewriting, unlike Set paths).
//
// Returns (allowed, remaining, resetIn, err): remaining actions in the current
// window after this decision, and the time until the window resets (0 when
// allowed). Invalid input (limit <= 0 or window <= 0) returns an error.
//
// With an L2 implementing Scripter, the budget lives in the shared backend:
// INCR + expire-on-first-increment via a Lua script — every instance sharing
// the cache shares the budget. When no such layer exists, enforcement falls
// back to per-process fixed-window counters (correct process-local only).
// Transient backend failures degrade to the same local counting with a logged
// warning rather than failing hard, mirroring the library's graceful-
// degradation contract; local input validation errors are still surfaced.
func (h *HybridCache) Allow(key string, limit int64, window time.Duration) (allowed bool, remaining int64, resetIn time.Duration, err error) {
	if key == "" {
		return false, 0, 0, fmt.Errorf("cache: allow: key must not be empty")
	}
	if limit <= 0 {
		return false, 0, 0, fmt.Errorf("cache: allow: limit must be positive, got %d", limit)
	}
	if window <= 0 {
		return false, 0, 0, fmt.Errorf("cache: allow: window must be positive, got %v", window)
	}

	fullKey := rateLimitKey(key)

	if s := h.scriptProvider(); s != nil {
		count, ttlLeft, serr := s.AllowN(fullKey, 1, window)
		if serr != nil {
			log.Printf("[hellnet-cache] rate-limit backend error for %s: %v — degrading to process-local window", key, serr)
		} else {
			allowed, remaining, resetIn = h.allowDecision(count, ttlLeft, limit)
			return allowed, remaining, resetIn, nil
		}
	}

	count, windowResetIn := h.allowLocal(fullKey, window)
	allowed, remaining, resetIn = h.allowDecision(count, windowResetIn, limit)
	return allowed, remaining, resetIn, nil
}

// allowDecision translates a raw window count into the public tuple.
func (h *HybridCache) allowDecision(count int64, resetIn time.Duration, limit int64) (allowed bool, remaining int64, resetRemaining time.Duration) {
	if resetIn < 0 {
		resetIn = 0
	}
	remaining = limit - count
	if remaining < 0 {
		remaining = 0
	}
	return count <= limit, remaining, resetIn
}

// allowLocal maintains the per-process fixed-window counters behind a mutex,
// cleaning up expired entries lazily so idle key churn stays bounded.
func (h *HybridCache) allowLocal(key string, window time.Duration) (count int64, resetIn time.Duration) {
	h.memMu.Lock()
	defer h.memMu.Unlock()

	now := time.Now()
	if h.memRL == nil {
		h.memRL = make(map[string]*rateWindow)
	}
	w, ok := h.memRL[key]
	if !ok || !now.Before(w.expiresAt) {
		w = &rateWindow{expiresAt: now.Add(window)}
		h.memRL[key] = w
		if len(h.memRL) > maxLocalRateWindows {
			h.sweepExpiredWindows(now)
		}
	}
	w.count++
	resetIn = w.expiresAt.Sub(now)
	return w.count, resetIn
}

// sweepExpiredWindows drops stale buckets. Caller must hold memMu.
func (h *HybridCache) sweepExpiredWindows(now time.Time) {
	for k, w := range h.memRL {
		if !now.Before(w.expiresAt) {
			delete(h.memRL, k)
		}
	}
}

// ---------------------------------------------------------------------------
// Distributed locks
// ---------------------------------------------------------------------------

// lockRetryInterval is the pacing between acquisition attempts while waiting.
const (
	lockRetryInterval = 25 * time.Millisecond
	defaultLockTTL    = 10 * time.Second

	maxLocalLockEntries = 256 // lazy-cleanup trigger for the local lock map
)

// lockEntry is a process-local lock slot used when no Scripter provider is
// configured. Expiry mirrors the distributed TTL auto-release.
type lockEntry struct {
	token     string
	expiresAt time.Time
}

// Lock acquires a mutual-exclusion lease named key, holding it for ttl unless
// explicitly unlocked, trying for up to wait to win contention.
//
// It returns (unlock, ok, err):
//   - ok=true: ownership acquired; unlock releases it (releases only when the
//     token still matches — compare-and-delete). A second unlock fails with an
//     error wrapping ErrLockNotHeld ("already released or expired").
//   - ok=false: contention lost within wait — not an error. unlock is nil.
//   - err: infrastructure failure (backend error in distributed mode, token
//     entropy exhaustion). Callers MUST distinguish err!=nil from ok=false.
//
// Tokens are random 16-byte hex values generated per call, so unlocks from
// other holders can never delete someone else's lease.
//
// Modes: when an L2 implements Scripter, the lease is a shared-backend
// SET-NX-PX lock enforced across every instance joining that backend; release
// is a token-checked Lua compare-and-del. Without such a layer the lock
// degrades to an in-process mutex map — correct PER-PROCESS ONLY.
//
// Guarantees and limits (both modes):
//   - Crash safety: ttl auto-releases a dead holder's lease. There is NO
//     renewal/watchdog in v1 — operations running longer than ttl lose
//     exclusivity silently. Choose ttl above worst-case work duration.
//   - NOT fencing-safe: between expiry and cleanup, a slow previous holder and
//     the new owner may briefly overlap on the guarded resource. Do not rely
//     on it for strongly-consistent resources without your own fencing tokens.
//
// Context model: ctx-free like every operation — orchestration derives the
// internal operation timeout; wait/ttl are explicit durations instead.
func (h *HybridCache) Lock(key string, ttl time.Duration, wait time.Duration) (unlock func() error, ok bool, err error) {
	if key == "" {
		return nil, false, fmt.Errorf("cache: lock: key must not be empty")
	}

	token, terr := newLockToken()
	if terr != nil {
		return nil, false, fmt.Errorf("cache: lock %q: %w", key, terr)
	}

	if ttl <= 0 {
		ttl = h.opts.DefaultTTL
	}
	if resolved := h.opts.capTTL(ttl); resolved > 0 {
		ttl = resolved
	}
	if ttl <= 0 {
		ttl = defaultLockTTL
	}

	if s := h.scriptProvider(); s != nil {
		return h.lockDistributed(s, key, token, ttl, wait)
	}
	return h.lockLocal(key, token, ttl, wait)
}

// newLockToken generates a random 16-byte hex identifier.
func newLockToken() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("token generation: %w", err)
	}
	return hex.EncodeToString(buf), nil
}

// lockDistributed runs the acquire/release protocol against a Scripter
// backend, retrying every lockRetryInterval until wait elapses.
func (h *HybridCache) lockDistributed(s Scripter, key, token string, ttl, wait time.Duration) (func() error, bool, error) {
	fullKey := lockKey(key)
	deadline := time.Now().Add(wait)

	for {
		acquired, aerr := s.TryLock(fullKey, token, ttl)
		if aerr == nil && acquired {
			return func() error {
				if rerr := s.Release(fullKey, token); rerr != nil {
					return fmt.Errorf("cache: unlock %q: %w", key, rerr)
				}
				return nil
			}, true, nil
		}
		if aerr != nil {
			// Unlike the rate limiter, silent mode-switching here would trade
			// cross-instance exclusivity for availability invisibly — surface
			// the failure and let the caller decide.
			return nil, false, fmt.Errorf("cache: lock %q try-acquire: %w", key, aerr)
		}
		if time.Now().After(deadline) {
			return nil, false, nil
		}
		sleepFor := lockRetryInterval
		if waitLeft := time.Until(deadline); waitLeft < sleepFor {
			sleepFor = waitLeft
		}
		time.Sleep(sleepFor)
	}
}

// lockLocal implements the same contract over a process-local map with lazy
// expiry and bounded sweeping. Correctness is process-local by definition.
func (h *HybridCache) lockLocal(key, token string, ttl, wait time.Duration) (func() error, bool, error) {
	fullKey := lockKey(key)
	deadline := time.Now().Add(wait)

	for {
		if h.tryLockLocal(fullKey, token, ttl) {
			return func() error { return h.unlockLocal(fullKey, token, key) }, true, nil
		}
		if time.Now().After(deadline) {
			return nil, false, nil
		}
		sleepFor := lockRetryInterval
		if waitLeft := time.Until(deadline); waitLeft < sleepFor {
			sleepFor = waitLeft
		}
		time.Sleep(sleepFor)
	}
}

// tryLockLocal atomically claims the slot unless a LIVE lease exists.
func (h *HybridCache) tryLockLocal(key, token string, ttl time.Duration) bool {
	h.memMu.Lock()
	defer h.memMu.Unlock()

	now := time.Now()
	if e, exists := h.memLK[key]; exists && now.Before(e.expiresAt) {
		return false
	}
	if h.memLK == nil {
		h.memLK = make(map[string]*lockEntry)
	}
	h.memLK[key] = &lockEntry{token: token, expiresAt: now.Add(ttl)}
	if len(h.memLK) > maxLocalLockEntries {
		for k, e := range h.memLK {
			if !now.Before(e.expiresAt) {
				delete(h.memLK, k)
			}
		}
	}
	return true
}

// unlockLocal validates the token before deleting — never deletes someone
// else's live lease — and enforces single-release semantics.
func (h *HybridCache) unlockLocal(key, token, origKey string) error {
	h.memMu.Lock()
	defer h.memMu.Unlock()

	e, exists := h.memLK[key]
	if !exists || e.token != token || !time.Now().Before(e.expiresAt) {
		return fmt.Errorf("cache: unlock %q: %w", origKey, ErrLockNotHeld)
	}
	delete(h.memLK, key)
	return nil
}

// scriptProvider returns the first provider capable of scripted coordination,
// probing the stack top-down (L1 memory never implements Scripter, so this
// effectively selects the external layer when wired).
func (h *HybridCache) scriptProvider() Scripter {
	for _, p := range h.providers {
		if s, ok := p.(Scripter); ok {
			return s
		}
	}
	return nil
}
