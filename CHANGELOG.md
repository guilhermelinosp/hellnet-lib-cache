# Changelog

## Unreleased

- `New` and `MustNew` now take `(ctx, instrument.Instrumentation)` (for example a
  `*telemetry.Telemetry`, or nil): `cache.New(ctx, tel)` replaces `cache.New()`.

### Added

- Added hermetic test isolation, goleak coverage, simulated local-coordination
  clocks, and miniredis-backed provider tests.
- Added `NewWithOptions` with `WithOptions`, `WithProviders`, and
  `WithSerializer` for explicit dependency injection.

### Changed

- Environment documentation now uses `HELLNET_CACHE_*`, with `HELLNET_*` as
  fallback.
- `_MS` and `_SEC` duration variables now interpret bare integers using their
  declared unit. Go and `HH:MM:SS` duration syntax remain supported.
- Defined invalid duration variables now make `New` return an error instead of
  silently using the default.
- An explicit `HELLNET_CACHE_ENABLE_L2=true` without a connection now fails
  construction. Unspecified L2 still degrades to memory-only with one warning.
- Redis retry backoff now uses `RetryBaseDelay`.

### Compatibility

- `New` continues loading `.env` for backward compatibility. `NewWithOptions`
  does not load dotenv files.
- `L1ExpirationScanFrequency` remains in `Options` for source compatibility;
  ristretto owns expiration processing, so this field has no runtime effect.
## Unreleased

- Added the private observability construction seam for the `instrument` contract.
- Added `WithInstrumentation` to `NewWithOptions`; signal emission remains queued for the instrumentation phases.
- Added context-first `GetContext`, `SetContext`, `SetBytesContext`, `RemoveContext`, `ExistsContext` and `GetOrSetContext` APIs with caller-parented cache spans.
- Added `hellnet.cache.operations` and `hellnet.cache.operation.duration` instruments for context-first operations.
- Added `HealthCheck(ctx)` for `/health` registration; it is intentionally not a readiness signal.
- Deprecated the legacy atomic `Metrics` type in favor of OpenTelemetry metrics.
- `cache.get` now reports `result=hit|miss|error` and records OTel span errors.
- Context-first set operations now report `result=success|error` and record OTel span errors.
- Added Redis OTel hooks using contract-supplied providers. This upgrades go-redis to v9.22.0 because the separately versioned `redisotel/v9` module has no compatible v9.7.x release.
- `GetOrSetContext` now reports `result=hit|miss|error` and records OTel span errors.
- Migrated hybrid-cache write, remove, warm and L2-degraded logs to the instrumentation contract; cache keys are no longer logged.
- Migrated external-provider, breaker and rate-limit degradation logs to the instrumentation contract; no production cache code imports `log`.
