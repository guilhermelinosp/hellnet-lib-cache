# Changelog

## Unreleased

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
