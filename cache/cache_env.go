package cache

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

func cacheEnv(prefixes []string, key, fallback string) string {
	for _, prefix := range prefixes {
		if value := os.Getenv(prefix + key); value != "" {
			return value
		}
	}
	return fallback
}

func cacheLookupEnv(prefixes []string, key string) (string, bool) {
	for _, prefix := range prefixes {
		if value, ok := os.LookupEnv(prefix + key); ok {
			return value, true
		}
	}
	return "", false
}

func cacheIntEnv(prefixes []string, key string, fallback int) int {
	value, err := strconv.Atoi(cacheEnv(prefixes, key, strconv.Itoa(fallback)))
	if err != nil {
		return fallback
	}
	return value
}

func cacheBoolEnv(prefixes []string, key string, fallback bool) bool {
	value, err := strconv.ParseBool(cacheEnv(prefixes, key, strconv.FormatBool(fallback)))
	if err != nil {
		return fallback
	}
	return value
}

func cacheDurationEnv(prefixes []string, key string, fallback time.Duration) (time.Duration, error) {
	raw, ok := cacheLookupEnv(prefixes, key)
	if !ok {
		return fallback, nil
	}
	unit := time.Nanosecond
	allowBareInteger := false
	switch {
	case strings.HasSuffix(key, "_MS"):
		unit, allowBareInteger = time.Millisecond, true
	case strings.HasSuffix(key, "_SEC"):
		unit, allowBareInteger = time.Second, true
	}
	value, err := parseCacheDuration(raw, unit, allowBareInteger)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return value, nil
}

func parseCacheDuration(raw string, unit time.Duration, allowBareInteger bool) (time.Duration, error) {
	if value, err := time.ParseDuration(raw); err == nil {
		return value, nil
	}
	var h, m, s int
	if n, err := fmt.Sscanf(raw, "%d:%d:%d", &h, &m, &s); err == nil && n == 3 {
		return time.Duration(h)*time.Hour + time.Duration(m)*time.Minute + time.Duration(s)*time.Second, nil
	}
	if allowBareInteger {
		integer, err := strconv.ParseInt(raw, 10, 64)
		if err == nil {
			return time.Duration(integer) * unit, nil
		}
	}
	return 0, fmt.Errorf("invalid duration %q", raw)
}
