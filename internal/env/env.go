package env

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

// LoadDotEnv loads optional dotenv files. Environment variables already set win.
func LoadDotEnv(files ...string) error {
	if len(files) == 0 {
		return godotenv.Load()
	}
	paths := make([]string, 0, len(files))
	for _, key := range files {
		if value := os.Getenv(key); value != "" {
			paths = append(paths, value)
		}
	}
	if len(paths) == 0 {
		return godotenv.Load()
	}
	return godotenv.Load(paths...)
}

// String returns an environment value or fallback.
func String(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

// Prefixed returns the first matching prefixed environment value.
func Prefixed(prefixes []string, key, fallback string) string {
	for _, prefix := range prefixes {
		if value := os.Getenv(prefix + key); value != "" {
			return value
		}
	}
	return fallback
}

// LookupPrefixed returns the first defined prefixed environment variable.
// Unlike Prefixed, an explicitly defined empty value is still reported.
func LookupPrefixed(prefixes []string, key string) (string, bool) {
	for _, prefix := range prefixes {
		if value, ok := os.LookupEnv(prefix + key); ok {
			return value, true
		}
	}
	return "", false
}

// Int returns a parsed integer environment value or fallback.
func Int(key string, fallback int) int {
	value, err := strconv.Atoi(String(key, strconv.Itoa(fallback)))
	if err != nil {
		return fallback
	}
	return value
}

// IntPrefixed returns a parsed prefixed integer or fallback.
func IntPrefixed(prefixes []string, key string, fallback int) int {
	value, err := strconv.Atoi(Prefixed(prefixes, key, strconv.Itoa(fallback)))
	if err != nil {
		return fallback
	}
	return value
}

// Bool returns a parsed boolean environment value or fallback.
func Bool(key string, fallback bool) bool {
	value, err := strconv.ParseBool(String(key, strconv.FormatBool(fallback)))
	if err != nil {
		return fallback
	}
	return value
}

// BoolPrefixed returns a parsed prefixed boolean or fallback.
func BoolPrefixed(prefixes []string, key string, fallback bool) bool {
	value, err := strconv.ParseBool(Prefixed(prefixes, key, strconv.FormatBool(fallback)))
	if err != nil {
		return fallback
	}
	return value
}

// Duration returns a parsed duration environment value or fallback.
func Duration(key string, fallback time.Duration) time.Duration {
	return ParseDuration(String(key, fallback.String()), fallback)
}

// DurationPrefixed returns a parsed prefixed duration or fallback.
func DurationPrefixed(prefixes []string, key string, fallback time.Duration) time.Duration {
	return ParseDuration(Prefixed(prefixes, key, fallback.String()), fallback)
}

// DurationPrefixedE parses a prefixed duration and reports malformed values.
// Keys ending in _MS or _SEC also accept a bare integer in that unit, while
// Go duration and HH:MM:SS syntax remain supported for compatibility.
func DurationPrefixedE(prefixes []string, key string, fallback time.Duration) (time.Duration, error) {
	raw, ok := LookupPrefixed(prefixes, key)
	if !ok {
		return fallback, nil
	}
	unit := time.Nanosecond
	switch {
	case strings.HasSuffix(key, "_MS"):
		unit = time.Millisecond
	case strings.HasSuffix(key, "_SEC"):
		unit = time.Second
	}
	value, err := parseDurationWithUnit(raw, unit, strings.HasSuffix(key, "_MS") || strings.HasSuffix(key, "_SEC"))
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return value, nil
}

// Slice returns a comma-separated environment value as trimmed items.
func Slice(key string) []string {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return nil
	}
	values := make([]string, 0)
	for _, value := range strings.Split(raw, ",") {
		if value = strings.TrimSpace(value); value != "" {
			values = append(values, value)
		}
	}
	return values
}

// ParseDuration parses Go or HH:MM:SS duration syntax.
func ParseDuration(raw string, fallback time.Duration) time.Duration {
	value, err := parseDurationWithUnit(raw, time.Nanosecond, false)
	if err == nil {
		return value
	}
	return fallback
}

func parseDurationWithUnit(raw string, unit time.Duration, allowBareInteger bool) (time.Duration, error) {
	if value, err := time.ParseDuration(raw); err == nil {
		return value, nil
	}
	var h, m, s int
	if n, err := fmt.Sscanf(raw, "%d:%d:%d", &h, &m, &s); err == nil && n == 3 {
		return time.Duration(h)*time.Hour + time.Duration(m)*time.Minute + time.Duration(s)*time.Second, nil
	}
	if !allowBareInteger {
		return 0, fmt.Errorf("invalid duration %q", raw)
	}
	integer, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid duration %q", raw)
	}
	return time.Duration(integer) * unit, nil
}
