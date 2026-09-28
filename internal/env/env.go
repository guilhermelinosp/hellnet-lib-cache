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

func String(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
func Prefixed(prefixes []string, key, fallback string) string {
	for _, prefix := range prefixes {
		if value := os.Getenv(prefix + key); value != "" {
			return value
		}
	}
	return fallback
}
func Int(key string, fallback int) int {
	value, err := strconv.Atoi(String(key, strconv.Itoa(fallback)))
	if err != nil {
		return fallback
	}
	return value
}
func IntPrefixed(prefixes []string, key string, fallback int) int {
	value, err := strconv.Atoi(Prefixed(prefixes, key, strconv.Itoa(fallback)))
	if err != nil {
		return fallback
	}
	return value
}
func Bool(key string, fallback bool) bool {
	value, err := strconv.ParseBool(String(key, strconv.FormatBool(fallback)))
	if err != nil {
		return fallback
	}
	return value
}
func BoolPrefixed(prefixes []string, key string, fallback bool) bool {
	value, err := strconv.ParseBool(Prefixed(prefixes, key, strconv.FormatBool(fallback)))
	if err != nil {
		return fallback
	}
	return value
}
func Duration(key string, fallback time.Duration) time.Duration {
	return ParseDuration(String(key, fallback.String()), fallback)
}
func DurationPrefixed(prefixes []string, key string, fallback time.Duration) time.Duration {
	return ParseDuration(Prefixed(prefixes, key, fallback.String()), fallback)
}
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
func ParseDuration(raw string, fallback time.Duration) time.Duration {
	if value, err := time.ParseDuration(raw); err == nil {
		return value
	}
	var h, m, s int
	if _, err := fmt.Sscanf(raw, "%d:%d:%d", &h, &m, &s); err == nil {
		return time.Duration(h)*time.Hour + time.Duration(m)*time.Minute + time.Duration(s)*time.Second
	}
	return fallback
}
