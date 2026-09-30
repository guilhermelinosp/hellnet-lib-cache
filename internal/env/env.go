package env

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"
)

// Environment loads optional dotenv files. Environment variables already set win.
func Environment(files ...string) error {
	if len(files) == 0 {
		return godotenv.Load()
	}
	paths := make([]string, 0)
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

// String returns an environment value or fallback when unset.
func String(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

// Int returns an integer environment value or fallback when unset or invalid.
func Int(key string, fallback int) int {
	value, err := strconv.Atoi(String(key, strconv.Itoa(fallback)))
	if err != nil {
		return fallback
	}
	return value
}

// Bool returns a boolean environment value or fallback when unset or invalid.
func Bool(key string, fallback bool) bool {
	value, err := strconv.ParseBool(String(key, strconv.FormatBool(fallback)))
	if err != nil {
		return fallback
	}
	return value
}

// Duration returns a duration environment value or fallback when unset or invalid.
func Duration(key string, fallback time.Duration) time.Duration {
	raw := String(key, fallback.String())
	if value, err := time.ParseDuration(raw); err == nil {
		return value
	}
	var h, m, s int
	if _, err := fmt.Sscanf(raw, "%d:%d:%d", &h, &m, &s); err == nil {
		return time.Duration(h)*time.Hour + time.Duration(m)*time.Minute + time.Duration(s)*time.Second
	}
	return fallback
}
