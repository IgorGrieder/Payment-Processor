package config

import "os"

// Config contains runtime configuration. Keep infrastructure configuration here
// so the application core does not depend on environment variables directly.
type Config struct {
	HTTPAddr    string
	DatabaseURL string
	NATSURL     string
}

func FromEnv() Config {
	return Config{
		HTTPAddr:    envOr("HTTP_ADDR", ":8080"),
		DatabaseURL: envOr("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/rinha?sslmode=disable"),
		NATSURL:     envOr("NATS_URL", "nats://localhost:4222"),
	}
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
