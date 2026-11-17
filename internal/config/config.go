// Package config loads settings from environment variables (12-factor).
package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds runtime settings. Empty optional values disable the integration
// (the service then runs with in-memory adapters, handy for local demos).
type Config struct {
	HTTPAddr        string
	LogLevel        string
	DatabaseURL     string
	RedisAddr       string
	KafkaBrokers    []string
	KafkaTopic      string
	KafkaDLQTopic   string
	KafkaGroupID    string
	Workers         int
	ShutdownTimeout time.Duration
}

// Load reads the configuration.
func Load() Config {
	return Config{
		HTTPAddr:        env("HTTP_ADDR", ":8080"),
		LogLevel:        env("LOG_LEVEL", "info"),
		DatabaseURL:     os.Getenv("DATABASE_URL"),
		RedisAddr:       os.Getenv("REDIS_ADDR"),
		KafkaBrokers:    splitNonEmpty(os.Getenv("KAFKA_BROKERS")),
		KafkaTopic:      env("KAFKA_TOPIC", "payments.events"),
		KafkaDLQTopic:   env("KAFKA_DLQ_TOPIC", "payments.events.dlq"),
		KafkaGroupID:    env("KAFKA_GROUP_ID", "ledger"),
		Workers:         envInt("RECONCILE_WORKERS", 8),
		ShutdownTimeout: 15 * time.Second,
	}
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(key)); err == nil && v > 0 {
		return v
	}
	return def
}

func splitNonEmpty(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
