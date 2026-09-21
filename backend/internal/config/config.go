// Package config loads process configuration from the environment.
package config

import (
	"os"
	"strconv"
)

type Config struct {
	HTTPAddr      string
	RedisAddr     string
	RedisPassword string
	RedisDB       int
	PostgresDSN   string
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func Load() Config {
	db, _ := strconv.Atoi(getenv("REDIS_DB", "0"))
	return Config{
		HTTPAddr:      getenv("HTTP_ADDR", ":8080"),
		RedisAddr:     getenv("REDIS_ADDR", "localhost:6379"),
		RedisPassword: getenv("REDIS_PASSWORD", ""),
		RedisDB:       db,
		PostgresDSN:   getenv("POSTGRES_DSN", "postgres://rl:rl@localhost:5432/ratelimit?sslmode=disable"),
	}
}
