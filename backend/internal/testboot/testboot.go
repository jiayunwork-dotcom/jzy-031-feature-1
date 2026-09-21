// Package testboot spins up an embedded PostgreSQL 16 and points tests at the
// local Redis, so the automated suite exercises the real persistence and
// shared-counter paths (not mocks).
package testboot

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// NewRedis returns a flushed client on the given test DB. Distinct packages
// use distinct DB indexes so parallel test binaries never share counters.
func NewRedis(t *testing.T, db int) *redis.Client {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr, DB: db})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Fatalf("redis ping: %v", err)
	}
	if err := rdb.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flush redis: %v", err)
	}
	t.Cleanup(func() {
		_ = rdb.FlushDB(context.Background())
		_ = rdb.Close()
	})
	return rdb
}
