package testboot

import (
	"context"
	"log"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"

	"ratelimit-gateway/internal/engine"
	"ratelimit-gateway/internal/stats"
	"ratelimit-gateway/internal/store"
)

var (
	dsn     string
	redisDB = 15 // engine-only tests default to DB 15
)

// Main is a TestMain body: it starts one embedded PG16 for the whole test
// binary, runs the tests, and stops the database. Each test package calls it
// from its own TestMain with a distinct PG port and Redis DB so packages can
// run in parallel without sharing rules or counters.
func Main(m *testing.M, pgPort uint32, redisDBIndex int) {
	redisDB = redisDBIndex
	ep := embeddedpostgres.DefaultConfig().
		Version(embeddedpostgres.V16).
		Port(pgPort).
		Database("ratelimit_test").
		Username("rl").
		Password("rl").
		CachePath("/tmp/embedded-pg16-cache").
		RuntimePath("/tmp/embedded-pg16-run-" + uintToStr(pgPort))

	pg := embeddedpostgres.NewDatabase(ep)
	if err := pg.Start(); err != nil {
		log.Fatalf("start embedded postgres on %d: %v", pgPort, err)
	}
	dsn = "postgres://rl:rl@localhost:" + uintToStr(pgPort) + "/ratelimit_test?sslmode=disable"
	code := m.Run()
	_ = pg.Stop()
	os.Exit(code)
}

// DSN returns the running test database DSN.
func DSN() string { return dsn }

func truncateRules(t *testing.T) {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	// Tolerate the very first run, before store.New has created the schema.
	_, err = pool.Exec(context.Background(), `
DO $$
BEGIN
  IF EXISTS (SELECT FROM pg_tables WHERE schemaname='public' AND tablename='rules') THEN
    EXECUTE 'TRUNCATE rules';
  END IF;
END $$;`)
	if err != nil {
		t.Fatal(err)
	}
}

// RedisOnly returns a flushed Redis-backed engine manager without Postgres.
func RedisOnly(t *testing.T) (*redis.Client, *engine.Manager) {
	t.Helper()
	rdb := NewRedis(t, 15)
	mgr, err := engine.NewManager(context.Background(), rdb)
	if err != nil {
		t.Fatalf("engine manager: %v", err)
	}
	return rdb, mgr
}

// FullStack truncates persisted rules, flushes Redis and wires the store +
// engine manager + recorder, giving every test a clean slate.
func FullStack(t *testing.T) (*redis.Client, *store.RuleStore, *engine.Manager, *stats.Recorder) {
	t.Helper()
	truncateRules(t)
	rdb := NewRedis(t, redisDB)
	ctx := context.Background()
	rules, err := store.New(ctx, dsn, rdb)
	if err != nil {
		t.Fatalf("rule store: %v", err)
	}
	go rules.SubscribeHotReload(ctx)
	mgr, err := engine.NewManager(ctx, rdb)
	if err != nil {
		t.Fatalf("engine manager: %v", err)
	}
	return rdb, rules, mgr, stats.New(rdb)
}

func uintToStr(v uint32) string {
	if v == 0 {
		return "0"
	}
	var b [12]byte
	i := len(b)
	for v > 0 {
		i--
		b[i] = byte('0' + v%10)
		v /= 10
	}
	return string(b[i:])
}
