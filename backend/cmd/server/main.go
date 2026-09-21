// Command server runs the rate-limiting gateway: it connects to Redis (shared
// counters) and PostgreSQL (persistent rules), loads and hot-reloads rules,
// and serves the gateway ingress + configuration/observability API.
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"ratelimit-gateway/internal/api"
	"ratelimit-gateway/internal/config"
	"ratelimit-gateway/internal/engine"
	"ratelimit-gateway/internal/quota"
	"ratelimit-gateway/internal/stats"
	"ratelimit-gateway/internal/store"
)

func main() {
	cfg := config.Load()
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	})
	if err := waitForRedis(ctx, rdb); err != nil {
		log.Fatalf("redis not ready: %v", err)
	}

	// Rule storage blocks until PostgreSQL is reachable so a fresh
	// `docker compose up` does not race the database.
	rules, err := waitForPostgres(ctx, cfg.PostgresDSN, rdb)
	if err != nil {
		log.Fatalf("rule store not ready: %v", err)
	}
	log.Printf("loaded %d rules from postgres", len(rules.List()))

	go rules.SubscribeHotReload(ctx)

	engines, err := engine.NewManager(ctx, rdb)
	if err != nil {
		log.Fatalf("init engines: %v", err)
	}
	rec := stats.New(rdb)
	checker := quota.New(rules, engines, rdb, rec)

	srv := api.NewServer(cfg, rules, engines, rec, checker, rdb)
	httpServer := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           srv.Router(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Printf("gateway listening on %s", cfg.HTTPAddr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http server: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = httpServer.Shutdown(shutdownCtx)
	log.Println("gateway stopped")
}

func waitForRedis(ctx context.Context, rdb *redis.Client) error {
	var err error
	for i := 0; i < 60; i++ {
		if err = rdb.Ping(ctx).Err(); err == nil {
			return nil
		}
		time.Sleep(time.Second)
	}
	return err
}

func waitForPostgres(ctx context.Context, dsn string, rdb *redis.Client) (*store.RuleStore, error) {
	var lastErr error
	for i := 0; i < 60; i++ {
		s, err := store.New(ctx, dsn, rdb)
		if err == nil {
			return s, nil
		}
		lastErr = err
		time.Sleep(time.Second)
	}
	return nil, lastErr
}
