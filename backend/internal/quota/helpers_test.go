package quota_test

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"

	"ratelimit-gateway/internal/engine"
	"ratelimit-gateway/internal/quota"
	"ratelimit-gateway/internal/stats"
	"ratelimit-gateway/internal/store"
	"ratelimit-gateway/internal/testboot"
)

func newMgr(ctx context.Context, rdb *redis.Client) (*engine.Manager, error) {
	return engine.NewManager(ctx, rdb)
}

func newRec(rdb *redis.Client) *stats.Recorder { return stats.New(rdb) }

type stack struct {
	rdb   *redis.Client
	rules *store.RuleStore
	mgr   *engine.Manager
	rec   *stats.Recorder
}

func newStack(t *testing.T) *stack {
	t.Helper()
	rdb, rules, mgr, rec := testboot.FullStack(t)
	return &stack{rdb, rules, mgr, rec}
}

func (s *stack) checker() *quota.Checker {
	return quota.New(s.rules, s.mgr, s.rdb, s.rec)
}

// newChecker keeps the per-test call sites small.
func newChecker(t *testing.T) (*store.RuleStore, *quota.Checker, *stats.Recorder) {
	t.Helper()
	s := newStack(t)
	return s.rules, s.checker(), s.rec
}

func testbootFull(t *testing.T) (*redis.Client, *store.RuleStore, *engine.Manager, *stats.Recorder) {
	t.Helper()
	s := newStack(t)
	return s.rdb, s.rules, s.mgr, s.rec
}

func newCheckerFrom(t *testing.T, rdb *redis.Client, rules *store.RuleStore, mgr *engine.Manager, rec *stats.Recorder) *quota.Checker {
	t.Helper()
	return quota.New(rules, mgr, rdb, rec)
}
