package store_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"ratelimit-gateway/internal/canary"
	"ratelimit-gateway/internal/model"
	"ratelimit-gateway/internal/store"
	"ratelimit-gateway/internal/testboot"
)

func canaryContent(threshold int64) *model.Rule {
	return &model.Rule{
		Name:       "rollout-me",
		Enabled:    true,
		Algorithm:  model.AlgoFixedWindow,
		Dimensions: []string{model.DimClient},
		Levels: map[model.Level]model.RuleLevel{
			model.LevelClient: {Threshold: threshold, WindowSeconds: 30},
		},
	}
}

func createBaseRule(ctx context.Context, rs *store.RuleStore, t *testing.T) *model.Rule {
	r := canaryContent(100)
	created, err := rs.Create(ctx, r)
	if err != nil {
		t.Fatalf("create base: %v", err)
	}
	return created
}

// TestRolloutStartAndGet: a started rollout snapshots old+canary at percent.
func TestRolloutStartAndGet(t *testing.T) {
	_, rs, ros, _, _ := testboot.FullStack(t)
	ctx := context.Background()
	base := createBaseRule(ctx, rs, t)

	ro, err := ros.Start(ctx, base, canaryContent(20), 10)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if ro.Percent != 10 {
		t.Fatalf("percent = %d want 10", ro.Percent)
	}
	if ro.Old.Levels[model.LevelClient].Threshold != 100 {
		t.Fatalf("old snapshot threshold wrong: %d", ro.Old.Levels[model.LevelClient].Threshold)
	}
	if ro.Canary.Levels[model.LevelClient].Threshold != 20 {
		t.Fatalf("canary threshold wrong: %d", ro.Canary.Levels[model.LevelClient].Threshold)
	}
	got, ok := ros.Get(base.ID)
	if !ok || got.Percent != 10 {
		t.Fatalf("Get did not return in-flight rollout: %+v", got)
	}
}

// TestRolloutStartRejectsBadCanary: the same validation that guards normal
// rules also guards canary content — an illegal new rule cannot enter judging.
func TestRolloutStartRejectsBadCanary(t *testing.T) {
	_, rs, ros, _, _ := testboot.FullStack(t)
	ctx := context.Background()
	base := createBaseRule(ctx, rs, t)

	bad := canaryContent(0) // non-positive threshold
	_, err := ros.Start(ctx, base, bad, 10)
	if err == nil {
		t.Fatal("illegal canary must be rejected")
	}
	var inv store.ErrInvalid
	if !asErrInvalid(err, &inv) {
		t.Fatalf("want ErrInvalid, got %T %v", err, err)
	}
	if !strings.Contains(strings.ToLower(err.Error()), "canary") {
		t.Fatalf("error should mention canary content: %v", err)
	}

	badAlgo := canaryContent(20)
	badAlgo.Algorithm = "nope"
	if _, err := ros.Start(ctx, base, badAlgo, 10); err == nil {
		t.Fatal("unknown algorithm in canary must be rejected")
	}
}

// TestRolloutSetPercentBounds: out of range / invalid percents are refused.
func TestRolloutSetPercentBounds(t *testing.T) {
	_, rs, ros, _, _ := testboot.FullStack(t)
	ctx := context.Background()
	base := createBaseRule(ctx, rs, t)
	if _, err := ros.Start(ctx, base, canaryContent(20), 10); err != nil {
		t.Fatal(err)
	}
	for _, p := range []int{-1, -50, 101, 1000} {
		if _, err := ros.SetPercent(ctx, base.ID, p); err == nil {
			t.Fatalf("percent %d must be rejected", p)
		}
	}
	for _, p := range []int{0, 50, 100} {
		if _, err := ros.SetPercent(ctx, base.ID, p); err != nil {
			t.Fatalf("percent %d must be accepted: %v", p, err)
		}
	}
}

// TestRolloutStartDuplicateRejected: a rule can only have one rollout.
func TestRolloutStartDuplicateRejected(t *testing.T) {
	_, rs, ros, _, _ := testboot.FullStack(t)
	ctx := context.Background()
	base := createBaseRule(ctx, rs, t)
	if _, err := ros.Start(ctx, base, canaryContent(20), 10); err != nil {
		t.Fatal(err)
	}
	if _, err := ros.Start(ctx, base, canaryContent(30), 10); err == nil {
		t.Fatal("second concurrent rollout must be rejected")
	}
}

// TestRolloutAbort: aborting removes the rollout so the rule is single-version
// again — exactly "as if the change never happened".
func TestRolloutAbort(t *testing.T) {
	_, rs, ros, _, _ := testboot.FullStack(t)
	ctx := context.Background()
	base := createBaseRule(ctx, rs, t)
	if _, err := ros.Start(ctx, base, canaryContent(20), 50); err != nil {
		t.Fatal(err)
	}
	if err := ros.Abort(ctx, base.ID); err != nil {
		t.Fatal(err)
	}
	if _, ok := ros.Get(base.ID); ok {
		t.Fatal("rollout must be gone after abort")
	}
	if err := ros.Abort(ctx, base.ID); err == nil {
		t.Fatal("aborting a non-existent rollout must error")
	}
}

// TestRolloutPromote: at 100% the canary becomes the single live rule and the
// rollout row disappears; promotion below 100% is refused.
func TestRolloutPromote(t *testing.T) {
	_, rs, ros, _, _ := testboot.FullStack(t)
	ctx := context.Background()
	base := createBaseRule(ctx, rs, t)
	if _, err := ros.Start(ctx, base, canaryContent(20), 50); err != nil {
		t.Fatal(err)
	}
	if _, err := ros.Promote(ctx, rs, base.ID); err == nil {
		t.Fatal("promote below 100% must be rejected")
	}
	if _, err := ros.SetPercent(ctx, base.ID, 100); err != nil {
		t.Fatal(err)
	}
	promoted, err := ros.Promote(ctx, rs, base.ID)
	if err != nil {
		t.Fatalf("promote: %v", err)
	}
	if promoted.Levels[model.LevelClient].Threshold != 20 {
		t.Fatalf("live rule must now be canary content, got %d",
			promoted.Levels[model.LevelClient].Threshold)
	}
	if _, ok := ros.Get(base.ID); ok {
		t.Fatal("rollout must be gone after promote")
	}
	live, _ := rs.Get(base.ID)
	if live.Levels[model.LevelClient].Threshold != 20 {
		t.Fatal("persisted live rule must be the canary")
	}
}

// TestRolloutPersistenceAcrossRestart: after a brand-new RolloutStore opens
// against the same Postgres, the rollout (rule, percent, both versions) is
// still there and decisions can resume at the same percent.
func TestRolloutPersistenceAcrossRestart(t *testing.T) {
	dsn := testboot.DSN()
	rdb := testboot.NewRedis(t, 11)
	ctx := context.Background()

	rs1, err := store.New(ctx, dsn, rdb)
	if err != nil {
		t.Fatal(err)
	}
	ros1, err := store.NewRolloutStore(ctx, rs1.PgPool(), rdb)
	if err != nil {
		t.Fatal(err)
	}
	base := createBaseRule(ctx, rs1, t)
	if _, err := ros1.Start(ctx, base, canaryContent(20), 37); err != nil {
		t.Fatal(err)
	}

	// restart: brand-new stores against the same durable storage
	rs2, err := store.New(ctx, dsn, rdb)
	if err != nil {
		t.Fatal(err)
	}
	ros2, err := store.NewRolloutStore(ctx, rs2.PgPool(), rdb)
	if err != nil {
		t.Fatal(err)
	}
	ro, ok := ros2.Get(base.ID)
	if !ok {
		t.Fatal("rollout must survive restart")
	}
	if ro.Percent != 37 {
		t.Fatalf("percent after restart = %d want 37", ro.Percent)
	}
	if ro.Old.Levels[model.LevelClient].Threshold != 100 ||
		ro.Canary.Levels[model.LevelClient].Threshold != 20 {
		t.Fatalf("version content lost across restart: old=%d canary=%d",
			ro.Old.Levels[model.LevelClient].Threshold,
			ro.Canary.Levels[model.LevelClient].Threshold)
	}

	// The subject that was canary before restart stays canary after restart.
	rctx := model.RequestContext{ClientID: "persist-client", APIPath: "/p"}
	subj := canary.Subject(&ro.Old, rctx)
	before := canary.SelectVersion(ro.RuleID, subj, 37)
	if canary.SelectVersion(ro.RuleID, subj, ro.Percent) != before {
		t.Fatal("subject bucket changed across restart")
	}
}

// TestRolloutHotSyncAcrossInstances: a percent change made on one instance is
// picked up by another instance via the Redis pub/sub channel.
func TestRolloutHotSyncAcrossInstances(t *testing.T) {
	dsn := testboot.DSN()
	rdb := testboot.NewRedis(t, 11)
	ctx := context.Background()

	rs1, err := store.New(ctx, dsn, rdb)
	if err != nil {
		t.Fatal(err)
	}
	ros1, err := store.NewRolloutStore(ctx, rs1.PgPool(), rdb)
	if err != nil {
		t.Fatal(err)
	}
	go ros1.SubscribeHotReload(ctx)

	// "second gateway instance": separate in-memory cache, same PG+Redis.
	rs2, err := store.New(ctx, dsn, rdb)
	if err != nil {
		t.Fatal(err)
	}
	ros2, err := store.NewRolloutStore(ctx, rs2.PgPool(), rdb)
	if err != nil {
		t.Fatal(err)
	}
	go ros2.SubscribeHotReload(ctx)

	base := createBaseRule(ctx, rs1, t)
	if _, err := ros1.Start(ctx, base, canaryContent(20), 10); err != nil {
		t.Fatal(err)
	}

	mustWaitFor(t, func() bool {
		ro, ok := ros2.Get(base.ID)
		return ok && ro.Percent == 10
	}, 3*time.Second, "instance 2 to learn the started rollout")

	if _, err := ros1.SetPercent(ctx, base.ID, 63); err != nil {
		t.Fatal(err)
	}
	mustWaitFor(t, func() bool {
		ro, ok := ros2.Get(base.ID)
		return ok && ro.Percent == 63
	}, 3*time.Second, "instance 2 to learn the new percent")

	if err := ros1.Abort(ctx, base.ID); err != nil {
		t.Fatal(err)
	}
	mustWaitFor(t, func() bool {
		_, ok := ros2.Get(base.ID)
		return !ok
	}, 3*time.Second, "instance 2 to learn the abort")
}

func mustWaitFor(t *testing.T, cond func() bool, d time.Duration, what string) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}
