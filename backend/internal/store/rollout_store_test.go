package store_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"ratelimit-gateway/internal/model"
	"ratelimit-gateway/internal/store"
	"ratelimit-gateway/internal/testboot"
)

func oldRolloutRule() *model.Rule {
	return &model.Rule{
		Name:       "rollout-base",
		Enabled:    true,
		Algorithm:  model.AlgoFixedWindow,
		Dimensions: []string{model.DimClient},
		Levels: map[model.Level]model.RuleLevel{
			model.LevelClient: {Threshold: 100, WindowSeconds: 10},
		},
	}
}

func newRolloutRule() *model.Rule {
	return &model.Rule{
		Name:       "rollout-base",
		Enabled:    true,
		Algorithm:  model.AlgoFixedWindow,
		Dimensions: []string{model.DimClient},
		Levels: map[model.Level]model.RuleLevel{
			model.LevelClient: {Threshold: 20, WindowSeconds: 10},
		},
	}
}

// TestRolloutPersistenceAcrossRestart: after starting a rollout, a brand new
// store instance ("gateway restart") loads the same percentage and both
// rule versions.
func TestRolloutPersistenceAcrossRestart(t *testing.T) {
	dsn := testboot.DSN()
	rdb := testboot.NewRedis(t, 11)
	ctx := context.Background()

	rs1, err := store.New(ctx, dsn, rdb)
	if err != nil {
		t.Fatal(err)
	}
	r, err := rs1.Create(ctx, oldRolloutRule())
	if err != nil {
		t.Fatal(err)
	}
	rl, err := rs1.StartRollout(ctx, r.ID, newRolloutRule(), 10)
	if err != nil {
		t.Fatalf("start rollout: %v", err)
	}
	if rl.Percent != 10 || rl.NewRule.Levels[model.LevelClient].Threshold != 20 {
		t.Fatalf("rollout content wrong: %+v", rl)
	}
	// old version must stay untouched
	if got, _ := rs1.Get(r.ID); got.Levels[model.LevelClient].Threshold != 100 {
		t.Fatalf("old version changed after rollout start: %d", got.Levels[model.LevelClient].Threshold)
	}

	rs2, err := store.New(ctx, dsn, rdb) // simulate restart / new instance
	if err != nil {
		t.Fatal(err)
	}
	rl2, ok := rs2.Rollout(r.ID)
	if !ok {
		t.Fatal("rollout must survive restart")
	}
	if rl2.Percent != 10 {
		t.Fatalf("percent after restart = %d, want 10", rl2.Percent)
	}
	if rl2.NewRule.Levels[model.LevelClient].Threshold != 20 {
		t.Fatalf("new version content lost across restart: %+v", rl2.NewRule.Levels)
	}
	if old, _ := rs2.Get(r.ID); old.Levels[model.LevelClient].Threshold != 100 {
		t.Fatal("old version must be intact after restart")
	}

	// and percentage adjustment persists as well
	if err := rs2.SetRolloutPercent(ctx, r.ID, 50); err != nil {
		t.Fatal(err)
	}
	rs3, err := store.New(ctx, dsn, rdb)
	if err != nil {
		t.Fatal(err)
	}
	rl3, _ := rs3.Rollout(r.ID)
	if rl3.Percent != 50 {
		t.Fatalf("adjusted percent after restart = %d, want 50", rl3.Percent)
	}
}

// TestRolloutHotSyncAcrossInstances: a percent change on one store instance
// is observed by a second instance subscribed to the same Redis channel.
func TestRolloutHotSyncAcrossInstances(t *testing.T) {
	dsn := testboot.DSN()
	rdb := testboot.NewRedis(t, 11)
	ctx := context.Background()

	rsA, err := store.New(ctx, dsn, rdb)
	if err != nil {
		t.Fatal(err)
	}
	rsB, err := store.New(ctx, dsn, rdb)
	if err != nil {
		t.Fatal(err)
	}
	go rsA.SubscribeHotReload(ctx)
	go rsB.SubscribeHotReload(ctx)
	// pub/sub delivers no replay: give both subscriptions time to establish
	// before the first change is published.
	time.Sleep(300 * time.Millisecond)

	r, err := rsA.Create(ctx, oldRolloutRule())
	if err != nil {
		t.Fatal(err)
	}
	waitUntil(t, func() bool {
		_, ok := rsB.Get(r.ID)
		return ok
	}, "instance B sees created rule")

	if _, err := rsA.StartRollout(ctx, r.ID, newRolloutRule(), 10); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, func() bool {
		rl, ok := rsB.Rollout(r.ID)
		return ok && rl.Percent == 10
	}, "instance B sees 10% rollout")

	if err := rsA.SetRolloutPercent(ctx, r.ID, 50); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, func() bool {
		rl, ok := rsB.Rollout(r.ID)
		return ok && rl.Percent == 50
	}, "instance B sees 50% rollout")

	if err := rsA.SetRolloutPercent(ctx, r.ID, 0); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, func() bool {
		_, ok := rsB.Rollout(r.ID)
		return !ok
	}, "instance B sees rollout aborted")
	if old, _ := rsB.Get(r.ID); old.Levels[model.LevelClient].Threshold != 100 {
		t.Fatal("after abort B must judge by the untouched old version")
	}
}

// TestRolloutPromoteReplacesRule: 100% ends the rollout and makes the new
// version the single rule.
func TestRolloutPromoteReplacesRule(t *testing.T) {
	_, rs, _, _ := testboot.FullStack(t)
	ctx := context.Background()
	r, err := rs.Create(ctx, oldRolloutRule())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rs.StartRollout(ctx, r.ID, newRolloutRule(), 10); err != nil {
		t.Fatal(err)
	}
	if err := rs.SetRolloutPercent(ctx, r.ID, 100); err != nil {
		t.Fatal(err)
	}
	if _, ok := rs.Rollout(r.ID); ok {
		t.Fatal("rollout row must be gone after promotion")
	}
	got, _ := rs.Get(r.ID)
	if got.Levels[model.LevelClient].Threshold != 20 {
		t.Fatalf("promoted rule threshold = %d, want 20", got.Levels[model.LevelClient].Threshold)
	}
}

// TestRolloutInvalidPercentAndContent: percentages outside 0..100 and
// illegal new-version content are rejected with a reason.
func TestRolloutInvalidPercentAndContent(t *testing.T) {
	_, rs, _, _ := testboot.FullStack(t)
	ctx := context.Background()
	r, err := rs.Create(ctx, oldRolloutRule())
	if err != nil {
		t.Fatal(err)
	}

	for _, bad := range []int{-1, 101, 1000} {
		err := rs.SetRolloutPercent(ctx, r.ID, bad)
		if !strings.Contains(err.Error(), "between 0 and 100") {
			t.Fatalf("percent %d: want validation error, got %v", bad, err)
		}
	}
	for _, bad := range []int{0, 100} {
		if err := model.ValidatePercent(bad); err != nil {
			t.Fatalf("ValidatePercent must accept boundary %d", bad)
		}
	}

	// illegal new version: threshold non-positive must not enter evaluation
	bad := newRolloutRule()
	bad.Levels[model.LevelClient] = model.RuleLevel{Threshold: 0, WindowSeconds: 10}
	if _, err := rs.StartRollout(ctx, r.ID, bad, 10); err == nil ||
		!strings.Contains(err.Error(), "new (canary) rule version is invalid") {
		t.Fatalf("illegal new version must be rejected with a reason, got %v", err)
	}
	if _, ok := rs.Rollout(r.ID); ok {
		t.Fatal("rejected rollout must not be persisted")
	}

	// start with 0 or 100 is rejected: those are plain edit / no-ops
	if _, err := rs.StartRollout(ctx, r.ID, newRolloutRule(), 0); err == nil {
		t.Fatal("start rollout at 0 must be rejected")
	}
	if _, err := rs.StartRollout(ctx, r.ID, newRolloutRule(), 100); err == nil {
		t.Fatal("start rollout at 100 must be rejected")
	}

	// editing a rule mid-rollout conflicts
	if _, err := rs.StartRollout(ctx, r.ID, newRolloutRule(), 10); err != nil {
		t.Fatal(err)
	}
	edit := oldRolloutRule()
	edit.ID = r.ID
	if _, err := rs.Update(ctx, edit); err == nil ||
		!strings.Contains(err.Error(), "in-progress rollout") {
		t.Fatalf("update mid-rollout must conflict, got %v", err)
	}
}

// TestAbortTwiceAndSetWithoutRollout report conflicts rather than silently
// succeeding.
func TestRolloutStateGuard(t *testing.T) {
	_, rs, _, _ := testboot.FullStack(t)
	ctx := context.Background()
	r, err := rs.Create(ctx, oldRolloutRule())
	if err != nil {
		t.Fatal(err)
	}
	if err := rs.AbortRollout(ctx, r.ID); err == nil {
		t.Fatal("aborting without a rollout must conflict")
	}
	if err := rs.SetRolloutPercent(ctx, r.ID, 50); err == nil {
		t.Fatal("adjusting without a rollout must conflict")
	}
}

func waitUntil(t *testing.T, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting: %s", what)
}

var _ = fmt.Sprintf
