package store_test

import (
	"context"
	"strings"
	"testing"

	"ratelimit-gateway/internal/model"
	"ratelimit-gateway/internal/store"
	"ratelimit-gateway/internal/testboot"
)

func validRule() *model.Rule {
	return &model.Rule{
		Name:       "ok",
		Enabled:    true,
		Algorithm:  model.AlgoTokenBucket,
		Dimensions: []string{model.DimClient},
		Levels: map[model.Level]model.RuleLevel{
			model.LevelClient: {Threshold: 10, Burst: 5},
		},
	}
}

// TestInvalidConfigsRejected: every malformed configuration is refused with a
// reason; the gateway never persists or runs a bad rule.
func TestInvalidConfigsRejected(t *testing.T) {
	_, rs, _, _ := testboot.FullStack(t)
	ctx := context.Background()

	bad := []struct {
		name   string
		mutate func(r *model.Rule)
		want   string
	}{
		{"empty name", func(r *model.Rule) { r.Name = "" }, "name"},
		{"unknown algorithm", func(r *model.Rule) { r.Algorithm = "magic_bucket" }, "unknown algorithm"},
		{"zero threshold", func(r *model.Rule) {
			r.Levels[model.LevelClient] = model.RuleLevel{Threshold: 0, Burst: 5}
		}, "threshold"},
		{"negative burst", func(r *model.Rule) {
			r.Levels[model.LevelClient] = model.RuleLevel{Threshold: 10, Burst: -1}
		}, "burst"},
		{"unknown dimension", func(r *model.Rule) { r.Dimensions = []string{"ssn"} }, "unknown dimension"},
		{"duplicate dimension", func(r *model.Rule) {
			r.Dimensions = []string{model.DimClient, model.DimClient}
		}, "duplicate dimension"},
		{"no levels", func(r *model.Rule) { r.Levels = map[model.Level]model.RuleLevel{} }, "at least one quota level"},
		{"client level without client dimension", func(r *model.Rule) {
			r.Dimensions = nil
		}, "client_id"},
		{"window algorithm missing window", func(r *model.Rule) {
			r.Algorithm = model.AlgoFixedWindow
			r.Levels[model.LevelClient] = model.RuleLevel{Threshold: 10}
		}, "window_seconds"},
		{"matcher on undeclared dimension", func(r *model.Rule) {
			r.Matchers = map[string][]string{model.DimGroup: {"g1"}}
		}, "does not declare"},
		{"matcher with empty value", func(r *model.Rule) {
			r.Dimensions = []string{model.DimClient}
			r.Matchers = map[string][]string{model.DimClient: {"  "}}
		}, "empty value"},
	}

	for _, tc := range bad {
		t.Run(tc.name, func(t *testing.T) {
			r := validRule()
			tc.mutate(r)
			_, err := rs.Create(ctx, r)
			if err == nil {
				t.Fatalf("expected rejection for %s", tc.name)
			}
			var inv store.ErrInvalid
			if !asErrInvalid(err, &inv) {
				t.Fatalf("expected ErrInvalid for %s, got %T %v", tc.name, err, err)
			}
			if !strings.Contains(strings.ToLower(err.Error()), tc.want) {
				t.Fatalf("for %s error %q does not mention %q", tc.name, err.Error(), tc.want)
			}
		})
	}
}

func asErrInvalid(err error, target *store.ErrInvalid) bool {
	if e, ok := err.(store.ErrInvalid); ok {
		*target = e
		return true
	}
	return false
}

// TestPersistenceAcrossRestart: rules written by one store instance survive a
// second store instance opening against the same Postgres, and quota
// semantics (algorithm, levels, dimensions) are identical after reload.
func TestPersistenceAcrossRestart(t *testing.T) {
	dsn := testboot.DSN()
	rdb := testboot.NewRedis(t, 11)
	ctx := context.Background()

	rs1, err := store.New(ctx, dsn, rdb)
	if err != nil {
		t.Fatal(err)
	}
	r := validRule()
	r.Name = "persist-me"
	r.Dimensions = []string{model.DimClient, model.DimAPI, model.DimGroup}
	r.Levels[model.LevelGlobal] = model.RuleLevel{Threshold: 10, Burst: 3}
	created, err := rs1.Create(ctx, r)
	if err != nil {
		t.Fatal(err)
	}

	// simulate gateway restart: brand-new store against same database
	rs2, err := store.New(ctx, dsn, rdb)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := rs2.Get(created.ID)
	if !ok {
		t.Fatal("rule must be present after restart")
	}
	if got.Algorithm != model.AlgoTokenBucket ||
		got.Levels[model.LevelGlobal].Burst != 3 ||
		got.Levels[model.LevelClient].Threshold != 10 ||
		len(got.Dimensions) != 3 {
		t.Fatalf("reloaded rule semantics changed: %+v", got)
	}
}

// TestUpdateAndDelete: illegal updates rejected, delete takes effect.
func TestUpdateAndDelete(t *testing.T) {
	_, rs, _, _ := testboot.FullStack(t)
	ctx := context.Background()
	r, err := rs.Create(ctx, validRule())
	if err != nil {
		t.Fatal(err)
	}
	r.Levels[model.LevelClient] = model.RuleLevel{Threshold: 0, Burst: 5}
	if _, err := rs.Update(ctx, r); err == nil {
		t.Fatal("update to non-positive threshold must be rejected")
	}
	if err := rs.Delete(ctx, r.ID); err != nil {
		t.Fatal(err)
	}
	if _, ok := rs.Get(r.ID); ok {
		t.Fatal("rule should be gone after delete")
	}
}
