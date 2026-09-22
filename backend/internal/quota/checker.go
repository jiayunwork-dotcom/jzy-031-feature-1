// Package quota combines multi-dimensional rule matching with the four-level
// quota cascade. A request is checked global -> group -> api -> client for
// every matching rule; the first level that denies wins. The fixed order and
// the atomic per-bucket Redis scripts make the verdict deterministic and
// reproducible even when several levels approach their limits together.
//
// Gray rollouts are handled at the boundary, not inside the cascade: for each
// rule the request is first deterministically assigned to the old or the new
// rule version (internal/canary), then the selected rule content flows
// through the unchanged matching + cascade path. The two versions' counters
// live in disjoint Redis key namespaces, so a saturated new-version client
// never touches the old-version client's remaining quota.
package quota

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"ratelimit-gateway/internal/canary"
	"ratelimit-gateway/internal/engine"
	"ratelimit-gateway/internal/matcher"
	"ratelimit-gateway/internal/model"
	"ratelimit-gateway/internal/stats"
	"ratelimit-gateway/internal/store"
)

// levelOrder is the single mandated evaluation order.
var levelOrder = []model.Level{model.LevelGlobal, model.LevelGroup, model.LevelAPI, model.LevelClient}

// LevelResult is the outcome for one level of one rule.
type LevelResult struct {
	Level       model.Level `json:"level"`
	Allowed     bool        `json:"allowed"`
	Remaining   int64       `json:"remaining"`
	ResetInMs   int64       `json:"reset_in_ms"`
	ReleaseInMs int64       `json:"release_in_ms,omitempty"`
	Key         string      `json:"key"`
}

// RuleResult collects every level decision for a matching rule.
type RuleResult struct {
	RuleID   string        `json:"rule_id"`
	RuleName string        `json:"rule_name"`
	Allowed  bool          `json:"allowed"`
	// Version is "old" or "new" while the rule is gray-released and empty
	// for an ordinary single-version rule.
	Version canary.Version `json:"version,omitempty"`
	Levels  []LevelResult  `json:"levels"`
	// DenyLevel / Reason identify exactly which quota stopped the request.
	DenyLevel model.Level `json:"deny_level,omitempty"`
	Reason    string      `json:"reason,omitempty"`
}

// Verdict is the gateway's answer for one inbound request.
type Verdict struct {
	Allowed bool `json:"allowed"`
	// Version identifies which version of the first denying rule stopped the
	// request; on an allowed request it is left empty (per-rule versions are
	// still visible in Results).
	Version  canary.Version `json:"version,omitempty"`
	RuleID   string         `json:"rule_id,omitempty"`
	RuleName string         `json:"rule_name,omitempty"`
	Level    model.Level    `json:"level,omitempty"`
	Reason   string         `json:"reason,omitempty"`
	Results  []RuleResult   `json:"results"`
}

// Checker evaluates requests against all live rules.
type Checker struct {
	rules   *store.RuleStore
	engines *engine.Manager
	rdb     *redis.Client
	rec     *stats.Recorder
	match   *matcher.Matcher
}

func New(rules *store.RuleStore, engines *engine.Manager, rdb *redis.Client, rec *stats.Recorder) *Checker {
	c := &Checker{rules: rules, engines: engines, rdb: rdb, rec: rec, match: matcher.New()}
	// Ending a rollout folds/discards the canary counter namespace.
	rules.SetCounterMigrator(c)
	return c
}

// effectiveRule picks the rule version a request must be judged by. The
// assignment is a pure function of (rule, rollout percent, subject), so the
// same subject lands on the same version on every instance and stays there
// while the percentage is unchanged. A rule without a rollout returns itself
// and an empty version — no phantom "new version" can ever participate.
func (c *Checker) effectiveRule(rule *model.Rule, rctx model.RequestContext) (*model.Rule, canary.Version) {
	rl, ok := c.rules.Rollout(rule.ID)
	if !ok {
		return rule, ""
	}
	if canary.SelectVersion(rule, rl.Percent, rctx) == canary.VersionNew {
		return &rl.NewRule, canary.VersionNew
	}
	return rule, canary.VersionOld
}

// Decide runs the full cascade. Consumed tokens stay consumed only on
// success; a rejecting level leaves lower levels untouched (short-circuit).
func (c *Checker) Decide(ctx context.Context, rctx model.RequestContext) (Verdict, error) {
	now := time.Now()
	verdict := Verdict{Allowed: true}
	rules := c.rules.List()

	// Pipeline the stats writes for every evaluation; commands themselves run
	// atomically per key inside Lua, the pipeline only batches bookkeeping.
	pipe := c.rdb.Pipeline()

	for _, rule := range rules {
		if !rule.Enabled {
			continue
		}
		// Version selection happens BEFORE matching: the bucketing is keyed
		// on the stable rule's subject dimensions, then the chosen version's
		// own content decides whether the request matches.
		eff, version := c.effectiveRule(rule, rctx)
		if !eff.Enabled || !c.match.Matches(eff, rctx) {
			continue
		}
		rr := RuleResult{RuleID: rule.ID, RuleName: eff.Name, Allowed: true, Version: version}

		// Atomic four-level cascade. Every present level global -> group ->
		// api -> client is evaluated in ONE Redis Lua script, so either all
		// levels consume or none do. The verdict is therefore independent of
		// evaluation order (all states are observed and consumed in one
		// atomic step) and concurrent gateway instances can never observe the
		// same spare slots and double-admit.
		type planned struct {
			lv  model.Level
			cfg model.RuleLevel
			key string
		}
		var plan []planned
		for _, lv := range levelOrder {
			cfg, ok := eff.Levels[lv]
			if !ok {
				continue
			}
			plan = append(plan, planned{
				lv: lv, cfg: cfg, key: c.match.Key(eff, version, lv, rctx),
			})
		}

		specs := make([]engine.KeySpec, len(plan))
		for i, p := range plan {
			specs[i] = engine.KeySpec{Key: p.key, Cfg: p.cfg}
		}
		mr, err := c.engines.TryMulti(ctx, eff.Algorithm, specs)
		if err != nil {
			return Verdict{}, fmt.Errorf("multi engine %s: %w", eff.Algorithm, err)
		}

		for i, p := range plan {
			d := mr.PerKey[i]
			rr.Levels = append(rr.Levels, LevelResult{
				Level: p.lv, Allowed: d.Allowed, Remaining: d.Remaining,
				ResetInMs: d.ResetInMs, ReleaseInMs: releaseDelay(eff.Algorithm, d), Key: p.key,
			})
		}

		if !mr.Allowed {
			denyPlan := plan[mr.DenyIndex]
			denyDecision := mr.PerKey[mr.DenyIndex]
			rr.Allowed = false
			rr.DenyLevel = denyPlan.lv
			rr.Reason = fmt.Sprintf("rule %q %s level %s quota exceeded (limit=%d)",
				eff.Name, versionTag(version), denyPlan.lv, displayLimit(eff.Algorithm, denyPlan.cfg))

			stats.Tick(pipe, rule.ID, string(version), false, now)
			verdict.Results = append(verdict.Results, rr)

			if verdict.Allowed {
				verdict.Allowed = false
				verdict.Version = version
				verdict.RuleID = rule.ID
				verdict.RuleName = eff.Name
				verdict.Level = rr.DenyLevel
				verdict.Reason = rr.Reason
			}
			ev := stats.DenyEvent{
				TimeMs:    now.UnixMilli(),
				RuleID:    rule.ID,
				RuleName:  eff.Name,
				Version:   string(version),
				Level:     string(rr.DenyLevel),
				Reason:    rr.Reason,
				ClientID:  rctx.ClientID,
				APIPath:   rctx.APIPath,
				Group:     rctx.Group,
				Remaining: denyDecision.Remaining,
			}
			_ = c.rec.PushDeny(ctx, ev)
			continue
		}

		// Admitted: allow-statistics land at the slowest paced release
		// (leaky bucket) so the output curve is the shaped one.
		var maxReleaseMs int64
		for _, d := range mr.PerKey {
			if rel := releaseDelay(eff.Algorithm, d); rel > maxReleaseMs {
				maxReleaseMs = rel
			}
		}
		releaseAt := now.Add(time.Duration(maxReleaseMs) * time.Millisecond)
		stats.Tick(pipe, rule.ID, string(version), true, releaseAt)
		verdict.Results = append(verdict.Results, rr)
	}

	if _, err := pipe.Exec(ctx); err != nil {
		return Verdict{}, err
	}
	return verdict, nil
}

// releaseDelay extracts a paced release delay only for the leaky bucket; the
// other algorithms release immediately.
func releaseDelay(algo model.Algorithm, d engine.Decision) int64 {
	if algo == model.AlgoLeakyBucket && d.Allowed {
		return d.ResetInMs
	}
	return 0
}

func versionTag(v canary.Version) string {
	switch v {
	case canary.VersionNew:
		return "[new]"
	case canary.VersionOld:
		return "[old]"
	}
	return ""
}

func displayLimit(algo model.Algorithm, cfg model.RuleLevel) int64 {
	switch algo {
	case model.AlgoTokenBucket, model.AlgoLeakyBucket:
		return cfg.Burst
	default:
		return cfg.Threshold
	}
}

// PeekRule reads current remaining allowance without consuming anything.
// The client key is derived from the supplied request context. When the rule
// is gray-released the caller chooses which version to peek; the default
// (empty version) shows the old/canonical counters.
func (c *Checker) PeekRule(ctx context.Context, rule *model.Rule, version canary.Version, rctx model.RequestContext) []LevelResult {
	eff := rule
	if version == canary.VersionNew {
		if rl, ok := c.rules.Rollout(rule.ID); ok {
			eff = &rl.NewRule
		} else {
			// no rollout anymore: nothing new-version-specific to peek
			return nil
		}
	}
	out := []LevelResult{}
	for _, lv := range levelOrder {
		cfg, ok := eff.Levels[lv]
		if !ok {
			continue
		}
		key := c.match.Key(eff, version, lv, rctx)
		d, err := c.engines.Peek(ctx, eff.Algorithm, key, cfg)
		if err != nil {
			continue
		}
		out = append(out, LevelResult{
			Level: lv, Allowed: d.Allowed, Remaining: d.Remaining,
			ResetInMs: d.ResetInMs, ReleaseInMs: releaseDelay(eff.Algorithm, d), Key: key,
		})
	}
	return out
}
