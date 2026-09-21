// Package quota combines multi-dimensional rule matching with the four-level
// quota cascade. A request is checked global -> group -> api -> client for
// every matching rule; the first level that denies wins. The fixed order and
// the atomic per-bucket Redis scripts make the verdict deterministic and
// reproducible even when several levels approach their limits together.
package quota

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

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
	Levels   []LevelResult `json:"levels"`
	// DenyLevel / Reason identify exactly which quota stopped the request.
	DenyLevel model.Level `json:"deny_level,omitempty"`
	Reason    string      `json:"reason,omitempty"`
}

// Verdict is the gateway's answer for one inbound request.
type Verdict struct {
	Allowed  bool         `json:"allowed"`
	RuleID   string       `json:"rule_id,omitempty"`
	RuleName string       `json:"rule_name,omitempty"`
	Level    model.Level  `json:"level,omitempty"`
	Reason   string       `json:"reason,omitempty"`
	Results  []RuleResult `json:"results"`
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
	return &Checker{rules: rules, engines: engines, rdb: rdb, rec: rec, match: matcher.New()}
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
		if !rule.Enabled || !c.match.Matches(rule, rctx) {
			continue
		}
		rr := RuleResult{RuleID: rule.ID, RuleName: rule.Name, Allowed: true}

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
			cfg, ok := rule.Levels[lv]
			if !ok {
				continue
			}
			plan = append(plan, planned{
				lv: lv, cfg: cfg, key: c.match.Key(rule, lv, rctx),
			})
		}

		specs := make([]engine.KeySpec, len(plan))
		for i, p := range plan {
			specs[i] = engine.KeySpec{Key: p.key, Cfg: p.cfg}
		}
		mr, err := c.engines.TryMulti(ctx, rule.Algorithm, specs)
		if err != nil {
			return Verdict{}, fmt.Errorf("multi engine %s: %w", rule.Algorithm, err)
		}

		for i, p := range plan {
			d := mr.PerKey[i]
			rr.Levels = append(rr.Levels, LevelResult{
				Level: p.lv, Allowed: d.Allowed, Remaining: d.Remaining,
				ResetInMs: d.ResetInMs, ReleaseInMs: releaseDelay(rule.Algorithm, d), Key: p.key,
			})
		}

		if !mr.Allowed {
			denyPlan := plan[mr.DenyIndex]
			denyDecision := mr.PerKey[mr.DenyIndex]
			rr.Allowed = false
			rr.DenyLevel = denyPlan.lv
			rr.Reason = fmt.Sprintf("rule %q level %s quota exceeded (limit=%d)",
				rule.Name, denyPlan.lv, displayLimit(rule.Algorithm, denyPlan.cfg))

			stats.Tick(pipe, rule.ID, false, now)
			verdict.Results = append(verdict.Results, rr)

			if verdict.Allowed {
				verdict.Allowed = false
				verdict.RuleID = rule.ID
				verdict.RuleName = rule.Name
				verdict.Level = rr.DenyLevel
				verdict.Reason = rr.Reason
			}
			ev := stats.DenyEvent{
				TimeMs:    now.UnixMilli(),
				RuleID:    rule.ID,
				RuleName:  rule.Name,
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
			if rel := releaseDelay(rule.Algorithm, d); rel > maxReleaseMs {
				maxReleaseMs = rel
			}
		}
		releaseAt := now.Add(time.Duration(maxReleaseMs) * time.Millisecond)
		stats.Tick(pipe, rule.ID, true, releaseAt)
		verdict.Results = append(verdict.Results, rr)
	}

	if _, err := pipe.Exec(ctx); err != nil {
		return Verdict{}, err
	}
	return verdict, nil
}

func displayLimit(algo model.Algorithm, cfg model.RuleLevel) int64 {
	switch algo {
	case model.AlgoTokenBucket, model.AlgoLeakyBucket:
		return cfg.Burst
	default:
		return cfg.Threshold
	}
}

// releaseDelay extracts a paced release delay only for the leaky bucket; the
// other algorithms release immediately.
func releaseDelay(algo model.Algorithm, d engine.Decision) int64 {
	if algo == model.AlgoLeakyBucket && d.Allowed {
		return d.ResetInMs
	}
	return 0
}

// PeekRule reads current remaining allowance without consuming anything.
// The client key is derived from the supplied request context.
func (c *Checker) PeekRule(ctx context.Context, rule *model.Rule, rctx model.RequestContext) []LevelResult {
	out := []LevelResult{}
	for _, lv := range levelOrder {
		cfg, ok := rule.Levels[lv]
		if !ok {
			continue
		}
		key := c.match.Key(rule, lv, rctx)
		d, err := c.engines.Peek(ctx, rule.Algorithm, key, cfg)
		if err != nil {
			continue
		}
		out = append(out, LevelResult{
			Level: lv, Allowed: d.Allowed, Remaining: d.Remaining,
			ResetInMs: d.ResetInMs, ReleaseInMs: releaseDelay(rule.Algorithm, d), Key: key,
		})
	}
	return out
}
