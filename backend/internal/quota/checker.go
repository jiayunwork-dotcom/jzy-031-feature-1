// Package quota combines multi-dimensional rule matching with the four-level
// quota cascade. A request is checked global -> group -> api -> client for
// every matching rule; the first level that denies wins. The fixed order and
// the atomic per-bucket Redis scripts make the verdict deterministic and
// reproducible even when several levels approach their limits together.
//
// When a gray rollout is in flight for a rule, the canary layer first assigns
// the request deterministically to one of the two versions; only that
// version's content is evaluated, against that version's own, fully isolated
// quota counters.
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
	Levels   []LevelResult `json:"levels"`
	// Version reports which rule content judged this request during a gray
	// rollout: "stable" (old) or "canary" (new). Empty when no rollout.
	Version string `json:"version,omitempty"`
	// DenyLevel / Reason identify exactly which quota stopped the request.
	DenyLevel model.Level `json:"deny_level,omitempty"`
	Reason    string      `json:"reason,omitempty"`
}

// Verdict is the gateway's answer for one inbound request.
type Verdict struct {
	Allowed  bool        `json:"allowed"`
	RuleID   string      `json:"rule_id,omitempty"`
	RuleName string      `json:"rule_name,omitempty"`
	Level    model.Level `json:"level,omitempty"`
	// Version is the version of the rule that produced the overall verdict.
	Version string       `json:"version,omitempty"`
	Reason  string       `json:"reason,omitempty"`
	Results []RuleResult `json:"results"`
}

// Checker evaluates requests against all live rules.
type Checker struct {
	rules    *store.RuleStore
	rollouts *store.RolloutStore
	engines  *engine.Manager
	rdb      *redis.Client
	rec      *stats.Recorder
	match    *matcher.Matcher
}

func New(rules *store.RuleStore, rollouts *store.RolloutStore,
	engines *engine.Manager, rdb *redis.Client, rec *stats.Recorder) *Checker {
	return &Checker{
		rules: rules, rollouts: rollouts, engines: engines, rdb: rdb,
		rec: rec, match: matcher.New(),
	}
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

	for _, live := range rules {
		if !live.Enabled {
			continue
		}

		// Gray rollout resolution: deterministically pick the version this
		// subject belongs to, then evaluate only that version's content. With
		// no rollout (or percent == 0) every request stays on the live rule.
		evalRule := live
		version := ""
		keyVersion := ""
		if c.rollouts != nil {
			if ro, ok := c.rollouts.Get(live.ID); ok && ro.Active() {
				subject := canary.Subject(&ro.Old, rctx)
				if canary.SelectVersion(ro.RuleID, subject, ro.Percent) == canary.VersionCanary {
					evalRule = &ro.Canary
					version = stats.VersionCanary
					keyVersion = string(canary.VersionCanary)
				} else {
					evalRule = &ro.Old
					version = stats.VersionStable
				}
			}
		}

		if !c.match.Matches(evalRule, rctx) {
			continue
		}

		rr, engineErr := c.evaluateRule(ctx, evalRule, keyVersion, rctx, now)
		if engineErr != nil {
			return Verdict{}, engineErr
		}
		rr.result.Version = version
		stats.Tick(pipe, evalRule.ID, versionLabel(version), !rr.denied, rr.statsAt)
		verdict.Results = append(verdict.Results, rr.result)
		// Surface the version of the first matching rule on the verdict itself
		// so an allow response still reveals which side judged the request.
		if verdict.RuleID == "" {
			verdict.RuleID = rr.result.RuleID
			verdict.RuleName = rr.result.RuleName
			verdict.Version = version
		}

		if rr.denied {
			if verdict.Allowed {
				verdict.Allowed = false
				verdict.RuleID = rr.result.RuleID
				verdict.RuleName = rr.result.RuleName
				verdict.Level = rr.result.DenyLevel
				verdict.Version = version
				verdict.Reason = rr.result.Reason
			}
			ev := stats.DenyEvent{
				TimeMs:    now.UnixMilli(),
				RuleID:    evalRule.ID,
				RuleName:  evalRule.Name,
				Level:     string(rr.result.DenyLevel),
				Reason:    rr.result.Reason,
				ClientID:  rctx.ClientID,
				APIPath:   rctx.APIPath,
				Group:     rctx.Group,
				Remaining: rr.denyRemaining,
				Version:   versionLabel(version),
			}
			_ = c.rec.PushDeny(ctx, ev)
		}
	}

	if _, err := pipe.Exec(ctx); err != nil {
		return Verdict{}, err
	}
	return verdict, nil
}

// versionLabel maps the internal ("" / canary) marker to the stats label.
func versionLabel(version string) string {
	if version == stats.VersionCanary {
		return stats.VersionCanary
	}
	return stats.VersionStable
}

type ruleEval struct {
	result        RuleResult
	denied        bool
	denyRemaining int64
	statsAt       time.Time
}

// evaluateRule runs the atomic four-level cascade for ONE version of a rule.
// keyVersion is "" for the stable (historical) counters and "canary" for the
// isolated new-version counters, so the two versions never share a bucket.
func (c *Checker) evaluateRule(ctx context.Context,
	rule *model.Rule, keyVersion string, rctx model.RequestContext, now time.Time) (ruleEval, error) {

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
			lv: lv, cfg: cfg, key: c.match.VersionedKey(rule, lv, rctx, keyVersion),
		})
	}

	specs := make([]engine.KeySpec, len(plan))
	for i, p := range plan {
		specs[i] = engine.KeySpec{Key: p.key, Cfg: p.cfg}
	}
	mr, err := c.engines.TryMulti(ctx, rule.Algorithm, specs)
	if err != nil {
		return ruleEval{}, fmt.Errorf("multi engine %s: %w", rule.Algorithm, err)
	}

	for i, p := range plan {
		d := mr.PerKey[i]
		rr.Levels = append(rr.Levels, LevelResult{
			Level: p.lv, Allowed: d.Allowed, Remaining: d.Remaining,
			ResetInMs: d.ResetInMs, ReleaseInMs: releaseDelay(rule.Algorithm, d), Key: p.key,
		})
	}

	out := ruleEval{result: rr, statsAt: now}
	if !mr.Allowed {
		denyPlan := plan[mr.DenyIndex]
		denyDecision := mr.PerKey[mr.DenyIndex]
		out.denied = true
		out.denyRemaining = denyDecision.Remaining
		rr.Allowed = false
		rr.DenyLevel = denyPlan.lv
		rr.Reason = fmt.Sprintf("rule %q level %s quota exceeded (limit=%d)",
			rule.Name, denyPlan.lv, displayLimit(rule.Algorithm, denyPlan.cfg))
		out.result = rr
		return out, nil
	}

	// Admitted: allow-statistics land at the slowest paced release
	// (leaky bucket) so the output curve is the shaped one.
	var maxReleaseMs int64
	for _, d := range mr.PerKey {
		if rel := releaseDelay(rule.Algorithm, d); rel > maxReleaseMs {
			maxReleaseMs = rel
		}
	}
	out.statsAt = now.Add(time.Duration(maxReleaseMs) * time.Millisecond)
	out.result = rr
	return out, nil
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
// The client key is derived from the supplied request context. When a rollout
// is in flight it peeks BOTH versions, tagging each result set via the key
// prefix; callers that want one version use PeekRuleVersion.
func (c *Checker) PeekRule(ctx context.Context, rule *model.Rule, rctx model.RequestContext) []LevelResult {
	return c.peekVersion(ctx, rule, rctx, "")
}

// PeekRuleVersion peeks one specific version of a rule without consuming:
// version "" = stable/historical counters, "canary" = new-version counters.
func (c *Checker) PeekRuleVersion(ctx context.Context, rule *model.Rule,
	rctx model.RequestContext, version string) []LevelResult {
	return c.peekVersion(ctx, rule, rctx, version)
}

func (c *Checker) peekVersion(ctx context.Context, rule *model.Rule,
	rctx model.RequestContext, keyVersion string) []LevelResult {
	out := []LevelResult{}
	for _, lv := range levelOrder {
		cfg, ok := rule.Levels[lv]
		if !ok {
			continue
		}
		key := c.match.VersionedKey(rule, lv, rctx, keyVersion)
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
