// Package stats records per-second allow/deny counters and the most recent
// rejected requests for the live dashboard. Counters live in Redis so every
// gateway instance contributes to the same curves.
package stats

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	denyLogLen = 50
	tsTTL      = 2 * time.Hour
)

// DenyEvent is one rejected request, surfaced in the dashboard.
type DenyEvent struct {
	TimeMs    int64  `json:"time_ms"`
	RuleID    string `json:"rule_id"`
	RuleName  string `json:"rule_name"`
	Level     string `json:"level"`
	Reason    string `json:"reason"`
	ClientID  string `json:"client_id"`
	APIPath   string `json:"api_path"`
	Group     string `json:"group"`
	Remaining int64  `json:"remaining"`
	// Version is "stable" or "canary": which rule version rejected the
	// request during a gray rollout. Empty for the pre-rollout aggregate.
	Version string `json:"version,omitempty"`
}

// Version labels used in Redis keys and on the wire.
const (
	VersionStable = "stable"
	VersionCanary = "canary"
)

// Point holds the outcomes recorded in one second bucket.
type Point struct {
	Ts    int64 `json:"ts"`
	Allow int64 `json:"allow"`
	Deny  int64 `json:"deny"`
}

type Recorder struct {
	rdb *redis.Client
}

func New(rdb *redis.Client) *Recorder { return &Recorder{rdb: rdb} }

func tsKey(ruleID, version, outcome string) string {
	return fmt.Sprintf("stat:{%s}:%s:%s", ruleID, version, outcome)
}
func denyKey(ruleID, version string) string {
	return fmt.Sprintf("deny:{%s}:%s", ruleID, version)
}

func outcomeKey(ruleID, version string, allowed bool) string {
	if allowed {
		return tsKey(ruleID, version, "allow")
	}
	return tsKey(ruleID, version, "deny")
}

// Tick records one evaluated request for a rule version. The timestamp is
// supplied by the caller: for the leaky bucket it is the request's paced
// release time so the allow curve reflects shaped output rather than arrival.
// Stable and canary series are kept in separate keys so the dashboard can draw
// the two versions' curves independently. Call in a pipeline to avoid extra
// round trips.
func Tick(pipe redis.Pipeliner, ruleID, version string, allowed bool, at time.Time) {
	sec := strconv.FormatInt(at.Unix(), 10)
	k1 := outcomeKey(ruleID, version, allowed)
	k2 := outcomeKey("_all", VersionStable, allowed)
	pipe.ZIncrBy(context.Background(), k1, 1, sec)
	pipe.ZIncrBy(context.Background(), k2, 1, sec)
	// Refresh retention so idle series keys are eventually reclaimed.
	pipe.Expire(context.Background(), k1, tsTTL)
	pipe.Expire(context.Background(), k2, tsTTL)
}

// ExpireTS sets retention on the time-series keys (best effort).
func (r *Recorder) ExpireTS(ctx context.Context, ruleID, version string) {
	for _, k := range []string{tsKey(ruleID, version, "allow"), tsKey(ruleID, version, "deny")} {
		_ = r.rdb.Expire(ctx, k, tsTTL).Err()
	}
}

// PushDeny appends a rejection to the rule version's capped log.
func (r *Recorder) PushDeny(ctx context.Context, e DenyEvent) error {
	raw, err := json.Marshal(e)
	if err != nil {
		return err
	}
	version := e.Version
	if version == "" {
		version = VersionStable
	}
	k := denyKey(e.RuleID, version)
	pipe := r.rdb.TxPipeline()
	pipe.LPush(ctx, k, raw)
	pipe.LTrim(ctx, k, 0, denyLogLen-1)
	pipe.Expire(ctx, k, tsTTL)
	_, err = pipe.Exec(ctx)
	return err
}

// TimeSeries returns the last `seconds` seconds of allow/deny counts for one
// rule version, zero-filling seconds with no events. Pass version "" to read
// the stable (pre-rollout) series; ruleID "" reads the global aggregate.
func (r *Recorder) TimeSeries(ctx context.Context, ruleID, version string, seconds int64) ([]Point, error) {
	if version == "" {
		version = VersionStable
	}
	if ruleID == "" {
		ruleID = "_all"
	}
	now := time.Now().Unix()
	cutoff := now - seconds + 1

	allow, err := r.rangeCounts(ctx, tsKey(ruleID, version, "allow"), cutoff)
	if err != nil {
		return nil, err
	}
	deny, err := r.rangeCounts(ctx, tsKey(ruleID, version, "deny"), cutoff)
	if err != nil {
		return nil, err
	}

	out := make([]Point, 0, seconds)
	for ts := cutoff; ts <= now; ts++ {
		out = append(out, Point{Ts: ts, Allow: allow[ts], Deny: deny[ts]})
	}
	return out, nil
}

func (r *Recorder) rangeCounts(ctx context.Context, key string, cutoff int64) (map[int64]int64, error) {
	// Storage is one ZSET member per second: member name = epoch second and
	// that member's score = the count in that second. So we read every member
	// (no score filter) and select seconds >= cutoff ourselves.
	vals, err := r.rdb.ZRangeWithScores(ctx, key, 0, -1).Result()
	if err != nil {
		return nil, err
	}
	out := make(map[int64]int64, len(vals))
	for _, z := range vals {
		ts, _ := strconv.ParseInt(z.Member.(string), 10, 64)
		if ts < cutoff {
			continue
		}
		out[ts] = int64(z.Score)
	}
	return out, nil
}

// RecentDenies returns the newest rejection events for a rule (or all rules).
// version "" reads both stable and canary logs; pass stats.VersionCanary etc.
// to narrow to one rule version.
func (r *Recorder) RecentDenies(ctx context.Context, ruleID, version string, limit int64) ([]DenyEvent, error) {
	if limit <= 0 || limit > denyLogLen {
		limit = denyLogLen
	}
	if ruleID != "" {
		if version != "" {
			return r.popDenies(ctx, denyKey(ruleID, version), limit)
		}
		var all []DenyEvent
		for _, v := range []string{VersionStable, VersionCanary} {
			evs, err := r.popDenies(ctx, denyKey(ruleID, v), limit)
			if err != nil {
				return nil, err
			}
			all = append(all, evs...)
		}
		return sortAndCap(all, limit), nil
	}
	var keys []string
	var cursor uint64
	for {
		batch, next, err := r.rdb.Scan(ctx, cursor, "deny:{*}:*", 200).Result()
		if err != nil {
			return nil, err
		}
		keys = append(keys, batch...)
		cursor = next
		if cursor == 0 {
			break
		}
	}
	var all []DenyEvent
	for _, k := range keys {
		if version != "" && !strings.HasSuffix(k, ":"+version) {
			continue
		}
		evs, err := r.popDenies(ctx, k, limit)
		if err != nil {
			return nil, err
		}
		all = append(all, evs...)
	}
	return sortAndCap(all, limit), nil
}

func sortAndCap(all []DenyEvent, limit int64) []DenyEvent {
	for i := 1; i < len(all); i++ {
		for j := i; j > 0 && all[j-1].TimeMs < all[j].TimeMs; j-- {
			all[j-1], all[j] = all[j], all[j-1]
		}
	}
	if int64(len(all)) > limit {
		all = all[:limit]
	}
	return all
}

func (r *Recorder) popDenies(ctx context.Context, key string, limit int64) ([]DenyEvent, error) {
	raws, err := r.rdb.LRange(ctx, key, 0, limit-1).Result()
	if err != nil {
		return nil, err
	}
	out := make([]DenyEvent, 0, len(raws))
	for _, raw := range raws {
		var e DenyEvent
		if err := json.NewDecoder(strings.NewReader(raw)).Decode(&e); err == nil {
			out = append(out, e)
		}
	}
	return out, nil
}
