package quota

import (
	"context"
	"strings"

	"github.com/redis/go-redis/v9"
)

// canarySegment is the marker matcher.Key inserts for the new version, and
// canonicalPrefix its absence. Both keys keep the same `{ruleID}` hash tag,
// so every RENAME below targets keys in one Redis cluster slot.
const canarySegment = "}:canary:"

// PromoteCounters folds the new-version counters into the canonical
// namespace when a rollout reaches 100%: rl:{id}:canary:level:... becomes
// rl:{id}:level:... for every live bucket. Keys that already exist at the
// target (the old counters from earlier traffic) are left untouched; the new
// version is authoritative after promotion, and any stale canary residue is
// deleted afterwards.
func (c *Checker) PromoteCounters(ctx context.Context, ruleID string) error {
	return c.scanCanary(ctx, ruleID, func(k string) error {
		target := strings.Replace(k, canarySegment, "}:", 1)
		exists, err := c.rdb.Exists(ctx, target).Result()
		if err != nil {
			return err
		}
		if exists == 0 {
			if err := c.rdb.Rename(ctx, k, target).Err(); err != nil {
				return err
			}
		} else {
			// Both sides hold state (e.g. independent global buckets): keep
			// the canonical old-namespace value and drop the canary copy.
			if err := c.rdb.Del(ctx, k).Err(); err != nil {
				return err
			}
		}
		return nil
	})
}

// AbortCounters deletes every new-version counter when a rollout is aborted,
// so the revoked version's state can never influence a later decision.
func (c *Checker) AbortCounters(ctx context.Context, ruleID string) error {
	return c.scanCanary(ctx, ruleID, func(k string) error {
		return c.rdb.Del(ctx, k).Err()
	})
}

func (c *Checker) scanCanary(ctx context.Context, ruleID string, fn func(key string) error) error {
	pattern := "rl:{" + ruleID + "}:canary:*"
	var cursor uint64
	for {
		keys, next, err := c.rdb.Scan(ctx, cursor, pattern, 200).Result()
		if err != nil {
			return err
		}
		for _, k := range keys {
			if err := fn(k); err != nil {
				// RENAME on a vanished key is harmless during teardown.
				if err != redis.Nil {
					continue
				}
			}
		}
		cursor = next
		if cursor == 0 {
			return nil
		}
	}
}
