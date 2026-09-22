package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"ratelimit-gateway/internal/canary"
	"ratelimit-gateway/internal/model"
)

const rolloutSchemaSQL = `
CREATE TABLE IF NOT EXISTS rule_rollouts (
	rule_id     TEXT PRIMARY KEY REFERENCES rules(id) ON DELETE CASCADE,
	percent     INTEGER NOT NULL,
	canary      JSONB NOT NULL,
	old         JSONB NOT NULL,
	created_at  TIMESTAMPTZ NOT NULL,
	updated_at  TIMESTAMPTZ NOT NULL
);
`

const rolloutPubChannel = "rollouts:changed"

// dbExec is satisfied by both *pgxpool.Pool and pgx.Tx, so reads/writes can
// run directly or inside a transaction.
type dbExec interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// RolloutStore persists in-flight gray rollouts in PostgreSQL and keeps a hot
// in-memory copy on every gateway instance, hot-synced over Redis pub/sub — the
// same mechanism RuleStore uses. A restarted or freshly joined instance loads
// the exact (rule, percent, old, new) tuples and keeps sharding traffic at the
// pre-restart percent.
type RolloutStore struct {
	pool *pgxpool.Pool
	rdb  *redis.Client

	mu       sync.RWMutex
	rollouts map[string]*canary.Rollout
}

// NewRolloutStore migrates its schema and loads all in-flight rollouts. It
// shares the RuleStore's pool and Redis client.
func NewRolloutStore(ctx context.Context, pool *pgxpool.Pool, rdb *redis.Client) (*RolloutStore, error) {
	s := &RolloutStore{pool: pool, rdb: rdb, rollouts: map[string]*canary.Rollout{}}
	if _, err := s.pool.Exec(ctx, rolloutSchemaSQL); err != nil {
		return nil, fmt.Errorf("migrate rollout schema: %w", err)
	}
	if err := s.loadAll(ctx); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *RolloutStore) loadAll(ctx context.Context) error {
	rows, err := s.pool.Query(ctx,
		`SELECT rule_id, percent, canary, old, created_at, updated_at FROM rule_rollouts`)
	if err != nil {
		return err
	}
	defer rows.Close()

	fresh := map[string]*canary.Rollout{}
	for rows.Next() {
		ro, err := scanRollout(rows)
		if err != nil {
			return err
		}
		fresh[ro.RuleID] = ro
	}
	if err := rows.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	s.rollouts = fresh
	s.mu.Unlock()
	return nil
}

func scanRollout(sc pgx.Row) (*canary.Rollout, error) {
	ro := &canary.Rollout{}
	var canaryJSON, oldJSON []byte
	if err := sc.Scan(&ro.RuleID, &ro.Percent, &canaryJSON, &oldJSON, &ro.CreatedAt, &ro.UpdatedAt); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(canaryJSON, &ro.Canary); err != nil {
		return nil, fmt.Errorf("decode canary rule: %w", err)
	}
	if err := json.Unmarshal(oldJSON, &ro.Old); err != nil {
		return nil, fmt.Errorf("decode old rule: %w", err)
	}
	return ro, nil
}

// Get returns a copy of one rollout (and false when none is in flight).
func (s *RolloutStore) Get(ruleID string) (*canary.Rollout, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ro, ok := s.rollouts[ruleID]
	if !ok {
		return nil, false
	}
	cp := *ro
	return &cp, true
}

// List returns copies of every in-flight rollout sorted by rule id.
func (s *RolloutStore) List() []*canary.Rollout {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*canary.Rollout, 0, len(s.rollouts))
	for _, ro := range s.rollouts {
		cp := *ro
		out = append(out, &cp)
	}
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j-1].RuleID > out[j].RuleID; j-- {
			out[j-1], out[j] = out[j], out[j-1]
		}
	}
	return out
}

// Start validates a brand-new rollout for a rule. old is the rule's current
// content (snapshotted as the stable version); cnry is the operator's new
// content. A percent of 0 starts the rollout parked: nothing changes until the
// percent is raised.
func (s *RolloutStore) Start(ctx context.Context, old, cnry *model.Rule, percent int) (*canary.Rollout, error) {
	if old == nil || cnry == nil {
		return nil, ErrInvalid{errors.New("rollout requires both old and new rule content")}
	}
	if _, exists := s.Get(old.ID); exists {
		return nil, ErrInvalid{fmt.Errorf("rule %s already has an in-flight rollout; abort or promote it first", old.ID)}
	}
	now := time.Now().UTC()
	ro := &canary.Rollout{
		RuleID:    old.ID,
		Percent:   percent,
		Canary:    *withID(cnry, old.ID),
		Old:       *withID(old, old.ID),
		CreatedAt: now,
		UpdatedAt: now,
	}
	if err := ro.Validate(); err != nil {
		return nil, ErrInvalid{err}
	}
	if err := s.upsert(ctx, ro); err != nil {
		return nil, err
	}
	if err := s.cacheAndPublish(ctx, ro); err != nil {
		return nil, err
	}
	return ro, nil
}

// SetPercent adjusts an existing rollout (10 -> 50 -> 100, or back to 0).
// Raising the percent only moves more subjects onto the canary; setting 0
// parks the change so all traffic is judged by the old version again while the
// canary content is retained.
func (s *RolloutStore) SetPercent(ctx context.Context, ruleID string, percent int) (*canary.Rollout, error) {
	ro, ok := s.Get(ruleID)
	if !ok {
		return nil, ErrNotFound{fmt.Errorf("no in-flight rollout for rule %s", ruleID)}
	}
	if err := canary.ValidatePercent(percent); err != nil {
		return nil, ErrInvalid{err}
	}
	ro.Percent = percent
	ro.UpdatedAt = time.Now().UTC()
	if err := s.upsert(ctx, ro); err != nil {
		return nil, err
	}
	if err := s.cacheAndPublish(ctx, ro); err != nil {
		return nil, err
	}
	return ro, nil
}

// UpdateCanary replaces the canary content of an in-flight rollout, keeping the
// current percent. Used when an operator edits the new rule while observing it.
func (s *RolloutStore) UpdateCanary(ctx context.Context, ruleID string, cnry *model.Rule) (*canary.Rollout, error) {
	ro, ok := s.Get(ruleID)
	if !ok {
		return nil, ErrNotFound{fmt.Errorf("no in-flight rollout for rule %s", ruleID)}
	}
	ro.Canary = *withID(cnry, ruleID)
	ro.UpdatedAt = time.Now().UTC()
	if err := ro.Validate(); err != nil {
		return nil, ErrInvalid{err}
	}
	if err := s.upsert(ctx, ro); err != nil {
		return nil, err
	}
	if err := s.cacheAndPublish(ctx, ro); err != nil {
		return nil, err
	}
	return ro, nil
}

// Promote finishes a rollout at 100%: the canary content becomes the single
// live rule and the rollout disappears. It runs in one transaction against
// rules + rule_rollouts, then hot-reloads every instance for both tables.
func (s *RolloutStore) Promote(ctx context.Context, rules *RuleStore, ruleID string) (*model.Rule, error) {
	ro, ok := s.Get(ruleID)
	if !ok {
		return nil, ErrNotFound{fmt.Errorf("no in-flight rollout for rule %s", ruleID)}
	}
	if ro.Percent < canary.MaxPercent {
		return nil, ErrInvalid{fmt.Errorf("can only promote at 100%%, current percent is %d", ro.Percent)}
	}
	newRule := ro.Canary
	newRule.ID = ruleID
	newRule.UpdatedAt = time.Now().UTC()
	if err := newRule.Validate(); err != nil {
		return nil, ErrInvalid{fmt.Errorf("canary rule is invalid: %w", err)}
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := upsertRuleExec(ctx, tx, &newRule); err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM rule_rollouts WHERE rule_id = $1`, ruleID); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}

	// Update both hot caches locally, then fan reloads out to every instance.
	rules.replaceCache(&newRule)
	s.deleteCache(ruleID)
	pipe := s.rdb.Pipeline()
	pipe.Publish(ctx, pubChannel, "reload")
	pipe.Publish(ctx, rolloutPubChannel, "reload")
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, err
	}
	return &newRule, nil
}

// Abort ends a rollout immediately: the row is deleted and every instance
// reverts to judging all traffic with the old (live) rule. The canary's
// counters linger unused in Redis but can never influence a decision again.
func (s *RolloutStore) Abort(ctx context.Context, ruleID string) error {
	if _, ok := s.Get(ruleID); !ok {
		return ErrNotFound{fmt.Errorf("no in-flight rollout for rule %s", ruleID)}
	}
	if _, err := s.pool.Exec(ctx, `DELETE FROM rule_rollouts WHERE rule_id = $1`, ruleID); err != nil {
		return err
	}
	s.deleteCache(ruleID)
	return s.rdb.Publish(ctx, rolloutPubChannel, "reload").Err()
}

func (s *RolloutStore) upsert(ctx context.Context, ro *canary.Rollout) error {
	canaryJSON, _ := json.Marshal(ro.Canary)
	oldJSON, _ := json.Marshal(ro.Old)
	_, err := s.pool.Exec(ctx, `
		INSERT INTO rule_rollouts (rule_id, percent, canary, old, created_at, updated_at)
		VALUES ($1,$2,$3,$4,$5,$6)
		ON CONFLICT (rule_id) DO UPDATE SET
		  percent=EXCLUDED.percent, canary=EXCLUDED.canary, old=EXCLUDED.old,
		  updated_at=EXCLUDED.updated_at`,
		ro.RuleID, ro.Percent, canaryJSON, oldJSON, ro.CreatedAt, ro.UpdatedAt)
	return err
}

func (s *RolloutStore) cacheAndPublish(ctx context.Context, ro *canary.Rollout) error {
	cp := *ro
	s.mu.Lock()
	s.rollouts[ro.RuleID] = &cp
	s.mu.Unlock()
	return s.rdb.Publish(ctx, rolloutPubChannel, "reload").Err()
}

func (s *RolloutStore) deleteCache(ruleID string) {
	s.mu.Lock()
	delete(s.rollouts, ruleID)
	s.mu.Unlock()
}

// SubscribeHotReload blocks, reloading rollouts whenever any instance changes
// one — the same hot-sync mechanism rules use.
func (s *RolloutStore) SubscribeHotReload(ctx context.Context) {
	sub := s.rdb.Subscribe(ctx, rolloutPubChannel)
	defer sub.Close()
	ch := sub.Channel()
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			if msg.Payload == "reload" {
				_ = s.loadAll(ctx)
			}
		}
	}
}

func withID(r *model.Rule, id string) *model.Rule {
	cp := *r
	cp.ID = id
	return &cp
}
