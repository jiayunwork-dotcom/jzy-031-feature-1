// Package store persists rate-limit rules in PostgreSQL and keeps a hot
// in-memory copy on every gateway instance. Rule changes are published over
// Redis pub/sub so every instance reloads immediately: thresholds take effect
// without restarting any process.
//
// Gray rollouts live here too. While a rollout exists, the persisted rule
// stays the OLD version and a rollouts row carries the new version plus its
// percentage; both tables are loaded and hot-reloaded together. Promoting a
// rollout (100%) replaces the rule and drops the row, aborting it (0%) just
// drops the row — the old rule row was never touched, so abort is
// instantaneous and leaves no residue.
package store

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"ratelimit-gateway/internal/model"
)

const schemaSQL = `
CREATE TABLE IF NOT EXISTS rules (
	id          TEXT PRIMARY KEY,
	name        TEXT NOT NULL,
	enabled     BOOLEAN NOT NULL DEFAULT TRUE,
	algorithm   TEXT NOT NULL,
	dimensions  JSONB NOT NULL,
	levels      JSONB NOT NULL,
	matchers    JSONB NOT NULL DEFAULT '{}'::jsonb,
	updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS rollouts (
	rule_id     TEXT PRIMARY KEY REFERENCES rules(id) ON DELETE CASCADE,
	percent     INTEGER NOT NULL CHECK (percent BETWEEN 1 AND 99),
	new_rule    JSONB NOT NULL,
	started_at  TIMESTAMPTZ NOT NULL,
	updated_at  TIMESTAMPTZ NOT NULL
);
`

const pubChannel = "rules:changed"

// CounterMigrator moves/clears the new-version Redis counters when a rollout
// ends. It is implemented by the quota layer (which owns key naming) and
// injected so this package never depends on the decision layer. Failures are
// best-effort bookkeeping and never block the configuration change.
type CounterMigrator interface {
	// PromoteCounters renames every canary counter of the rule onto the
	// canonical (old-version) key.
	PromoteCounters(ctx context.Context, ruleID string) error
	// AbortCounters deletes every canary counter of the rule.
	AbortCounters(ctx context.Context, ruleID string) error
}

// RuleStore is the source of truth for rules and rollouts.
type RuleStore struct {
	pg  *pgxpool.Pool
	rdb *redis.Client

	mu       sync.RWMutex
	rules    map[string]*model.Rule
	rollouts map[string]*model.Rollout

	migrator CounterMigrator
}

// New opens the pool, migrates the schema and loads all rules.
func New(ctx context.Context, dsn string, rdb *redis.Client) (*RuleStore, error) {
	pg, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("connect postgres: %w", err)
	}
	if err := pg.Ping(ctx); err != nil {
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	if _, err := pg.Exec(ctx, schemaSQL); err != nil {
		return nil, fmt.Errorf("migrate schema: %w", err)
	}
	s := &RuleStore{
		pg: pg, rdb: rdb,
		rules:    map[string]*model.Rule{},
		rollouts: map[string]*model.Rollout{},
	}
	if err := s.loadAll(ctx); err != nil {
		return nil, err
	}
	return s, nil
}

// SetCounterMigrator wires the post-promotion/abort counter cleanup. It is
// set once at process startup once the decision layer exists.
func (s *RuleStore) SetCounterMigrator(m CounterMigrator) {
	s.mu.Lock()
	s.migrator = m
	s.mu.Unlock()
}

func (s *RuleStore) loadAll(ctx context.Context) error {
	rrows, err := s.pg.Query(ctx,
		`SELECT id, name, enabled, algorithm, dimensions, levels, matchers, updated_at FROM rules ORDER BY id`)
	if err != nil {
		return err
	}
	fresh := map[string]*model.Rule{}
	for rrows.Next() {
		r, err := scanRule(rrows)
		if err != nil {
			rrows.Close()
			return err
		}
		fresh[r.ID] = r
	}
	rrows.Close()
	if err := rrows.Err(); err != nil {
		return err
	}

	prows, err := s.pg.Query(ctx,
		`SELECT rule_id, percent, new_rule, started_at, updated_at FROM rollouts ORDER BY rule_id`)
	if err != nil {
		return err
	}
	freshRoll := map[string]*model.Rollout{}
	for prows.Next() {
		rl, err := scanRollout(prows)
		if err != nil {
			prows.Close()
			return err
		}
		freshRoll[rl.RuleID] = rl
	}
	prows.Close()
	if err := prows.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	s.rules = fresh
	s.rollouts = freshRoll
	s.mu.Unlock()
	return nil
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanRule(sc rowScanner) (*model.Rule, error) {
	r := &model.Rule{}
	var dimsJSON, levelsJSON, matchersJSON []byte
	if err := sc.Scan(&r.ID, &r.Name, &r.Enabled, &r.Algorithm,
		&dimsJSON, &levelsJSON, &matchersJSON, &r.UpdatedAt); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(dimsJSON, &r.Dimensions); err != nil {
		return nil, fmt.Errorf("decode dimensions: %w", err)
	}
	if err := json.Unmarshal(levelsJSON, &r.Levels); err != nil {
		return nil, fmt.Errorf("decode levels: %w", err)
	}
	if err := json.Unmarshal(matchersJSON, &r.Matchers); err != nil {
		return nil, fmt.Errorf("decode matchers: %w", err)
	}
	return r, nil
}

func scanRollout(sc rowScanner) (*model.Rollout, error) {
	rl := &model.Rollout{}
	var newRuleJSON []byte
	if err := sc.Scan(&rl.RuleID, &rl.Percent, &newRuleJSON, &rl.StartedAt, &rl.UpdatedAt); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(newRuleJSON, &rl.NewRule); err != nil {
		return nil, fmt.Errorf("decode canary new_rule: %w", err)
	}
	return rl, nil
}

// List returns every rule, sorted by id for determinism.
func (s *RuleStore) List() []*model.Rule {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*model.Rule, 0, len(s.rules))
	for _, r := range s.rules {
		cp := *r
		out = append(out, &cp)
	}
	// deterministic order for multi-rule evaluation
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j-1].ID > out[j].ID; j-- {
			out[j-1], out[j] = out[j], out[j-1]
		}
	}
	return out
}

// Get returns a copy of one rule (its OLD/persisted version).
func (s *RuleStore) Get(id string) (*model.Rule, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r, ok := s.rules[id]
	if !ok {
		return nil, false
	}
	cp := *r
	return &cp, true
}

// Rollout returns a copy of the rule's in-progress rollout, if any.
func (s *RuleStore) Rollout(ruleID string) (*model.Rollout, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rl, ok := s.rollouts[ruleID]
	if !ok {
		return nil, false
	}
	cp := *rl
	cp.NewRule = *cloneRule(&rl.NewRule)
	return &cp, true
}

// cloneRule returns a shallow-independent copy of r (slices/maps are copied
// so callers can never mutate cached state).
func cloneRule(r *model.Rule) *model.Rule {
	cp := *r
	cp.Dimensions = append([]string(nil), r.Dimensions...)
	if r.Matchers != nil {
		cp.Matchers = make(map[string][]string, len(r.Matchers))
		for k, v := range r.Matchers {
			cp.Matchers[k] = append([]string(nil), v...)
		}
	}
	cp.Levels = make(map[model.Level]model.RuleLevel, len(r.Levels))
	for k, v := range r.Levels {
		cp.Levels[k] = v
	}
	return &cp
}

// Create inserts and validates a new rule.
func (s *RuleStore) Create(ctx context.Context, r *model.Rule) (*model.Rule, error) {
	if err := r.Validate(); err != nil {
		return nil, ErrInvalid{err}
	}
	r.ID = uuid.NewString()
	r.UpdatedAt = time.Now().UTC()
	if err := s.upsert(ctx, r); err != nil {
		return nil, err
	}
	if err := s.cacheAndPublish(ctx, r); err != nil {
		return nil, err
	}
	return r, nil
}

// Update validates and replaces an existing rule and hot-reloads all
// instances. A rule with an in-progress rollout cannot be edited directly:
// the operator must promote or abort the rollout first, so the two versions
// can never silently diverge from the configured split.
func (s *RuleStore) Update(ctx context.Context, r *model.Rule) (*model.Rule, error) {
	if r.ID == "" {
		return nil, ErrInvalid{fmt.Errorf("rule id required for update")}
	}
	if _, ok := s.Get(r.ID); !ok {
		return nil, ErrNotFound{fmt.Errorf("rule %s not found", r.ID)}
	}
	if _, ok := s.Rollout(r.ID); ok {
		return nil, ErrConflict{fmt.Errorf("rule %s has an in-progress rollout: promote it to 100%% or abort it to 0%% before editing", r.ID)}
	}
	if err := r.Validate(); err != nil {
		return nil, ErrInvalid{err}
	}
	r.UpdatedAt = time.Now().UTC()
	if err := s.upsert(ctx, r); err != nil {
		return nil, err
	}
	if err := s.cacheAndPublish(ctx, r); err != nil {
		return nil, err
	}
	return r, nil
}

// StartRollout begins (or replaces) a gray release for a rule. The persisted
// rule content is left untouched and remains the old version; the new version
// is validated with the exact same rules before it is stored.
func (s *RuleStore) StartRollout(ctx context.Context, ruleID string, newRule *model.Rule, percent int) (*model.Rollout, error) {
	if err := model.ValidatePercent(percent); err != nil {
		return nil, ErrInvalid{err}
	}
	if _, ok := s.Get(ruleID); !ok {
		return nil, ErrNotFound{fmt.Errorf("rule %s not found", ruleID)}
	}
	newRule.ID = ruleID
	rl := &model.Rollout{
		RuleID:  ruleID,
		Percent: percent,
		NewRule: *newRule,
	}
	if err := rl.ValidateStart(); err != nil {
		return nil, ErrInvalid{err}
	}
	now := time.Now().UTC()
	if existing, has := s.Rollout(ruleID); has {
		rl.StartedAt = existing.StartedAt
	} else {
		rl.StartedAt = now
	}
	rl.UpdatedAt = now

	raw, err := json.Marshal(rl.NewRule)
	if err != nil {
		return nil, err
	}
	if _, err := s.pg.Exec(ctx, `
		INSERT INTO rollouts (rule_id, percent, new_rule, started_at, updated_at)
		VALUES ($1,$2,$3,$4,$5)
		ON CONFLICT (rule_id) DO UPDATE SET
		  percent=EXCLUDED.percent, new_rule=EXCLUDED.new_rule,
		  started_at=EXCLUDED.started_at, updated_at=EXCLUDED.updated_at`,
		ruleID, rl.Percent, string(raw), rl.StartedAt, rl.UpdatedAt); err != nil {
		return nil, err
	}

	s.mu.Lock()
	cp := *rl
	s.rollouts[ruleID] = &cp // old rule content stays untouched in s.rules
	s.mu.Unlock()
	if err := s.publish(ctx); err != nil {
		return nil, err
	}
	return rl, nil
}

// SetRolloutPercent resizes the canary bucket without changing either
// version's content:
//   - 1..99 adjusts the percentage monotonically (smaller values move already
//     hashed subjects back, but the mapping stays a pure function of percent);
//   - 100 promotes: the new version becomes the rule and the rollout ends;
//   - 0 aborts: the row is dropped and all traffic instantly returns to the
//     old, never-modified rule version.
func (s *RuleStore) SetRolloutPercent(ctx context.Context, ruleID string, percent int) error {
	if err := model.ValidatePercent(percent); err != nil {
		return ErrInvalid{err}
	}
	if _, ok := s.Get(ruleID); !ok {
		return ErrNotFound{fmt.Errorf("rule %s not found", ruleID)}
	}
	rl, ok := s.Rollout(ruleID)
	if !ok {
		return ErrConflict{fmt.Errorf("rule %s has no in-progress rollout", ruleID)}
	}
	if percent == 0 {
		return s.AbortRollout(ctx, ruleID)
	}
	if percent == 100 {
		return s.PromoteRollout(ctx, ruleID)
	}
	if percent == rl.Percent {
		return nil
	}
	now := time.Now().UTC()
	if _, err := s.pg.Exec(ctx,
		`UPDATE rollouts SET percent=$2, updated_at=$3 WHERE rule_id=$1`,
		ruleID, percent, now); err != nil {
		return err
	}
	s.mu.Lock()
	rl.Percent = percent
	rl.UpdatedAt = now
	cp := *rl
	s.rollouts[ruleID] = &cp
	s.mu.Unlock()
	return s.publish(ctx)
}

// PromoteRollout ends a rollout by replacing the rule with its new version
// and dropping the rollout row. The new content was already validated when
// the rollout started.
func (s *RuleStore) PromoteRollout(ctx context.Context, ruleID string) error {
	rl, ok := s.Rollout(ruleID)
	if !ok {
		return ErrConflict{fmt.Errorf("rule %s has no in-progress rollout", ruleID)}
	}
	newRule := rl.NewRule
	newRule.ID = ruleID
	newRule.UpdatedAt = time.Now().UTC()

	tx, err := s.pg.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err := upsertTx(ctx, tx, &newRule); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM rollouts WHERE rule_id=$1`, ruleID); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}

	s.mu.Lock()
	cp := newRule
	s.rules[ruleID] = &cp
	delete(s.rollouts, ruleID)
	migrator := s.migrator
	s.mu.Unlock()
	_ = s.publish(ctx)
	// Fold the canary counters into the canonical namespace. Best effort:
	// counters are transient state and the new keys simply start fresh if a
	// rename races traffic.
	if migrator != nil {
		_ = migrator.PromoteCounters(ctx, ruleID)
	}
	return nil
}

// AbortRollout instantly revokes the rollout: the rollout row disappears and
// every request goes back to the untouched old rule. Canary counters are
// deleted so they can never influence a later decision.
func (s *RuleStore) AbortRollout(ctx context.Context, ruleID string) error {
	if _, ok := s.Rollout(ruleID); !ok {
		return ErrConflict{fmt.Errorf("rule %s has no in-progress rollout", ruleID)}
	}
	if _, err := s.pg.Exec(ctx, `DELETE FROM rollouts WHERE rule_id=$1`, ruleID); err != nil {
		return err
	}
	s.mu.Lock()
	delete(s.rollouts, ruleID)
	migrator := s.migrator
	s.mu.Unlock()
	_ = s.publish(ctx)
	if migrator != nil {
		_ = migrator.AbortCounters(ctx, ruleID)
	}
	return nil
}

// Delete removes a rule (and its rollout via ON DELETE CASCADE) everywhere.
func (s *RuleStore) Delete(ctx context.Context, id string) error {
	if _, ok := s.Get(id); !ok {
		return ErrNotFound{fmt.Errorf("rule %s not found", id)}
	}
	_, hasRollout := s.Rollout(id)
	if _, err := s.pg.Exec(ctx, `DELETE FROM rules WHERE id = $1`, id); err != nil {
		return err
	}
	s.mu.Lock()
	delete(s.rules, id)
	delete(s.rollouts, id)
	migrator := s.migrator
	s.mu.Unlock()
	if err := s.rdb.Publish(ctx, pubChannel, "delete:"+id).Err(); err != nil {
		return err
	}
	if hasRollout && migrator != nil {
		_ = migrator.AbortCounters(ctx, id)
	}
	return nil
}

func (s *RuleStore) upsert(ctx context.Context, r *model.Rule) error {
	return upsertTx(ctx, s.pg, r)
}

type pgExec interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// upsertTx is written against the pool/tx shared Exec shape.
func upsertTx(ctx context.Context, exec pgExec, r *model.Rule) error {
	dims, _ := json.Marshal(r.Dimensions)
	levels, _ := json.Marshal(r.Levels)
	matchers, _ := json.Marshal(r.Matchers)
	if r.Matchers == nil {
		matchers = []byte("{}")
	}
	_, err := exec.Exec(ctx, `
		INSERT INTO rules (id, name, enabled, algorithm, dimensions, levels, matchers, updated_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
		ON CONFLICT (id) DO UPDATE SET
		  name=EXCLUDED.name, enabled=EXCLUDED.enabled, algorithm=EXCLUDED.algorithm,
		  dimensions=EXCLUDED.dimensions, levels=EXCLUDED.levels, matchers=EXCLUDED.matchers,
		  updated_at=EXCLUDED.updated_at`,
		r.ID, r.Name, r.Enabled, string(r.Algorithm), dims, levels, matchers, r.UpdatedAt)
	return err
}

func (s *RuleStore) cacheAndPublish(ctx context.Context, r *model.Rule) error {
	cp := *r
	s.mu.Lock()
	s.rules[r.ID] = &cp
	s.mu.Unlock()
	return s.publish(ctx)
}

func (s *RuleStore) publish(ctx context.Context) error {
	return s.rdb.Publish(ctx, pubChannel, "reload").Err()
}

// SubscribeHotReload blocks, reloading rules AND rollouts whenever another
// instance (or this instance) publishes a change. The Redis subscription
// confirmation frame is consumed before the message loop starts, so once the
// subscribing goroutine has had a scheduling point after this call the very
// first published change cannot be lost to a still-pending SUBSCRIBE.
func (s *RuleStore) SubscribeHotReload(ctx context.Context) {
	sub := s.rdb.Subscribe(ctx, pubChannel)
	defer sub.Close()
	// First Receive() returns the *Subscription confirmation frame; using
	// Receive directly (rather than Channel()) means we know the client is
	// actually subscribed before we enter the message loop.
	if _, err := sub.Receive(ctx); err != nil {
		return
	}
	for {
		msg, err := sub.ReceiveMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			return
		}
		if msg.Payload == "reload" {
			_ = s.loadAll(ctx)
		} else if len(msg.Payload) > 7 && msg.Payload[:7] == "delete:" {
			id := msg.Payload[7:]
			s.mu.Lock()
			delete(s.rules, id)
			delete(s.rollouts, id)
			s.mu.Unlock()
		}
	}
}

// ErrInvalid wraps validation failures for the API layer.
type ErrInvalid struct{ Err error }

func (e ErrInvalid) Error() string { return e.Err.Error() }

// ErrNotFound wraps missing rules.
type ErrNotFound struct{ Err error }

func (e ErrNotFound) Error() string { return e.Err.Error() }

// ErrConflict wraps state conflicts (e.g. editing a rule mid-rollout).
type ErrConflict struct{ Err error }

func (e ErrConflict) Error() string { return e.Err.Error() }
