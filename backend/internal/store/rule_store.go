// Package store persists rate-limit rules in PostgreSQL and keeps a hot
// in-memory copy on every gateway instance. Rule changes are published over
// Redis pub/sub so every instance reloads immediately: thresholds take effect
// without restarting any process.
package store

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
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
`

const pubChannel = "rules:changed"

// RuleStore is the source of truth for rules.
type RuleStore struct {
	pg  *pgxpool.Pool
	rdb *redis.Client

	mu    sync.RWMutex
	rules map[string]*model.Rule
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
	s := &RuleStore{pg: pg, rdb: rdb, rules: map[string]*model.Rule{}}
	if err := s.loadAll(ctx); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *RuleStore) loadAll(ctx context.Context) error {
	rows, err := s.pg.Query(ctx,
		`SELECT id, name, enabled, algorithm, dimensions, levels, matchers, updated_at FROM rules ORDER BY id`)
	if err != nil {
		return err
	}
	defer rows.Close()

	fresh := map[string]*model.Rule{}
	for rows.Next() {
		r, err := scanRow(rows)
		if err != nil {
			return err
		}
		fresh[r.ID] = r
	}
	if err := rows.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	s.rules = fresh
	s.mu.Unlock()
	return nil
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanRow(sc rowScanner) (*model.Rule, error) {
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

// List returns every enabled-or-disabled rule, sorted by id for determinism.
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

// Get returns a copy of one rule.
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

// Create inserts and validates a new rule.
func (s *RuleStore) Create(ctx context.Context, r *model.Rule) (*model.Rule, error) {
	if err := r.Validate(); err != nil {
		return nil, ErrInvalid{err}
	}
	r.ID = uuid.NewString()
	if !r.Enabled {
		// explicit false stays false; a zero value on create means enabled
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

// Update validates and replaces an existing rule and hot-reloads all instances.
func (s *RuleStore) Update(ctx context.Context, r *model.Rule) (*model.Rule, error) {
	if r.ID == "" {
		return nil, ErrInvalid{fmt.Errorf("rule id required for update")}
	}
	if _, ok := s.Get(r.ID); !ok {
		return nil, ErrNotFound{fmt.Errorf("rule %s not found", r.ID)}
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

// Delete removes a rule everywhere.
func (s *RuleStore) Delete(ctx context.Context, id string) error {
	if _, ok := s.Get(id); !ok {
		return ErrNotFound{fmt.Errorf("rule %s not found", id)}
	}
	if _, err := s.pg.Exec(ctx, `DELETE FROM rules WHERE id = $1`, id); err != nil {
		return err
	}
	s.mu.Lock()
	delete(s.rules, id)
	s.mu.Unlock()
	return s.rdb.Publish(ctx, pubChannel, "delete:"+id).Err()
}

func (s *RuleStore) upsert(ctx context.Context, r *model.Rule) error {
	dims, _ := json.Marshal(r.Dimensions)
	levels, _ := json.Marshal(r.Levels)
	matchers, _ := json.Marshal(r.Matchers)
	if r.Matchers == nil {
		matchers = []byte("{}")
	}
	_, err := s.pg.Exec(ctx, `
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
	return s.rdb.Publish(ctx, pubChannel, "reload").Err()
}

// SubscribeHotReload blocks, reloading rules whenever another instance
// (or this instance) publishes a change.
func (s *RuleStore) SubscribeHotReload(ctx context.Context) {
	sub := s.rdb.Subscribe(ctx, pubChannel)
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
			} else if len(msg.Payload) > 7 && msg.Payload[:7] == "delete:" {
				id := msg.Payload[7:]
				s.mu.Lock()
				delete(s.rules, id)
				s.mu.Unlock()
			}
		}
	}
}

// ErrInvalid wraps validation failures for the API layer.
type ErrInvalid struct{ Err error }

func (e ErrInvalid) Error() string { return e.Err.Error() }

// ErrNotFound wraps missing rules.
type ErrNotFound struct{ Err error }

func (e ErrNotFound) Error() string { return e.Err.Error() }
