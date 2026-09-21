// Package engine holds the five rate-limiting algorithms. Each algorithm
// lives in its own file and ships its own Redis Lua script: correctness under
// concurrency comes from Redis executing the whole check-and-consume script
// atomically, so two gateway instances can never read the same old counter
// and both let a request through.
package engine

import (
	"context"
	"errors"
	"strconv"

	"github.com/redis/go-redis/v9"

	"ratelimit-gateway/internal/model"
)

// Decision is the result of one bucket/window evaluation.
type Decision struct {
	Allowed   bool  `json:"allowed"`
	Remaining int64 `json:"remaining"`
	// ResetInMs is the milliseconds until the bucket is full / the window
	// rolls over / the oldest log entry expires.
	ResetInMs int64 `json:"reset_in_ms"`
}

// Engine evaluates a single algorithm against a single Redis key.
type Engine interface {
	Name() model.Algorithm
	Eval(ctx context.Context, key string, cfg model.RuleLevel, peek bool) (Decision, error)
}

// Manager dispatches to the engine selected by the rule's algorithm.
type Manager struct {
	rdb     *redis.Client
	engines map[model.Algorithm]Engine
}

// NewManager wires the five engines and pre-loads every Lua script.
func NewManager(ctx context.Context, rdb *redis.Client) (*Manager, error) {
	m := &Manager{rdb: rdb, engines: map[model.Algorithm]Engine{}}
	specs := []struct {
		algo model.Algorithm
		src  string
		args func(cfg model.RuleLevel, nonce string) []string
	}{
		{model.AlgoTokenBucket, tokenBucketLua, bucketArgs},
		{model.AlgoLeakyBucket, leakyBucketLua, bucketArgs},
		{model.AlgoFixedWindow, fixedWindowLua, windowArgs},
		{model.AlgoSlidingWindow, slidingWindowLua, windowArgs},
		{model.AlgoSlidingLog, slidingLogLua, logArgs},
	}
	for _, s := range specs {
		e := &scriptEngine{
			rdb:       rdb,
			algo:      s.algo,
			src:       s.src,
			buildArgs: s.args,
		}
		if err := e.load(ctx); err != nil {
			return nil, err
		}
		m.engines[s.algo] = e
	}
	return m, nil
}

func (m *Manager) get(algo model.Algorithm) (Engine, error) {
	e, ok := m.engines[algo]
	if !ok {
		return nil, errors.New("no engine for algorithm: " + string(algo))
	}
	return e, nil
}

// Try reserves one token/slot, mutating shared state.
func (m *Manager) Try(ctx context.Context, algo model.Algorithm, key string, cfg model.RuleLevel) (Decision, error) {
	e, err := m.get(algo)
	if err != nil {
		return Decision{}, err
	}
	return e.Eval(ctx, key, cfg, false)
}

// Peek computes the same answer without mutating state (for the dashboard).
func (m *Manager) Peek(ctx context.Context, algo model.Algorithm, key string, cfg model.RuleLevel) (Decision, error) {
	e, err := m.get(algo)
	if err != nil {
		return Decision{}, err
	}
	return e.Eval(ctx, key, cfg, true)
}

// scriptEngine is the shared EVALSHA-with-fallback runner.
type scriptEngine struct {
	rdb       *redis.Client
	algo      model.Algorithm
	src       string
	sha       string
	buildArgs func(cfg model.RuleLevel, nonce string) []string
}

func (e *scriptEngine) Name() model.Algorithm { return e.algo }

func (e *scriptEngine) load(ctx context.Context) error {
	sha, err := e.rdb.ScriptLoad(ctx, e.src).Result()
	if err != nil {
		return err
	}
	e.sha = sha
	return nil
}

func (e *scriptEngine) Eval(ctx context.Context, key string, cfg model.RuleLevel, peek bool) (Decision, error) {
	nonce := newNonce()
	args := e.buildArgs(cfg, nonce)
	args = append(args, btos(peek))

	run := func(useSHA bool) ([]interface{}, error) {
		if useSHA {
			return e.rdb.EvalSha(ctx, e.sha, []string{key}, argsToAny(args)...).Slice()
		}
		return e.rdb.Eval(ctx, e.src, []string{key}, argsToAny(args)...).Slice()
	}

	res, err := run(true)
	if err != nil && isNoScript(err) {
		if lerr := e.load(ctx); lerr != nil {
			return Decision{}, lerr
		}
		res, err = run(false)
	}
	if err != nil {
		return Decision{}, err
	}
	if len(res) < 3 {
		return Decision{}, errors.New("engine script returned malformed result")
	}
	return Decision{
		Allowed:   asInt64(res[0]) == 1,
		Remaining: asInt64(res[1]),
		ResetInMs: asInt64(res[2]),
	}, nil
}

func bucketArgs(cfg model.RuleLevel, nonce string) []string {
	return []string{strconv.FormatInt(cfg.Threshold, 10), strconv.FormatInt(cfg.Burst, 10), nonce}
}

func windowArgs(cfg model.RuleLevel, nonce string) []string {
	return []string{strconv.FormatInt(cfg.Threshold, 10), strconv.FormatInt(cfg.WindowSeconds, 10), nonce}
}

func logArgs(cfg model.RuleLevel, nonce string) []string {
	return []string{strconv.FormatInt(cfg.Threshold, 10), strconv.FormatInt(cfg.WindowSeconds, 10), nonce}
}

func argsToAny(ss []string) []interface{} {
	out := make([]interface{}, len(ss))
	for i, s := range ss {
		out[i] = s
	}
	return out
}

func btos(b bool) string {
	if b {
		return "1"
	}
	return "0"
}

func isNoScript(err error) bool {
	return err != nil && errors.Is(err, redis.Nil) == false &&
		contains(err.Error(), "NOSCRIPT")
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

func asInt64(v interface{}) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case int:
		return int64(x)
	case string:
		n, _ := strconv.ParseInt(x, 10, 64)
		return n
	}
	return 0
}
