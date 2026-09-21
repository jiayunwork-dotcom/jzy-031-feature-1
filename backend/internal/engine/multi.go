package engine

import (
	"context"
	"errors"
	"strconv"

	"ratelimit-gateway/internal/model"
)

// KeySpec is one level's key and quota in a multi-level atomic evaluation.
type KeySpec struct {
	Key string
	Cfg model.RuleLevel
}

// MultiResult is the atomic verdict over several keys plus per-key decisions.
type MultiResult struct {
	Allowed   bool
	DenyIndex int // 0-based index of the first denying key; -1 when allowed
	PerKey    []Decision
}

// multiScript pairs each algorithm with its N-key atomic script.
var multiScripts = map[model.Algorithm]string{
	model.AlgoTokenBucket:   tokenBucketMultiLua,
	model.AlgoLeakyBucket:   leakyBucketMultiLua,
	model.AlgoFixedWindow:   fixedWindowMultiLua,
	model.AlgoSlidingWindow: slidingWindowMultiLua,
	model.AlgoSlidingLog:    slidingLogMultiLua,
}

// TryMulti evaluates N keys of one algorithm atomically: either all levels
// consume or none do, and the first saturated level (in slice order) is
// reported. This is what keeps the four-level cascade race-free under
// contention from multiple gateway instances.
func (m *Manager) TryMulti(ctx context.Context, algo model.Algorithm, keys []KeySpec) (MultiResult, error) {
	src, ok := multiScripts[algo]
	if !ok {
		return MultiResult{}, errAlgo(algo)
	}

	keyNames := make([]string, len(keys))
	for i, k := range keys {
		keyNames[i] = k.Key
	}
	args := []string{strconv.Itoa(len(keys))}
	for i, k := range keys {
		switch algo {
		case model.AlgoSlidingLog:
			args = append(args,
				strconv.FormatInt(k.Cfg.Threshold, 10),
				strconv.FormatInt(k.Cfg.WindowSeconds, 10),
				newNonce()+strconv.Itoa(i))
		default:
			args = append(args, strconv.FormatInt(k.Cfg.Threshold, 10))
			if algo == model.AlgoTokenBucket || algo == model.AlgoLeakyBucket {
				args = append(args, strconv.FormatInt(k.Cfg.Burst, 10))
			} else {
				args = append(args, strconv.FormatInt(k.Cfg.WindowSeconds, 10))
			}
		}
	}
	args = append(args, "0") // peek = false

	res, err := m.evalMulti(ctx, src, keyNames, args)
	if err != nil {
		return MultiResult{}, err
	}
	return parseMulti(res, len(keys))
}

func (m *Manager) evalMulti(ctx context.Context, src string, keys []string, args []string) ([]interface{}, error) {
	// EVAL directly: multi-key scripts aren't preloaded, but Redis caches the
	// script content server-side via SCRIPT cache after the first EVAL.
	return m.rdb.Eval(ctx, src, keys, stringsToAny(args)...).Slice()
}

func parseMulti(res []interface{}, n int) (MultiResult, error) {
	out := MultiResult{DenyIndex: -1, PerKey: make([]Decision, 0, n)}
	if len(res) != n*3+1 {
		return out, errMalformed()
	}
	for i := 0; i < n; i++ {
		out.PerKey = append(out.PerKey, Decision{
			Allowed:   asInt64(res[i*3]) == 1,
			Remaining: asInt64(res[i*3+1]),
			ResetInMs: asInt64(res[i*3+2]),
		})
	}
	deny := asInt64(res[n*3])
	if deny > 0 {
		out.DenyIndex = int(deny) - 1
		out.Allowed = false
	} else {
		out.Allowed = true
	}
	return out, nil
}

func stringsToAny(ss []string) []any {
	out := make([]any, len(ss))
	for i, s := range ss {
		out[i] = s
	}
	return out
}

func errAlgo(algo model.Algorithm) error {
	return errors.New("no multi-key engine for algorithm: " + string(algo))
}

func errMalformed() error {
	return errors.New("multi-key engine script returned a malformed result")
}
