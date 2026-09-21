// Package model defines the domain types: rate-limit rules and the requests
// that flow through the gateway.
package model

import (
	"fmt"
	"strings"
	"time"
)

// Algorithm selects one of the five supported limiting algorithms.
type Algorithm string

const (
	AlgoTokenBucket   Algorithm = "token_bucket"
	AlgoLeakyBucket   Algorithm = "leaky_bucket"
	AlgoFixedWindow   Algorithm = "fixed_window"
	AlgoSlidingWindow Algorithm = "sliding_window"
	AlgoSlidingLog    Algorithm = "sliding_log"
)

// Level is one of the four coexisting quota scopes.
type Level string

const (
	LevelGlobal Level = "global"
	LevelGroup  Level = "group"
	LevelAPI    Level = "api"
	LevelClient Level = "client"
)

// Dimension names that may be used in a rule's multi-dimensional key.
const (
	DimClient = "client_id"
	DimAPI    = "api_path"
	DimGroup  = "group"
)

// RuleLevel is one level's quota inside a rule.
type RuleLevel struct {
	// Threshold is the refill/leak rate (per second) for the bucket
	// algorithms and the request budget for the window algorithms.
	Threshold int64 `json:"threshold"`
	// Burst is the bucket capacity (token bucket / leaky bucket). Ignored
	// by the window algorithms.
	Burst int64 `json:"burst,omitempty"`
	// WindowSeconds is the window length for the three window algorithms.
	WindowSeconds int64 `json:"window_seconds,omitempty"`
}

// Rule is a persisted rate-limit configuration.
type Rule struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Enabled   bool      `json:"enabled"`
	Algorithm Algorithm `json:"algorithm"`
	// Dimensions is the AND-combination this rule is keyed on,
	// e.g. ["client_id","api_path"].
	Dimensions []string `json:"dimensions"`
	// Levels holds the quotas that coexist; a request is checked against
	// every present level, most permissive (global) first.
	Levels map[Level]RuleLevel `json:"levels"`
	// Matchers optionally restrict which values a rule applies to.
	// A nil/empty value for a dimension means "any".
	Matchers  map[string][]string `json:"matchers,omitempty"`
	UpdatedAt time.Time           `json:"updated_at"`
}

// Validate rejects illegal configurations so the gateway never runs with a
// bad rule. All error messages are surfaced to the operator.
func (r *Rule) Validate() error {
	if strings.TrimSpace(r.Name) == "" {
		return fmt.Errorf("rule name must not be empty")
	}
	switch r.Algorithm {
	case AlgoTokenBucket, AlgoLeakyBucket, AlgoFixedWindow,
		AlgoSlidingWindow, AlgoSlidingLog:
	default:
		return fmt.Errorf("unknown algorithm %q", r.Algorithm)
	}

	seen := map[string]bool{}
	validDims := map[string]bool{DimClient: true, DimAPI: true, DimGroup: true}
	for _, d := range r.Dimensions {
		if !validDims[d] {
			return fmt.Errorf("unknown dimension %q", d)
		}
		if seen[d] {
			return fmt.Errorf("duplicate dimension %q", d)
		}
		seen[d] = true
	}

	levelsPresent := 0
	for _, lv := range []Level{LevelGlobal, LevelGroup, LevelAPI, LevelClient} {
		cfg, ok := r.Levels[lv]
		if !ok {
			continue
		}
		levelsPresent++
		if cfg.Threshold <= 0 {
			return fmt.Errorf("level %s threshold must be a positive integer, got %d", lv, cfg.Threshold)
		}
		switch r.Algorithm {
		case AlgoTokenBucket, AlgoLeakyBucket:
			if cfg.Burst <= 0 {
				return fmt.Errorf("level %s burst (bucket capacity) must be positive for bucket algorithms", lv)
			}
		default:
			if cfg.WindowSeconds <= 0 {
				return fmt.Errorf("level %s window_seconds must be positive for window algorithms", lv)
			}
		}
		// A level that references a dimension not declared in the rule
		// would never be attributable to a concrete request value.
		switch lv {
		case LevelGroup:
			if !seen[DimGroup] {
				return fmt.Errorf("level %s requires dimension %q to be declared", lv, DimGroup)
			}
		case LevelAPI:
			if !seen[DimAPI] {
				return fmt.Errorf("level %s requires dimension %q to be declared", lv, DimAPI)
			}
		case LevelClient:
			if !seen[DimClient] {
				return fmt.Errorf("level %s requires dimension %q to be declared", lv, DimClient)
			}
		}
	}
	if levelsPresent == 0 {
		return fmt.Errorf("rule must define at least one quota level")
	}

	// Matchers must reference declared dimensions and provide a value.
	for dim, vals := range r.Matchers {
		if !seen[dim] {
			return fmt.Errorf("matcher on dimension %q which the rule does not declare", dim)
		}
		if len(vals) == 0 {
			return fmt.Errorf("matcher for dimension %q must list at least one value", dim)
		}
		for _, v := range vals {
			if strings.TrimSpace(v) == "" {
				return fmt.Errorf("matcher for dimension %q contains an empty value", dim)
			}
		}
	}
	return nil
}

// NeedsDimension reports whether the rule is keyed on d.
func (r *Rule) NeedsDimension(d string) bool {
	for _, x := range r.Dimensions {
		if x == d {
			return true
		}
	}
	return false
}

// RequestContext is everything extracted from an inbound HTTP request that
// matching and quota keys are built from.
type RequestContext struct {
	ClientID string
	APIPath  string
	Group    string
}
