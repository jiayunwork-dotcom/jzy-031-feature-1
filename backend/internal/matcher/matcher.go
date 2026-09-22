// Package matcher selects which rules a request falls under and builds the
// Redis keys that keep each AND-combination of dimension values separately
// counted. Different dimension values always get different keys, so client A
// exhausting its quota can never consume client B's.
//
// During a gray rollout the old and new rule versions each own a completely
// separate counter namespace: Key takes the version and inserts a `canary`
// segment for the new version. Both variants keep the rule id hash tag, so
// the promotion migration can RENAME keys cluster-safely.
package matcher

import (
	"strings"

	"ratelimit-gateway/internal/canary"
	"ratelimit-gateway/internal/model"
)

// Matcher evaluates rule predicates and constructs quota keys.
type Matcher struct{}

func New() *Matcher { return &Matcher{} }

func dimValue(rctx model.RequestContext, dim string) string {
	switch dim {
	case model.DimClient:
		return rctx.ClientID
	case model.DimAPI:
		return rctx.APIPath
	case model.DimGroup:
		return rctx.Group
	}
	return ""
}

// Matches reports whether every dimension the rule declares is satisfied by
// the request (AND semantics). An optional matcher allow-list restricts the
// values the rule applies to.
func (m *Matcher) Matches(rule *model.Rule, rctx model.RequestContext) bool {
	for _, dim := range rule.Dimensions {
		v := dimValue(rctx, dim)
		if v == "" {
			return false // cannot attribute this request to this dimension
		}
		if vals, ok := rule.Matchers[dim]; ok && !contains(vals, v) {
			return false
		}
	}
	return true
}

func contains(xs []string, v string) bool {
	for _, x := range xs {
		if x == v {
			return true
		}
	}
	return false
}

// Key builds the Redis key for one (rule, version, level, request) bucket.
//
// The key contains exactly the dimension values that define the level:
// global is shared by everyone; group groups by `group`; api by `api_path`;
// client by the full AND-combination the rule declares plus the client, so a
// client's quota is tracked per rule/interface independently.
//
// Format:
//
//	old:  rl:{ruleID}:{level}:<dim pairs>
//	new:  rl:{ruleID}:canary:{level}:<dim pairs>
func (m *Matcher) Key(rule *model.Rule, version canary.Version, level model.Level, rctx model.RequestContext) string {
	var b strings.Builder
	b.WriteString("rl:{")
	b.WriteString(rule.ID)
	b.WriteString("}:")
	if version == canary.VersionNew {
		b.WriteString("canary:")
	}
	b.WriteString(string(level))
	b.WriteString(":")

	switch level {
	case model.LevelGlobal:
		b.WriteString("_")
	case model.LevelGroup:
		b.WriteString("g=")
		b.WriteString(dimValue(rctx, model.DimGroup))
	case model.LevelAPI:
		b.WriteString("a=")
		b.WriteString(dimValue(rctx, model.DimAPI))
	case model.LevelClient:
		// client key carries the whole AND-combination the rule declares,
		// always ending in the concrete client value.
		first := true
		for _, dim := range rule.Dimensions {
			if !first {
				b.WriteString("|")
			}
			first = false
			b.WriteString(shortDim(dim))
			b.WriteString("=")
			b.WriteString(dimValue(rctx, dim))
		}
		if first { // rule has only global level; still attribute client if present
			b.WriteString("c=")
			b.WriteString(dimValue(rctx, model.DimClient))
		}
	}
	return b.String()
}

func shortDim(d string) string {
	switch d {
	case model.DimClient:
		return "c"
	case model.DimAPI:
		return "a"
	case model.DimGroup:
		return "g"
	}
	return d
}
