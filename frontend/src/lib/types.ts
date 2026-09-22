// API types mirror the Go backend's JSON wire format.

export type Algorithm =
  | 'token_bucket'
  | 'leaky_bucket'
  | 'fixed_window'
  | 'sliding_window'
  | 'sliding_log'

export type Level = 'global' | 'group' | 'api' | 'client'

export type Dimension = 'client_id' | 'api_path' | 'group'

export interface RuleLevel {
  threshold: number
  burst?: number
  window_seconds?: number
}

export interface Rule {
  id: string
  name: string
  enabled: boolean
  algorithm: Algorithm
  dimensions: Dimension[]
  levels: Partial<Record<Level, RuleLevel>>
  matchers?: Partial<Record<Dimension, string[]>>
  updated_at?: string
}

export interface RuleDraft {
  name: string
  enabled: boolean
  algorithm: Algorithm
  dimensions: Dimension[]
  levels: Partial<Record<Level, RuleLevel>>
  matchers: Partial<Record<Dimension, string[]>>
}

export type RolloutVersion = 'stable' | 'canary'

// Rollout is one in-flight gray change: percent of traffic judged by the
// canary (new) rule, plus the frozen old and new rule contents.
export interface Rollout {
  rule_id: string
  percent: number
  canary: Rule
  old: Rule
}

export interface LevelResult {
  level: Level
  allowed: boolean
  remaining: number
  reset_in_ms: number
  release_in_ms?: number
  key: string
}

export interface RuleResult {
  rule_id: string
  rule_name: string
  allowed: boolean
  version?: RolloutVersion | ''
  deny_level?: Level
  reason?: string
  levels: LevelResult[]
}

export interface Verdict {
  allowed: boolean
  rule_id?: string
  rule_name?: string
  level?: Level
  version?: RolloutVersion | ''
  reason?: string
  results: RuleResult[]
}

export interface Point {
  ts: number
  allow: number
  deny: number
}

export interface DenyEvent {
  time_ms: number
  rule_id: string
  rule_name: string
  level: Level
  reason: string
  client_id: string
  api_path: string
  group: string
  remaining: number
  version?: RolloutVersion
}

export interface ActiveKey {
  level: string
  version?: RolloutVersion
  key: string
  remaining: number
  reset_in_ms: number
  kind: string
}

export interface ReplayItem {
  seq: number
  allowed: boolean
  time_ms: number
  rule_id?: string
  level?: string
  version?: RolloutVersion | ''
  reason?: string
}

export interface ReplayResult {
  total: number
  allowed: number
  denied: number
  by_version?: Record<string, { allowed: number; denied: number }>
  items: ReplayItem[]
}
