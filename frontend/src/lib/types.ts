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

export interface LevelResult {
  level: Level
  allowed: boolean
  remaining: number
  reset_in_ms: number
  release_in_ms?: number
  key: string
}

export interface Verdict {
  allowed: boolean
  rule_id?: string
  rule_name?: string
  level?: Level
  reason?: string
  results: Array<{
    rule_id: string
    rule_name: string
    allowed: boolean
    deny_level?: Level
    reason?: string
    levels: LevelResult[]
  }>
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
}

export interface ActiveKey {
  level: string
  key: string
  remaining: number
  reset_in_ms: number
  kind: string
}
