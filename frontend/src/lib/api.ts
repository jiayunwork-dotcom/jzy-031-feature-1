import type {
  DenyEvent,
  Point,
  ReplayResult,
  Rollout,
  Rule,
  RuleDraft,
  RuleLevel,
  Verdict,
} from './types'

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(path, {
    headers: { 'Content-Type': 'application/json' },
    ...init,
  })
  const text = await res.text()
  const body = text ? JSON.parse(text) : {}
  if (!res.ok) {
    throw new Error(body.error || `HTTP ${res.status}`)
  }
  return body as T
}

export const api = {
  listRules: () => request<Rule[]>('/api/rules'),

  createRule: (draft: RuleDraft) =>
    request<Rule>('/api/rules', { method: 'POST', body: JSON.stringify(draft) }),

  updateRule: (id: string, draft: RuleDraft) =>
    request<Rule>(`/api/rules/${id}`, { method: 'PUT', body: JSON.stringify(draft) }),

  deleteRule: (id: string) =>
    request<{ deleted: string }>(`/api/rules/${id}`, { method: 'DELETE' }),

  // ---- gray rollout (canary) control ----
  listRollouts: () => request<{ rollouts: Rollout[] }>('/api/rollouts'),

  startRollout: (id: string, draft: RuleDraft, percent: number) =>
    request<Rollout>(`/api/rules/${id}/rollout`, {
      method: 'POST',
      body: JSON.stringify({ percent, canary: toWire(draft) }),
    }),

  setRolloutPercent: (id: string, percent: number) =>
    request<Rollout>(`/api/rules/${id}/rollout`, {
      method: 'PUT',
      body: JSON.stringify({ percent }),
    }),

  updateRolloutCanary: (id: string, draft: RuleDraft, percent: number) =>
    request<Rollout>(`/api/rules/${id}/rollout`, {
      method: 'PUT',
      body: JSON.stringify({ percent, canary: toWire(draft) }),
    }),

  promoteRollout: (id: string) =>
    request<Rule>(`/api/rules/${id}/rollout/promote`, { method: 'POST' }),

  abortRollout: (id: string) =>
    request<{ aborted: string }>(`/api/rules/${id}/rollout`, { method: 'DELETE' }),

  check: (req: { client_id: string; api_path: string; group: string }) =>
    request<Verdict>('/api/gateway/check', {
      method: 'POST',
      body: JSON.stringify(req),
    }),

  timeSeries: (ruleId: string, seconds: number, version: 'stable' | 'canary' = 'stable') =>
    request<{ points: Point[] }>(
      `/api/metrics/timeseries?rule_id=${encodeURIComponent(ruleId)}&seconds=${seconds}&version=${version}`,
    ),

  denies: (ruleId: string, version?: 'stable' | 'canary') => {
    const v = version ? `&version=${version}` : ''
    return request<{ denies: DenyEvent[] }>(
      `/api/metrics/denies?rule_id=${encodeURIComponent(ruleId)}&limit=20${v}`,
    )
  },

  ruleState: (id: string, dims: { client_id: string; api_path: string; group: string }) => {
    const q = new URLSearchParams({
      client_id: dims.client_id,
      api_path: dims.api_path,
      group: dims.group,
    })
    return request<{
      selected: any[]
      active_keys: any[]
      rollout?: Rollout
      stable_selected?: any[]
      canary_selected?: any[]
    }>(`/api/rules/${id}/state?${q.toString()}`)
  },

  replay: (req: {
    count: number
    batch: number
    interval_ms: number
    client_id: string
    api_path: string
    group: string
    distinct_clients?: number
  }) =>
    request<ReplayResult>('/api/tools/replay', {
      method: 'POST',
      body: JSON.stringify(req),
    }),
}

// RuleDraft on the wire is the same shape minus id; keep levels as-is.
function toWire(draft: RuleDraft): RuleDraft {
  return draft
}

// Helper used by the editor to clone a Rule into an editable draft.
export function ruleToDraft(r: Rule): RuleDraft {
  return {
    name: r.name,
    enabled: r.enabled,
    algorithm: r.algorithm,
    dimensions: [...r.dimensions],
    levels: JSON.parse(JSON.stringify(r.levels)) as Partial<
      Record<import('./types').Level, RuleLevel>
    >,
    matchers: r.matchers ? JSON.parse(JSON.stringify(r.matchers)) : {},
  }
}
