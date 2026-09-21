import type {
  DenyEvent,
  Point,
  Rule,
  RuleDraft,
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

  check: (req: { client_id: string; api_path: string; group: string }) =>
    request<Verdict>('/api/gateway/check', {
      method: 'POST',
      body: JSON.stringify(req),
    }),

  timeSeries: (ruleId: string, seconds: number) =>
    request<{ points: Point[] }>(
      `/api/metrics/timeseries?rule_id=${encodeURIComponent(ruleId)}&seconds=${seconds}`,
    ),

  denies: (ruleId: string) =>
    request<{ denies: DenyEvent[] }>(
      `/api/metrics/denies?rule_id=${encodeURIComponent(ruleId)}&limit=20`,
    ),

  ruleState: (id: string, dims: { client_id: string; api_path: string; group: string }) => {
    const q = new URLSearchParams({
      client_id: dims.client_id,
      api_path: dims.api_path,
      group: dims.group,
    })
    return request<{ selected: any[]; active_keys: any[] }>(
      `/api/rules/${id}/state?${q.toString()}`,
    )
  },

  replay: (req: {
    count: number
    batch: number
    interval_ms: number
    client_id: string
    api_path: string
    group: string
  }) =>
    request<{
      total: number
      allowed: number
      denied: number
      items: Array<{ seq: number; allowed: boolean; time_ms: number; level?: string; reason?: string }>
    }>('/api/tools/replay', { method: 'POST', body: JSON.stringify(req) }),
}
