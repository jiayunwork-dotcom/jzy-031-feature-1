<script setup lang="ts">
import { ref, reactive, computed } from 'vue'
import type { Rule, Verdict } from '../lib/types'
import { api } from '../lib/api'

const props = defineProps<{ rule: Rule | null }>()

const cfg = reactive({
  client_id: 'client-A',
  api_path: '/orders',
  group: 'group-1',
  count: 60,
  batch: 10,
  interval_ms: 100,
})

const running = ref(false)
const result = ref<{ allowed: number; denied: number; items: Array<{ seq: number; allowed: boolean; level?: string }> } | null>(null)
const lastVerdict = ref<Verdict | null>(null)
const error = ref('')

// preset scenarios that make the algorithm differences obvious
const PRESETS = [
  {
    name: '瞬时突发 ×6',
    apply() {
      cfg.count = 6
      cfg.batch = 6
      cfg.interval_ms = 0
    },
  },
  {
    name: '每秒 10 个 ×6 批',
    apply() {
      cfg.count = 60
      cfg.batch = 10
      cfg.interval_ms = 1000
    },
  },
  {
    name: '跨窗口边界（观察固定/滑动差异）',
    apply() {
      cfg.count = 30
      cfg.batch = 15
      cfg.interval_ms = 900
    },
  },
]

async function run() {
  if (!props.rule) {
    error.value = '请先在左侧选择一条规则'
    return
  }
  error.value = ''
  running.value = true
  result.value = null
  try {
    const r = await api.replay({
      count: cfg.count,
      batch: cfg.batch,
      interval_ms: cfg.interval_ms,
      client_id: cfg.client_id,
      api_path: cfg.api_path,
      group: cfg.group,
    })
    result.value = { allowed: r.allowed, denied: r.denied, items: r.items }
  } catch (e: any) {
    error.value = e.message
  } finally {
    running.value = false
  }
}

async function single() {
  if (!props.rule) {
    error.value = '请先在左侧选择一条规则'
    return
  }
  error.value = ''
  try {
    lastVerdict.value = await api.check({
      client_id: cfg.client_id,
      api_path: cfg.api_path,
      group: cfg.group,
    })
  } catch (e: any) {
    // a 429 also carries a JSON verdict
    try {
      lastVerdict.value = JSON.parse(e.message)
    } catch {
      error.value = e.message
    }
  }
}

const sequence = computed(() => result.value?.items ?? [])
</script>

<template>
  <div class="panel">
    <h2>发压 / 流量回放</h2>
    <div v-if="error" class="error">{{ error }}</div>

    <div class="grid-2">
      <label class="field"><span class="lbl">client_id</span><input v-model="cfg.client_id" /></label>
      <label class="field"><span class="lbl">group</span><input v-model="cfg.group" /></label>
    </div>
    <label class="field"><span class="lbl">api_path</span><input v-model="cfg.api_path" /></label>

    <div class="grid-3 grid-2">
      <label class="field">
        <span class="lbl">总请求数</span>
        <input v-model.number="cfg.count" type="number" min="1" max="2000" />
      </label>
      <label class="field">
        <span class="lbl">每批并发数（突发宽度）</span>
        <input v-model.number="cfg.batch" type="number" min="1" />
      </label>
      <label class="field">
        <span class="lbl">批间隔 ms</span>
        <input v-model.number="cfg.interval_ms" type="number" min="0" />
      </label>
    </div>

    <div class="row" style="margin: 6px 0 12px">
      <button v-for="p in PRESETS" :key="p.name" class="secondary small" @click="p.apply()">
        {{ p.name }}
      </button>
    </div>

    <div class="row">
      <button @click="run" :disabled="running">{{ running ? '发压中…' : '▶ 发压' }}</button>
      <button class="secondary" @click="single">发单个请求</button>
    </div>

    <div v-if="result" style="margin-top:12px">
      <div class="grid-3 grid-4">
        <div class="stat"><div class="v">{{ result.allowed + result.denied }}</div><div class="k">总数</div></div>
        <div class="stat"><div class="v green">{{ result.allowed }}</div><div class="k">放行</div></div>
        <div class="stat"><div class="v red">{{ result.denied }}</div><div class="k">拒绝</div></div>
        <div class="stat">
          <div class="v amber">{{ Math.round((result.allowed / (result.allowed + result.denied)) * 100) }}%</div>
          <div class="k">放行率</div>
        </div>
      </div>
      <div class="muted" style="margin-top:8px">判定序列（绿=放行，红=拒绝，按请求顺序）：</div>
      <div class="seq">
        <div
          v-for="it in sequence"
          :key="it.seq"
          class="cell"
          :class="it.allowed ? 'a' : 'd'"
          :title="`#${it.seq + 1} ${it.allowed ? '放行' : '拒绝 ' + (it.level ?? '')}`"
        />
      </div>
    </div>

    <div v-if="lastVerdict" class="verdict-box" :class="lastVerdict.allowed ? 'ok' : 'no'">
      <template v-if="lastVerdict.allowed">
        <strong style="color:var(--green)">放行 200</strong>
        <div v-for="rr in lastVerdict.results" :key="rr.rule_id" style="margin-top:4px">
          规则「{{ rr.rule_name }}」各级余量：
          <span v-for="lv in rr.levels" :key="lv.key" class="mono" style="margin-right:8px">
            {{ lv.level }}={{ lv.remaining }}
          </span>
        </div>
      </template>
      <template v-else>
        <strong style="color:var(--red)">拒绝 429</strong>
        <div style="margin-top:4px">
          被规则「{{ lastVerdict.rule_name }}」的
          <strong>{{ lastVerdict.level }}</strong> 级配额挡下
        </div>
        <div class="muted">{{ lastVerdict.reason }}</div>
      </template>
    </div>
  </div>
</template>
