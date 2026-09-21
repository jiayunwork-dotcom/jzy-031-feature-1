<script setup lang="ts">
import { reactive, ref, watch, computed } from 'vue'
import type { Algorithm, Dimension, Level, Rule, RuleDraft, RuleLevel } from '../lib/types'
import { api } from '../lib/api'

const props = defineProps<{ selected: Rule | null }>()
const emit = defineEmits<{
  (e: 'saved', rule: Rule): void
  (e: 'canceled'): void
  (e: 'deleted', id: string): void
}>()

const ALGORITHMS: { value: Algorithm; label: string; hint: string }[] = [
  { value: 'token_bucket', label: '令牌桶', hint: '恒速补令牌，桶满则弃；可吃掉桶容量的突发' },
  { value: 'leaky_bucket', label: '漏桶', hint: '恒速漏出，入桶超容即拒；输出被整形为平滑速率' },
  { value: 'fixed_window', label: '固定窗口', hint: '整秒窗口计数，跨窗清零；边界可能双倍突发' },
  { value: 'sliding_window', label: '滑动窗口', hint: '相邻两窗口按时间加权，抹平边界双倍突发' },
  { value: 'sliding_log', label: '滑动日志', hint: '精确保留窗口内每次命中，逐条逐出过期记录' },
]

const DIMENSIONS: { value: Dimension; label: string }[] = [
  { value: 'client_id', label: '客户端标识 client_id' },
  { value: 'api_path', label: '接口路径 api_path' },
  { value: 'group', label: '来源分组 group' },
]

const LEVELS: { value: Level; label: string; dim: Dimension }[] = [
  { value: 'global', label: '全局 global', dim: 'client_id' },
  { value: 'group', label: '分组 group', dim: 'group' },
  { value: 'api', label: '接口 api', dim: 'api_path' },
  { value: 'client', label: '客户端 client', dim: 'client_id' },
]

const isBucket = (a: Algorithm) => a === 'token_bucket' || a === 'leaky_bucket'

function blankLevel(): RuleLevel {
  return { threshold: 10, burst: 5, window_seconds: 1 }
}

function blank(): RuleDraft {
  return {
    name: '',
    enabled: true,
    algorithm: 'token_bucket',
    dimensions: ['client_id'],
    levels: { client: { threshold: 10, burst: 5 } },
    matchers: {},
  }
}

const form = reactive<RuleDraft>(blank())
const matcherText = reactive<Record<string, string>>({})
const error = ref<string>('')
const saving = ref(false)

const editingId = computed(() => props.selected?.id ?? null)
const needsWindow = computed(() => !isBucket(form.algorithm))
const needsBurst = computed(() => isBucket(form.algorithm))

watch(
  () => props.selected,
  (r) => {
    error.value = ''
    if (r) {
      form.name = r.name
      form.enabled = r.enabled
      form.algorithm = r.algorithm
      form.dimensions = [...r.dimensions]
      form.levels = JSON.parse(JSON.stringify(r.levels))
      form.matchers = r.matchers ? JSON.parse(JSON.stringify(r.matchers)) : {}
      for (const d of DIMENSIONS) {
        matcherText[d.value] = (r.matchers?.[d.value] ?? []).join(', ')
      }
    } else {
      Object.assign(form, blank())
      for (const d of DIMENSIONS) matcherText[d.value] = ''
    }
  },
  { immediate: true },
)

function toggleDim(d: Dimension) {
  const i = form.dimensions.indexOf(d)
  if (i >= 0) form.dimensions.splice(i, 1)
  else form.dimensions.push(d)
}

function toggleLevel(lv: Level) {
  if (form.levels[lv]) {
    delete form.levels[lv]
  } else {
    form.levels[lv] = blankLevel()
  }
}

// Switching between bucket and window algorithms rewrites level fields so an
// illegal combination (e.g. burst on a window) can never be submitted.
watch(
  () => form.algorithm,
  (a) => {
    for (const lv of Object.keys(form.levels) as Level[]) {
      const cur = form.levels[lv]!
      if (isBucket(a)) {
        form.levels[lv] = { threshold: cur.threshold || 10, burst: cur.burst || 5 }
      } else {
        form.levels[lv] = {
          threshold: cur.threshold || 10,
          window_seconds: cur.window_seconds || 1,
        }
      }
    }
  },
)

function clientValidate(): string | null {
  if (!form.name.trim()) return '规则名称不能为空'
  if (form.dimensions.length === 0) return '至少选择一个限流维度'
  const present = Object.keys(form.levels) as Level[]
  if (present.length === 0) return '至少配置一级配额'
  const needs: Record<Level, Dimension | null> = {
    global: null,
    group: 'group',
    api: 'api_path',
    client: 'client_id',
  }
  for (const lv of present) {
    const d = needs[lv]
    if (d && !form.dimensions.includes(d)) {
      return `${lv} 级配额要求声明维度 ${d}`
    }
    const cfg = form.levels[lv]!
    if (!cfg.threshold || cfg.threshold <= 0) return `${lv} 级阈值必须为正整数`
    if (needsBurst.value && (!cfg.burst || cfg.burst <= 0))
      return `${lv} 级桶容量必须为正整数`
    if (needsWindow.value && (!cfg.window_seconds || cfg.window_seconds <= 0))
      return `${lv} 级窗口长度必须为正整数（秒）`
  }
  return null
}

async function save() {
  error.value = ''
  const v = clientValidate()
  if (v) {
    error.value = v
    return
  }
  // Parse matcher lists from comma separated text.
  const matchers: RuleDraft['matchers'] = {}
  for (const d of form.dimensions) {
    const txt = (matcherText[d] ?? '').trim()
    if (txt) {
      matchers[d] = txt
        .split(/[,，\n]/)
        .map((s) => s.trim())
        .filter(Boolean)
      if (matchers[d]!.length === 0) {
        error.value = `维度 ${d} 的匹配值缺失`
        return
      }
    }
  }
  const payload: RuleDraft = {
    name: form.name.trim(),
    enabled: form.enabled,
    algorithm: form.algorithm,
    dimensions: [...form.dimensions],
    levels: JSON.parse(JSON.stringify(form.levels)),
    matchers,
  }
  saving.value = true
  try {
    const saved = editingId.value
      ? await api.updateRule(editingId.value, payload)
      : await api.createRule(payload)
    emit('saved', saved)
  } catch (e: any) {
    error.value = e.message
  } finally {
    saving.value = false
  }
}

async function remove() {
  if (!editingId.value) return
  if (!confirm(`确认删除规则「${form.name}」？`)) return
  error.value = ''
  try {
    await api.deleteRule(editingId.value)
    emit('deleted', editingId.value)
  } catch (e: any) {
    error.value = e.message
  }
}
</script>

<template>
  <div class="panel">
    <h2>{{ editingId ? '编辑规则' : '新建规则' }}</h2>
    <div v-if="error" class="error">{{ error }}</div>

    <label class="field">
      <span class="lbl">规则名称</span>
      <input v-model="form.name" placeholder="例如：下单接口-客户端限流" />
    </label>

    <label class="field">
      <span class="lbl">限流算法</span>
      <select v-model="form.algorithm">
        <option v-for="a in ALGORITHMS" :key="a.value" :value="a.value">{{ a.label }}</option>
      </select>
      <span class="muted" style="margin-top: 4px; display: block">
        {{ ALGORITHMS.find((a) => a.value === form.algorithm)?.hint }}
      </span>
    </label>

    <div class="field">
      <span class="lbl">限流维度（按 AND 组合生效）</span>
      <div class="checks">
        <label v-for="d in DIMENSIONS" :key="d.value">
          <input type="checkbox" :checked="form.dimensions.includes(d.value)" @change="toggleDim(d.value)" />
          {{ d.label }}
        </label>
      </div>
    </div>

    <div class="field">
      <span class="lbl">配额级别（可多级并存，请求逐级校验）</span>
      <div v-for="lv in LEVELS" :key="lv.value" class="level-card">
        <label style="display:flex;align-items:center;gap:6px;font-size:12px;color:var(--text)">
          <input
            type="checkbox"
            style="width:auto"
            :checked="!!form.levels[lv.value]"
            @change="toggleLevel(lv.value)"
          />
          <h3 style="margin:0">{{ lv.label }}</h3>
        </label>
        <div v-if="form.levels[lv.value]" class="grid-2" style="margin-top:7px">
          <label class="field" style="margin:0">
            <span class="lbl">{{ needsBurst ? '补充/漏出速率（每秒）' : '窗口阈值（次）' }}</span>
            <input v-model.number="form.levels[lv.value]!.threshold" type="number" min="1" />
          </label>
          <label v-if="needsBurst" class="field" style="margin:0">
            <span class="lbl">桶容量 burst</span>
            <input v-model.number="form.levels[lv.value]!.burst" type="number" min="1" />
          </label>
          <label v-if="needsWindow" class="field" style="margin:0">
            <span class="lbl">窗口长度（秒）</span>
            <input v-model.number="form.levels[lv.value]!.window_seconds" type="number" min="1" />
          </label>
        </div>
      </div>
    </div>

    <div class="field">
      <span class="lbl">维度取值白名单（可选，逗号分隔；留空表示任意值）</span>
      <label v-for="d in DIMENSIONS" :key="d.value" v-show="form.dimensions.includes(d.value)" class="field">
        <span class="lbl">{{ d.label }}</span>
        <input v-model="matcherText[d.value]" :placeholder="`只对这些 ${d.value} 生效`" />
      </label>
    </div>

    <label class="checks" style="margin-bottom:12px">
      <input type="checkbox" v-model="form.enabled" style="width:auto" />
      规则启用（停用后不再参与判定）
    </label>

    <div class="row">
      <button @click="save" :disabled="saving">{{ editingId ? '保存热更新' : '创建规则' }}</button>
      <button v-if="editingId" class="danger" @click="remove">删除</button>
      <button v-if="editingId" class="secondary" @click="emit('canceled')">取消</button>
    </div>
  </div>
</template>
