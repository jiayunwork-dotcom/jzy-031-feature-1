<script setup lang="ts">
import { ref, watch, onMounted, onUnmounted, computed } from 'vue'
import type { DenyEvent, Point, Rollout, Rule } from '../lib/types'
import { api } from '../lib/api'
import LiveChart from './LiveChart.vue'

const props = defineProps<{
  rule: Rule | null
  rollout: Rollout | null
}>()

const points = ref<Point[]>([])
const canaryPoints = ref<Point[]>([])
const denies = ref<DenyEvent[]>([])
const selectedLevels = ref<Array<{ level: string; remaining: number; reset_in_ms: number; release_in_ms?: number; key: string }>>([])
const stableLevels = ref<any[]>([])
const canaryLevels = ref<any[]>([])
const error = ref('')
let timer: number | undefined

const WINDOW_SECONDS = 60

async function refresh() {
  if (!props.rule) {
    points.value = []
    canaryPoints.value = []
    denies.value = []
    selectedLevels.value = []
    stableLevels.value = []
    canaryLevels.value = []
    return
  }
  error.value = ''
  try {
    const [ts, dn, st] = await Promise.all([
      api.timeSeries(props.rule.id, WINDOW_SECONDS, 'stable'),
      api.denies(props.rule.id),
      api.ruleState(props.rule.id, {
        client_id: 'client-A',
        api_path: '/orders',
        group: 'group-1',
      }),
    ])
    points.value = ts.points
    denies.value = dn.denies
    selectedLevels.value = st.selected
    stableLevels.value = st.stable_selected ?? []
    canaryLevels.value = st.canary_selected ?? []

    if (props.rollout) {
      const cts = await api.timeSeries(props.rule.id, WINDOW_SECONDS, 'canary')
      canaryPoints.value = cts.points
    } else {
      canaryPoints.value = []
    }
  } catch (e: any) {
    error.value = e.message
  }
}

watch(() => [props.rule?.id, props.rollout?.percent], refresh, { immediate: false })

onMounted(() => {
  refresh()
  timer = window.setInterval(refresh, 1000)
})
onUnmounted(() => {
  if (timer) window.clearInterval(timer)
})

const last = computed(() => points.value[points.value.length - 1])
const canaryLast = computed(() => canaryPoints.value[canaryPoints.value.length - 1])
const totalAllow = computed(() => points.value.reduce((s, p) => s + p.allow, 0))
const totalDeny = computed(() => points.value.reduce((s, p) => s + p.deny, 0))
const canaryTotalAllow = computed(() => canaryPoints.value.reduce((s, p) => s + p.allow, 0))
const canaryTotalDeny = computed(() => canaryPoints.value.reduce((s, p) => s + p.deny, 0))

function fmtTime(ms: number) {
  return new Date(ms).toLocaleTimeString('zh-CN', { hour12: false })
}

const LEVEL_LABEL: Record<string, string> = {
  global: '全局',
  group: '分组',
  api: '接口',
  client: '客户端',
}
</script>

<template>
  <div class="panel">
    <h2>
      实时观测
      <span v-if="rule" class="muted" style="font-weight:400">· {{ rule.name }}</span>
      <span v-if="rollout" class="badge" style="margin-left:6px;background:#f5a623;color:#1a1a1a">
        灰度 {{ rollout.percent }}%
      </span>
    </h2>
    <div v-if="error" class="error">{{ error }}</div>
    <div v-if="!rule" class="muted">选择或创建一条规则后，这里会实时显示放行/拒绝曲线与余量。</div>

    <template v-else>
      <!-- Aggregate (stable when no rollout) -->
      <div class="grid-4">
        <div class="stat"><div class="v green">{{ last?.allow ?? 0 }}</div><div class="k">{{ rollout ? '旧版 放行/秒' : '当前放行/秒' }}</div></div>
        <div class="stat"><div class="v red">{{ last?.deny ?? 0 }}</div><div class="k">{{ rollout ? '旧版 拒绝/秒' : '当前拒绝/秒' }}</div></div>
        <div class="stat"><div class="v">{{ totalAllow }}</div><div class="k">近 {{ WINDOW_SECONDS }}s 放行</div></div>
        <div class="stat"><div class="v red">{{ totalDeny }}</div><div class="k">近 {{ WINDOW_SECONDS }}s 拒绝</div></div>
      </div>

      <div style="margin-top:14px">
        <div class="muted" style="font-size:12px;margin-bottom:4px">
          {{ rollout ? '旧版本（stable）走势' : '放行 / 拒绝走势' }}
        </div>
        <LiveChart :points="points" />
      </div>

      <!-- Canary-only curves while a rollout is in flight -->
      <template v-if="rollout">
        <div class="grid-4" style="margin-top:16px">
          <div class="stat"><div class="v" style="color:#f5a623">{{ canaryLast?.allow ?? 0 }}</div><div class="k">新版 放行/秒</div></div>
          <div class="stat"><div class="v red">{{ canaryLast?.deny ?? 0 }}</div><div class="k">新版 拒绝/秒</div></div>
          <div class="stat"><div class="v" style="color:#f5a623">{{ canaryTotalAllow }}</div><div class="k">新版近 {{ WINDOW_SECONDS }}s 放行</div></div>
          <div class="stat"><div class="v red">{{ canaryTotalDeny }}</div><div class="k">新版近 {{ WINDOW_SECONDS }}s 拒绝</div></div>
        </div>
        <div style="margin-top:10px">
          <div class="muted" style="font-size:12px;margin-bottom:4px">新版本（canary）走势 · 配额与旧版完全隔离</div>
          <LiveChart :points="canaryPoints" />
        </div>
      </template>

      <h2 style="margin-top:18px">各级余量 / 窗口占用</h2>
      <div v-if="!rollout" class="grid-4">
        <div v-for="lv in selectedLevels" :key="lv.key" class="stat">
          <div class="k">{{ LEVEL_LABEL[lv.level] ?? lv.level }}</div>
          <div class="v" :class="lv.remaining > 0 ? 'green' : 'red'">{{ lv.remaining }}</div>
          <div class="k mono">
            {{ lv.release_in_ms ? `释放 ${lv.release_in_ms}ms` : `${lv.reset_in_ms}ms 后重置` }}
          </div>
        </div>
        <div v-if="selectedLevels.length === 0" class="muted">该规则暂无被触发的配额键。</div>
      </div>

      <template v-else>
        <div class="muted" style="font-size:12px;margin:4px 0">旧版本（stable）</div>
        <div class="grid-4">
          <div v-for="lv in stableLevels" :key="'s-' + lv.key" class="stat">
            <div class="k">{{ LEVEL_LABEL[lv.level] ?? lv.level }}</div>
            <div class="v" :class="lv.remaining > 0 ? 'green' : 'red'">{{ lv.remaining }}</div>
            <div class="k mono">{{ lv.reset_in_ms }}ms 后重置</div>
          </div>
          <div v-if="stableLevels.length === 0" class="muted">旧版本该主体暂无计数。</div>
        </div>
        <div class="muted" style="font-size:12px;margin:10px 0 4px">新版本（canary）</div>
        <div class="grid-4">
          <div v-for="lv in canaryLevels" :key="'c-' + lv.key" class="stat">
            <div class="k">{{ LEVEL_LABEL[lv.level] ?? lv.level }}</div>
            <div class="v" :class="lv.remaining > 0 ? 'green' : 'red'">{{ lv.remaining }}</div>
            <div class="k mono">{{ lv.reset_in_ms }}ms 后重置</div>
          </div>
          <div v-if="canaryLevels.length === 0" class="muted">新版本该主体暂无计数。</div>
        </div>
      </template>

      <h2 style="margin-top:18px">最近被拒请求</h2>
      <table class="denies">
        <thead>
          <tr>
            <th>时间</th><th>规则</th><th>版本</th><th>级别</th><th>客户端</th><th>接口</th><th>原因</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="(d, i) in denies" :key="d.time_ms + '-' + i">
            <td class="mono">{{ fmtTime(d.time_ms) }}</td>
            <td>{{ d.rule_name }}</td>
            <td>
              <span class="badge" :style="d.version === 'canary' ? 'background:#f5a623;color:#1a1a1a' : ''">
                {{ d.version === 'canary' ? '新版' : '旧版' }}
              </span>
            </td>
            <td><span class="badge algo">{{ LEVEL_LABEL[d.level] ?? d.level }}</span></td>
            <td class="mono">{{ d.client_id }}</td>
            <td class="mono">{{ d.api_path }}</td>
            <td class="muted">{{ d.reason }}</td>
          </tr>
        </tbody>
      </table>
      <div v-if="denies.length === 0" class="muted">最近没有拒绝记录。</div>
    </template>
  </div>
</template>
