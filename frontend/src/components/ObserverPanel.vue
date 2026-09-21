<script setup lang="ts">
import { ref, watch, onMounted, onUnmounted, computed } from 'vue'
import type { DenyEvent, Point, Rule } from '../lib/types'
import { api } from '../lib/api'
import LiveChart from './LiveChart.vue'

const props = defineProps<{ rule: Rule | null }>()

const points = ref<Point[]>([])
const denies = ref<DenyEvent[]>([])
const selectedLevels = ref<Array<{ level: string; remaining: number; reset_in_ms: number; release_in_ms?: number; key: string }>>([])
const error = ref('')
let timer: number | undefined

const WINDOW_SECONDS = 60

async function refresh() {
  if (!props.rule) {
    points.value = []
    denies.value = []
    selectedLevels.value = []
    return
  }
  error.value = ''
  try {
    const [ts, dn, st] = await Promise.all([
      api.timeSeries(props.rule.id, WINDOW_SECONDS),
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
  } catch (e: any) {
    error.value = e.message
  }
}

watch(() => props.rule?.id, refresh, { immediate: false })

onMounted(() => {
  refresh()
  timer = window.setInterval(refresh, 1000)
})
onUnmounted(() => {
  if (timer) window.clearInterval(timer)
})

const last = computed(() => points.value[points.value.length - 1])
const totalAllow = computed(() => points.value.reduce((s, p) => s + p.allow, 0))
const totalDeny = computed(() => points.value.reduce((s, p) => s + p.deny, 0))

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
    </h2>
    <div v-if="error" class="error">{{ error }}</div>
    <div v-if="!rule" class="muted">选择或创建一条规则后，这里会实时显示放行/拒绝曲线与余量。</div>

    <template v-else>
      <div class="grid-4">
        <div class="stat"><div class="v green">{{ last?.allow ?? 0 }}</div><div class="k">当前放行/秒</div></div>
        <div class="stat"><div class="v red">{{ last?.deny ?? 0 }}</div><div class="k">当前拒绝/秒</div></div>
        <div class="stat"><div class="v">{{ totalAllow }}</div><div class="k">近 {{ WINDOW_SECONDS }}s 放行</div></div>
        <div class="stat"><div class="v red">{{ totalDeny }}</div><div class="k">近 {{ WINDOW_SECONDS }}s 拒绝</div></div>
      </div>

      <div style="margin-top:14px">
        <LiveChart :points="points" />
      </div>

      <h2 style="margin-top:18px">各级余量 / 窗口占用</h2>
      <div class="grid-4">
        <div v-for="lv in selectedLevels" :key="lv.key" class="stat">
          <div class="k">{{ LEVEL_LABEL[lv.level] ?? lv.level }}</div>
          <div class="v" :class="lv.remaining > 0 ? 'green' : 'red'">{{ lv.remaining }}</div>
          <div class="k mono">
            {{ lv.release_in_ms ? `释放 ${lv.release_in_ms}ms` : `${lv.reset_in_ms}ms 后重置` }}
          </div>
        </div>
        <div v-if="selectedLevels.length === 0" class="muted">该规则暂无被触发的配额键。</div>
      </div>

      <h2 style="margin-top:18px">最近被拒请求</h2>
      <table class="denies">
        <thead>
          <tr>
            <th>时间</th><th>规则</th><th>级别</th><th>客户端</th><th>接口</th><th>原因</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="(d, i) in denies" :key="d.time_ms + '-' + i">
            <td class="mono">{{ fmtTime(d.time_ms) }}</td>
            <td>{{ d.rule_name }}</td>
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
