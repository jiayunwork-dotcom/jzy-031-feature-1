<script setup lang="ts">
import { computed, ref } from 'vue'
import type { Rollout } from '../lib/types'
import { api } from '../lib/api'

const props = defineProps<{ rollout: Rollout | null }>()
const emit = defineEmits<{
  (e: 'changed'): void
  (e: 'edit-canary', rollout: Rollout): void
}>()

const busy = ref(false)
const error = ref('')

const PRESETS = [0, 10, 25, 50, 75, 100]

async function act(fn: () => Promise<unknown>, ok?: () => void) {
  busy.value = true
  error.value = ''
  try {
    await fn()
    ok?.()
    emit('changed')
  } catch (e: any) {
    error.value = e.message
  } finally {
    busy.value = false
  }
}

function setPercent(p: number) {
  if (!props.rollout || props.rollout.percent === p) return
  act(() => api.setRolloutPercent(props.rollout!.rule_id, p))
}
function promote() {
  if (!props.rollout) return
  if (!confirm('确认在 100% 全量后，让新版本成为唯一生效规则并结束本次放量？')) return
  act(() => api.promoteRollout(props.rollout!.rule_id))
}
function abort() {
  if (!props.rollout) return
  if (!confirm('确认立即中止本次放量？全部流量将瞬间回到旧规则，新版本计数不再生效。')) return
  act(() => api.abortRollout(props.rollout!.rule_id))
}

const isFull = computed(() => props.rollout?.percent === 100)
</script>

<template>
  <div v-if="rollout" class="panel" style="border-color: var(--amber, #f5a623)">
    <h2>灰度放量控制 <span class="muted" style="font-weight:400">· 当前 {{ rollout.percent }}%</span></h2>
    <div v-if="error" class="error">{{ error }}</div>

    <div class="canary-bar">
      <div class="canary-fill" :style="{ width: rollout.percent + '%' }"></div>
      <span class="canary-label">新版本 {{ rollout.percent }}%</span>
    </div>

    <div class="row" style="margin:10px 0; flex-wrap:wrap; gap:6px">
      <button
        v-for="p in PRESETS"
        :key="p"
        class="small"
        :class="rollout.percent === p ? '' : 'secondary'"
        :disabled="busy"
        @click="setPercent(p)"
      >
        {{ p }}%
      </button>
    </div>

    <div class="muted" style="font-size:12px; margin-bottom:10px">
      提高比例只把更多主体单调地搬进新版本（已在新桶的主体不会被洗牌回去）；调回
      <strong>0%</strong> 即全部回退到旧规则但保留新版本内容。
    </div>

    <div class="row">
      <button class="secondary" :disabled="busy" @click="emit('edit-canary', rollout)">编辑新版本内容</button>
      <button :disabled="busy || !isFull" @click="promote">全量生效（100% 收口）</button>
      <button class="danger" :disabled="busy" @click="abort">中止放量 / 回退</button>
    </div>
  </div>
</template>

<style scoped>
.canary-bar {
  position: relative;
  height: 22px;
  border-radius: 4px;
  background: rgba(255, 255, 255, 0.08);
  overflow: hidden;
}
.canary-fill {
  position: absolute;
  inset: 0 auto 0 0;
  background: linear-gradient(90deg, #f5a623, #ff7a45);
  transition: width 0.25s ease;
}
.canary-label {
  position: absolute;
  inset: 0;
  display: flex;
  align-items: center;
  padding-left: 8px;
  font-size: 12px;
  color: #fff;
}
</style>
