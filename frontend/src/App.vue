<script setup lang="ts">
import { ref, onMounted, computed } from 'vue'
import type { Rollout, Rule, RuleDraft } from './lib/types'
import { api } from './lib/api'
import RuleList from './components/RuleList.vue'
import RuleEditor from './components/RuleEditor.vue'
import ObserverPanel from './components/ObserverPanel.vue'
import TrafficGenerator from './components/TrafficGenerator.vue'
import CanaryPanel from './components/CanaryPanel.vue'

const rules = ref<Rule[]>([])
const rollouts = ref<Rollout[]>([])
const selectedId = ref<string | null>(null)
const loadError = ref('')
const justSaved = ref(false)
const busy = ref(false)
// When set, the editor edits the canary content of this rollout.
const editingCanary = ref<Rollout | null>(null)

async function load() {
  try {
    const [rs, ros] = await Promise.all([api.listRules(), api.listRollouts()])
    rules.value = rs
    rollouts.value = ros.rollouts
    if (selectedId.value && !rules.value.find((r) => r.id === selectedId.value)) {
      selectedId.value = null
    }
    if (editingCanary.value && !rollouts.value.find((x) => x.rule_id === editingCanary.value!.rule_id)) {
      editingCanary.value = null
    }
  } catch (e: any) {
    loadError.value = '无法连接后端网关：' + e.message
  }
}

const selectedRule = () => rules.value.find((r) => r.id === selectedId.value) ?? null
const selectedRollout = computed(
  () => rollouts.value.find((x) => x.rule_id === selectedId.value) ?? null,
)

function onSaved(saved: Rule) {
  selectedId.value = saved.id
  justSaved.value = true
  load().then(() => setTimeout(() => (justSaved.value = false), 1500))
}
function onDeleted() {
  selectedId.value = null
  load()
}
function onCreate() {
  selectedId.value = null
  editingCanary.value = null
}

async function onStartCanary(p: { id: string; draft: RuleDraft; percent: number }) {
  busy.value = true
  loadError.value = ''
  try {
    await api.startRollout(p.id, p.draft, p.percent)
    selectedId.value = p.id
    await load()
  } catch (e: any) {
    loadError.value = '启动灰度失败：' + e.message
  } finally {
    busy.value = false
  }
}

async function onCanarySaved(p: { id: string; draft: RuleDraft }) {
  const ro = rollouts.value.find((x) => x.rule_id === p.id)
  busy.value = true
  loadError.value = ''
  try {
    await api.updateRolloutCanary(p.id, p.draft, ro?.percent ?? 0)
    editingCanary.value = null
    await load()
  } catch (e: any) {
    loadError.value = '保存新版本失败：' + e.message
  } finally {
    busy.value = false
  }
}

onMounted(load)
</script>

<template>
  <div class="app">
    <div class="header">
      <h1>分布式速率限制与流量整形网关</h1>
      <p>
        在浏览器配置限流规则（五种算法 · 多维 AND · 四级配额），由后端 Go 网关基于 Redis 即时判定与计数；
        规则热生效、多实例共享配额。规则改动支持<strong>按比例灰度放量</strong>：先小流量走新版本，逐步扩大到 100%，
        出问题可一键中止、全部流量瞬间回到旧版本；分桶确定性、可复现，新旧版本配额完全隔离。
      </p>
      <div v-if="loadError" class="error">{{ loadError }}</div>
      <div v-if="justSaved" class="verdict-box ok" style="display:inline-block;margin:0 0 8px">
        规则已保存并<strong>热生效</strong>，所有网关实例即刻按新规则判定。
      </div>
      <div v-if="busy" class="muted" style="display:inline-block;margin:0 0 8px">处理中…</div>
    </div>

    <div class="layout">
      <div>
        <RuleList :rules="rules" :selected-id="selectedId" @select="selectedId = $event" @create="onCreate" />
        <div style="height:16px"></div>
        <RuleEditor
          :key="editingCanary ? 'canary-' + editingCanary.rule_id : selectedId ?? 'new'"
          :selected="selectedRule()"
          :canary-of="editingCanary"
          :existing-rollout="selectedRollout"
          @saved="onSaved"
          @deleted="onDeleted"
          @canceled="selectedId = null"
          @start-canary="onStartCanary"
          @canary-saved="onCanarySaved"
          @canary-canceled="editingCanary = null"
        />
      </div>
      <div>
        <CanaryPanel
          :rollout="selectedRollout"
          @changed="load"
          @edit-canary="(ro) => (editingCanary = ro)"
        />
        <div v-if="selectedRollout" style="height:16px"></div>
        <ObserverPanel :rule="selectedRule()" :rollout="selectedRollout" />
        <div style="height:16px"></div>
        <TrafficGenerator :rule="selectedRule()" :rollout="selectedRollout" />
      </div>
    </div>
  </div>
</template>
