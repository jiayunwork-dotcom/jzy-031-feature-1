<script setup lang="ts">
import { ref, onMounted } from 'vue'
import type { Rule } from './lib/types'
import { api } from './lib/api'
import RuleList from './components/RuleList.vue'
import RuleEditor from './components/RuleEditor.vue'
import ObserverPanel from './components/ObserverPanel.vue'
import TrafficGenerator from './components/TrafficGenerator.vue'

const rules = ref<Rule[]>([])
const selectedId = ref<string | null>(null)
const loadError = ref('')
const justSaved = ref(false)

async function load() {
  try {
    rules.value = await api.listRules()
    if (selectedId.value && !rules.value.find((r) => r.id === selectedId.value)) {
      selectedId.value = null
    }
  } catch (e: any) {
    loadError.value = '无法连接后端网关：' + e.message
  }
}

const selectedRule = () => rules.value.find((r) => r.id === selectedId.value) ?? null

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
}

onMounted(load)
</script>

<template>
  <div class="app">
    <div class="header">
      <h1>分布式速率限制与流量整形网关</h1>
      <p>
        在浏览器配置限流规则（五种算法 · 多维 AND · 四级配额），由后端 Go 网关基于 Redis 即时判定与计数；
        规则热生效、多实例共享配额，曲线每秒刷新。
      </p>
      <div v-if="loadError" class="error">{{ loadError }}</div>
      <div v-if="justSaved" class="verdict-box ok" style="display:inline-block;margin:0 0 8px">
        规则已保存并<strong>热生效</strong>，所有网关实例即刻按新规则判定。
      </div>
    </div>

    <div class="layout">
      <div>
        <RuleList :rules="rules" :selected-id="selectedId" @select="selectedId = $event" @create="onCreate" />
        <div style="height:16px"></div>
        <RuleEditor
          :key="selectedId ?? 'new'"
          :selected="selectedRule()"
          @saved="onSaved"
          @deleted="onDeleted"
          @canceled="selectedId = null"
        />
      </div>
      <div>
        <ObserverPanel :rule="selectedRule()" />
        <div style="height:16px"></div>
        <TrafficGenerator :rule="selectedRule()" />
      </div>
    </div>
  </div>
</template>
