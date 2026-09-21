<script setup lang="ts">
import { computed } from 'vue'
import type { Rule } from '../lib/types'

const props = defineProps<{
  rules: Rule[]
  selectedId: string | null
}>()
const emit = defineEmits<{
  (e: 'select', id: string): void
  (e: 'create'): void
}>()

const ALGO_LABEL: Record<string, string> = {
  token_bucket: '令牌桶',
  leaky_bucket: '漏桶',
  fixed_window: '固定窗口',
  sliding_window: '滑动窗口',
  sliding_log: '滑动日志',
}

const ordered = computed(() => [...props.rules].sort((a, b) => a.name.localeCompare(b.name)))
</script>

<template>
  <div class="panel">
    <div class="row" style="justify-content: space-between; align-items: center">
      <h2 style="margin: 0">限流规则</h2>
      <button class="small" @click="emit('create')">＋ 新建</button>
    </div>
    <div class="rule-list" style="margin-top: 12px">
      <div
        v-for="r in ordered"
        :key="r.id"
        class="rule-item"
        :class="{ active: r.id === selectedId }"
        @click="emit('select', r.id)"
      >
        <div>
          <div class="name">{{ r.name }}</div>
          <div class="meta">
            <span class="badge algo">{{ ALGO_LABEL[r.algorithm] ?? r.algorithm }}</span>
            <span class="badge">{{ Object.keys(r.levels).join(' / ') }}</span>
            <span class="badge" :class="r.enabled ? 'on' : 'off'">
              {{ r.enabled ? '启用' : '停用' }}
            </span>
          </div>
        </div>
      </div>
      <div v-if="ordered.length === 0" class="muted">还没有规则，点击「新建」开始。</div>
    </div>
  </div>
</template>
