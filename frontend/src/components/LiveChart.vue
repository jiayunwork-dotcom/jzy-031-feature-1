<script setup lang="ts">
import { computed } from 'vue'
import type { Point } from '../lib/types'

const props = defineProps<{
  points: Point[]
  height?: number
}>()

const W = 720
const H = computed(() => props.height ?? 220)
const PAD = 28

const maxY = computed(() => {
  let m = 1
  for (const p of props.points) m = Math.max(m, p.allow + p.deny)
  // round up to a nice grid
  const steps = [1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000]
  for (const s of steps) if (s >= m) return s
  return m
})

const n = computed(() => Math.max(props.points.length, 2))

function x(i: number) {
  return PAD + (i / (n.value - 1)) * (W - PAD - 8)
}
function yAllow(p: Point) {
  return H.value - 14 - (p.allow / maxY.value) * (H.value - PAD - 20)
}
function yTotal(p: Point) {
  return H.value - 14 - ((p.allow + p.deny) / maxY.value) * (H.value - PAD - 20)
}

const allowPath = computed(() =>
  props.points.map((p, i) => `${i === 0 ? 'M' : 'L'}${x(i).toFixed(1)},${yAllow(p).toFixed(1)}`).join(' '),
)
const denyPath = computed(() =>
  props.points.map((p, i) => `${i === 0 ? 'M' : 'L'}${x(i).toFixed(1)},${yTotal(p).toFixed(1)}`).join(' '),
)

// area fill under allow curve
const allowArea = computed(() => {
  if (props.points.length === 0) return ''
  const first = `M${x(0).toFixed(1)},${H.value - 14}`
  const mid = props.points
    .map((p, i) => `L${x(i).toFixed(1)},${yAllow(p).toFixed(1)}`)
    .join(' ')
  const last = `L${x(n.value - 1).toFixed(1)},${H.value - 14} Z`
  return `${first} ${mid} ${last}`
})

const gridLines = computed(() => {
  const lines: number[] = []
  for (let i = 0; i <= 4; i++) lines.push((maxY.value / 4) * i)
  return lines
})

const latest = computed(() => props.points[props.points.length - 1])
</script>

<template>
  <div>
    <svg :viewBox="`0 0 ${W} ${H}`" width="100%" preserveAspectRatio="none" style="display:block">
      <!-- grid -->
      <g v-for="(g, i) in gridLines" :key="i">
        <line
          :x1="PAD"
          :x2="W - 8"
          :y1="H - 14 - (g / maxY) * (H - PAD - 20)"
          :y2="H - 14 - (g / maxY) * (H - PAD - 20)"
          stroke="#2b3650"
          stroke-width="1"
          stroke-dasharray="3 4"
        />
        <text :x="4" :y="H - 10 - (g / maxY) * (H - PAD - 20)" fill="#8b97ad" font-size="9">
          {{ Math.round(g) }}
        </text>
      </g>
      <path :d="allowArea" fill="rgba(46,204,113,0.12)" />
      <path :d="allowPath" fill="none" stroke="#2ecc71" stroke-width="2" />
      <path :d="denyPath" fill="none" stroke="#ff5d6c" stroke-width="2" stroke-dasharray="5 3" />
      <!-- latest markers -->
      <g v-if="latest">
        <circle :cx="x(points.length - 1)" :cy="yAllow(latest)" r="3" fill="#2ecc71" />
        <circle :cx="x(points.length - 1)" :cy="yTotal(latest)" r="3" fill="#ff5d6c" />
      </g>
    </svg>
    <div class="row" style="gap:16px;font-size:11px;color:var(--muted);margin-top:2px;padding-left:30px">
      <span><span style="color:#2ecc71">●</span> 放行/秒</span>
      <span><span style="color:#ff5d6c">●</span> 放行+拒绝/秒（虚线）</span>
      <span style="margin-left:auto">每格 1 秒 · 共 {{ points.length }} 秒</span>
    </div>
  </div>
</template>
