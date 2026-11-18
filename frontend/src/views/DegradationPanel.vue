<template>
  <section v-if="enabled" class="domain-panel" data-testid="degradation-panel">
    <header class="domain-head">
      <div>
        <h3>降级状态清单</h3>
        <p class="page-desc">
          清单只依据拓扑运行数据中的「节点 → 源端」映射生成；名称歧义时拒绝写入，并保留来源版本。
        </p>
      </div>
      <div class="page-actions">
        <input
          v-model="topologyInput"
          class="topology-input"
          data-testid="degradation-topology"
          placeholder="拓扑名称，如 任务拓扑样例1"
        />
        <button class="btn" data-testid="degradation-refresh" type="button" @click="refresh">读取清单</button>
        <button class="btn primary" data-testid="degradation-build" type="button" :disabled="busy" @click="build">
          生成降级清单
        </button>
      </div>
    </header>

    <p v-if="errorMessage" class="error-text" data-testid="degradation-error">{{ errorMessage }}</p>

    <div v-if="result" class="degradation-status" :data-status="result.status">
      <span v-if="result.status === 'empty'" class="empty-badge" data-testid="degradation-empty">
        暂无降级状态数据：{{ result.message || '该拓扑还没有可用的降级清单' }}
      </span>
      <span v-else-if="result.status === 'refused'" class="error-badge" data-testid="degradation-refused">
        已拒绝写入：{{ result.error }}
      </span>
      <span v-else-if="result.status === 'missing_topology'" class="error-badge">
        {{ result.error }}
      </span>
      <span v-else class="ok-badge" data-testid="degradation-version">
        清单 v{{ result.manifest_version }} · 来源拓扑运行版本 v{{ result.source_version }}
      </span>
      <span v-if="result.similar_names?.length" class="warn-text" data-testid="degradation-similar">
        近似名称：{{ result.similar_names.join('，') }}
      </span>
    </div>

    <table v-if="result?.entries?.length" class="data-table">
      <thead>
        <tr>
          <th>拓扑节点</th>
          <th>源端</th>
          <th>源端绑定</th>
          <th>吞吐配置</th>
          <th>最大QPS</th>
          <th>降级QPS</th>
          <th>策略</th>
        </tr>
      </thead>
      <tbody>
        <tr
          v-for="entry in result.entries"
          :key="entry.binding_key"
          :data-testid="`degradation-row-${entry.node}`"
          :data-source="entry.source"
        >
          <td>{{ entry.node }}</td>
          <td>{{ entry.source }}</td>
          <td class="mono">{{ entry.binding_key }}</td>
          <td>{{ entry.throughput_config_name }}#{{ entry.throughput_config_id }}</td>
          <td>{{ entry.max_qps }}</td>
          <td>{{ entry.degraded_qps }}</td>
          <td>{{ entry.strategy }}</td>
        </tr>
      </tbody>
    </table>

    <div v-if="result?.refusals?.length" class="panel-alert" data-testid="degradation-refusals">
      <strong>以下源端名称无法唯一绑定吞吐配置，清单未写入：</strong>
      <ul>
        <li v-for="refusal in result.refusals" :key="refusal.binding_key">
          {{ refusal.source }}（{{ refusal.binding_key }}）：{{ refusal.reason }}
        </li>
      </ul>
    </div>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref, watch } from 'vue'

import { buildDegradationManifest, loadDegradationManifest } from '@/api/backend-service'
import type { DegradationResult } from '@/data/topology'

const props = defineProps<{ moduleKey: string }>()

const enabled = computed(() => props.moduleKey === 'throughput_mgr')
const topologyInput = ref('任务拓扑样例1')
const result = ref<DegradationResult | null>(null)
const errorMessage = ref('')
const busy = ref(false)

async function refresh() {
  if (!enabled.value || !topologyInput.value.trim()) return
  errorMessage.value = ''
  try {
    result.value = await loadDegradationManifest(topologyInput.value.trim())
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '降级状态清单读取失败'
  }
}

async function build() {
  if (!enabled.value || !topologyInput.value.trim()) return
  busy.value = true
  errorMessage.value = ''
  try {
    result.value = await buildDegradationManifest(topologyInput.value.trim())
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '降级状态清单生成失败'
  } finally {
    busy.value = false
  }
}

watch(() => props.moduleKey, () => {
  result.value = null
  errorMessage.value = ''
  void refresh()
})

onMounted(refresh)
</script>

<style scoped>
.domain-panel {
  margin: 16px 0;
  padding: 14px;
  background: #fff;
  border: 1px solid var(--border);
  border-radius: 8px;
}
.domain-head { display: flex; justify-content: space-between; align-items: flex-start; gap: 12px; flex-wrap: wrap; }
.topology-input { padding: 6px 10px; border: 1px solid var(--border); border-radius: 6px; font-size: 13px; width: 220px; }
.degradation-status { display: flex; flex-wrap: wrap; gap: 12px; margin: 10px 0; font-size: 13px; }
.mono { font-family: monospace; font-size: 12px; }
.ok-badge { color: #15803d; }
.empty-badge { color: var(--muted); }
.error-badge { color: #b42318; }
.warn-text { color: #b45309; }
.panel-alert { margin-top: 10px; padding: 10px 12px; border: 1px solid #fda29b; background: #fef3f2; border-radius: 6px; font-size: 13px; }
</style>
