<template>
  <section v-if="enabled" class="domain-panel" data-testid="topology-binding-panel">
    <header class="domain-head">
      <div>
        <h3>节点绑定校验</h3>
        <p class="page-desc">
          算子链节点按「模块键#配置ID」完整标识匹配唯一配置；名称歧义或状态不可用时不会发布运行版本。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn" data-testid="binding-refresh" type="button" @click="refresh">刷新绑定</button>
        <button class="btn primary" data-testid="binding-deploy" type="button" :disabled="busy" @click="deploy">
          校验并发布
        </button>
      </div>
    </header>

    <p v-if="errorMessage" class="error-text" data-testid="binding-error">{{ errorMessage }}</p>
    <p v-if="notice" class="notice-text" data-testid="binding-notice">{{ notice }}</p>

    <div v-if="binding" class="binding-meta">
      <span>节点数：{{ binding.nodes.length }}</span>
      <span>绑定签名：{{ binding.signature || '—' }}</span>
      <span>运行版本：{{ binding.run_version || '未发布' }}</span>
      <span :class="binding.ok ? 'ok-text' : 'error-text'">
        {{ binding.ok ? '全部节点唯一绑定' : '存在需要修正的节点' }}
      </span>
    </div>

    <table v-if="binding" class="data-table">
      <thead>
        <tr>
          <th>算子链节点</th>
          <th>模块</th>
          <th>配置ID</th>
          <th>配置名称</th>
          <th>状态</th>
          <th>绑定标识</th>
          <th>结果</th>
        </tr>
      </thead>
      <tbody>
        <tr
          v-for="node in binding.nodes"
          :key="node.node"
          :data-testid="`binding-node-${node.node}`"
          :data-binding="node.binding_key || ''"
        >
          <td>{{ node.node }}</td>
          <td>{{ node.module ?? '—' }}</td>
          <td>{{ node.config_id ?? '—' }}</td>
          <td>{{ node.config_name ?? '—' }}</td>
          <td>{{ node.status ?? '—' }}</td>
          <td class="mono">{{ node.binding_key ?? '未绑定' }}</td>
          <td>
            <span v-if="node.binding_key && node.available" class="ok-text">
              可用{{ node.pinned ? '（运行版本钉住）' : '' }}
            </span>
            <span v-else-if="node.binding_key" class="warn-text">状态不可发布</span>
            <span v-else class="error-text">{{ node.detail || node.reason }}</span>
            <ul v-if="node.candidates?.length" class="candidate-list">
              <li v-for="candidate in node.candidates" :key="candidate.binding_key">
                候选 {{ candidate.binding_key }}（{{ candidate.config_name }}）
              </li>
            </ul>
          </td>
        </tr>
        <tr v-if="!binding.nodes.length">
          <td :colspan="7" class="empty-state">该拓扑算子链为空，暂无可校验节点</td>
        </tr>
      </tbody>
    </table>

    <div v-if="deployResult && !deployResult.ok" class="panel-alert" data-testid="binding-blocked">
      <strong>{{ deployResult.error }}</strong>
      <ul>
        <li v-for="reason in deployResult.reasons ?? []" :key="reason">{{ reason }}</li>
      </ul>
      <p class="page-desc">运行版本保持上一次已封存结果，不会返回半成品部署。</p>
    </div>

    <div v-if="deployResult?.published" class="panel-ok" data-testid="binding-published">
      已发布运行版本 v{{ deployResult.version }}，共 {{ Object.keys(deployResult.bindings ?? {}).length }} 个节点绑定。
    </div>
    <div v-else-if="deployResult?.idempotent" class="panel-ok" data-testid="binding-idempotent">
      节点绑定与已发布版本一致，复用运行版本 v{{ deployResult.version }}。
    </div>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref, watch } from 'vue'

import { deployTopology, loadTopologyBinding } from '@/api/backend-service'
import type { TopologyBindingResult, TopologyDeployResult } from '@/data/topology'

const props = defineProps<{ moduleKey: string; entryId: number | null }>()

const enabled = computed(() => props.moduleKey === 'task_topology' && props.entryId !== null)
const binding = ref<TopologyBindingResult | null>(null)
const deployResult = ref<TopologyDeployResult | null>(null)
const errorMessage = ref('')
const notice = ref('')
const busy = ref(false)

function reset() {
  binding.value = null
  deployResult.value = null
  errorMessage.value = ''
  notice.value = ''
}

async function refresh() {
  if (!enabled.value || props.entryId === null) return
  errorMessage.value = ''
  try {
    binding.value = await loadTopologyBinding(props.entryId)
    if (!binding.value.ok) {
      notice.value = '存在名称歧义或未匹配节点，可改用「模块键#配置ID」限定节点标识。'
    } else {
      notice.value = ''
    }
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '节点绑定读取失败'
  }
}

async function deploy() {
  if (!enabled.value || props.entryId === null) return
  busy.value = true
  errorMessage.value = ''
  notice.value = ''
  try {
    deployResult.value = await deployTopology(props.entryId)
    if (deployResult.value.message) notice.value = deployResult.value.message
    await refresh()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '拓扑部署失败'
  } finally {
    busy.value = false
  }
}

watch(() => [props.moduleKey, props.entryId], () => {
  reset()
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
.domain-head { display: flex; justify-content: space-between; align-items: flex-start; gap: 12px; }
.binding-meta { display: flex; flex-wrap: wrap; gap: 16px; margin: 10px 0; font-size: 12px; color: var(--muted); }
.mono { font-family: monospace; font-size: 12px; }
.ok-text { color: #15803d; }
.warn-text { color: #b45309; }
.notice-text { color: #b45309; font-size: 13px; }
.candidate-list { margin: 4px 0 0; padding-left: 16px; color: var(--muted); font-size: 12px; }
.panel-alert { margin-top: 10px; padding: 10px 12px; border: 1px solid #fda29b; background: #fef3f2; border-radius: 6px; font-size: 13px; }
.panel-ok { margin-top: 10px; padding: 10px 12px; border: 1px solid #a6f4c5; background: #ecfdf3; border-radius: 6px; font-size: 13px; }
</style>
